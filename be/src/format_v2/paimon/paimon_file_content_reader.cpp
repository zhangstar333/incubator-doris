// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "format_v2/paimon/paimon_file_content_reader.h"

#include <cstring>
#include <limits>
#include <map>
#include <string>
#include <tuple>

#include "core/column/column_varbinary.h"
#include "format_v2/paimon/paimon_blob_location.h"
#include "format_v2/paimon/paimon_file_content_reader_range.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/file_factory.h"
#include "io/io_common.h"
#include "runtime/file_content_reader.h"
#include "runtime/file_read_io_context.h"
#include "runtime/runtime_state.h"

namespace doris::format::paimon {
namespace {
Status validate_content_request(const FileReadRequest& read_request, int64_t max_payload_size) {
    if (read_request.uri.empty() || read_request.uri.find('\0') != std::string_view::npos ||
        read_request.offset < 0 || read_request.length < 0) {
        return Status::Corruption("Incomplete Paimon FILE range");
    }
    if (read_request.length > max_payload_size ||
        read_request.length > std::numeric_limits<int32_t>::max()) {
        return Status::InvalidArgument(
                "Paimon BLOB inline size {} bytes exceeds paimon_blob_max_size={} bytes or "
                "FILE capacity",
                read_request.length, max_payload_size);
    }
    return Status::OK();
}
} // namespace

Status PaimonFileContentReader::read_blob(RuntimeState* runtime_state,
                                          const TFileReadResource& file_read_resource,
                                          const std::vector<FileReadRequest>& read_requests,
                                          ColumnVarbinary* payload_column) {
    const auto& paimon_resource = file_read_resource.paimon;
    PaimonBlobReadMode blob_read_mode;
    switch (paimon_resource.table_type) {
    case TPaimonTableType::PRIMARY_KEY:
        blob_read_mode = PaimonBlobReadMode::RAW_RANGE;
        break;
    case TPaimonTableType::APPEND:
        blob_read_mode = PaimonBlobReadMode::APPEND_RECORD;
        break;
    default:
        return Status::InvalidArgument("Unsupported Paimon table type: {}",
                                       static_cast<int>(paimon_resource.table_type));
    }
    // Handles and I/O context are call-local. Reuse handles within this batch and release them
    // before returning.
    io::FileSystemProperties filesystem_properties;
    filesystem_properties.system_type = paimon_resource.file_type;
    filesystem_properties.properties = paimon_resource.properties;
    filesystem_properties.hdfs_params = paimon_resource.hdfs_params;
    filesystem_properties.broker_addresses = paimon_resource.broker_addresses;
    io::IOContext io_context;
    init_file_read_io_context(runtime_state, &io_context);
    io_context.is_disposable = runtime_state->query_options().disable_file_cache;
    const std::string filesystem_name;
    const auto max_payload_size = runtime_state->query_options().paimon_blob_max_size;
    const PaimonBlobReadContext blob_read_context {
            .system_properties = filesystem_properties,
            .runtime_state = runtime_state,
            .io_ctx = &io_context,
            .profile = nullptr,
            .fs_name = filesystem_name,
            .file_cache_admission = paimon_resource.file_cache_admission,
            .blob_max_size = max_payload_size};
    using BlobRangeKey = std::tuple<std::string_view, int64_t, int64_t>;
    std::map<BlobRangeKey, std::vector<size_t>> output_rows_by_blob_range;
    for (const auto& read_request : read_requests) {
        RETURN_IF_ERROR(validate_content_request(read_request, max_payload_size));
        output_rows_by_blob_range[{read_request.uri, read_request.offset, read_request.length}]
                .push_back(read_request.output_row_index);
    }
    PaimonFileContentRangeReader blob_range_reader;
    PODArray<char> payload_buffer;
    for (const auto& [blob_range, output_row_indices] : output_rows_by_blob_range) {
        RETURN_IF_CANCELLED(runtime_state);
        const auto& [blob_uri, payload_offset, payload_length] = blob_range;
        const PaimonBlobLocation blob_location {
                .uri = blob_uri, .offset = payload_offset, .length = payload_length};
        RETURN_IF_ERROR(blob_range_reader.read_range(blob_location, blob_read_context,
                                                     &payload_buffer, blob_read_mode));
        char* payload_data =
                payload_buffer.empty() ? nullptr : payload_column->alloc(payload_buffer.size());
        if (!payload_buffer.empty()) {
            std::memcpy(payload_data, payload_buffer.data(), payload_buffer.size());
        }
        const StringView payload_value(payload_data, cast_set<uint32_t>(payload_buffer.size()));
        for (const auto output_row_index : output_row_indices) {
            payload_column->get_data()[output_row_index] = payload_value;
        }
    }
    return Status::OK();
}
} // namespace doris::format::paimon
