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

#include "format_v2/lance/lance_file_content_reader.h"

#include <lance/lance.h>

#include <algorithm>
#include <cctype>
#include <limits>
#include <string>
#include <utility>

#include "core/column/column_varbinary.h"
#include "format_v2/lance/lance_reader_helper.h"
#include "format_v2/lance/lance_session_manager.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/file_content_reader.h"
#include "runtime/runtime_state.h"
#include "util/defer_op.h"

namespace doris::format::lance {

Status LanceFileContentReader::validate_resource(const TLanceBlobResource& lance_resource) {
    if (!lance_resource.__isset.dataset_uri ||
        lance_resource.dataset_uri.find('\0') != std::string::npos ||
        !lance_resource.__isset.version || lance_resource.version <= 0 ||
        !lance_resource.__isset.column_name || lance_resource.column_name.empty() ||
        lance_resource.column_name.find('\0') != std::string::npos) {
        return Status::InvalidArgument("Incomplete Lance FILE resource");
    }
    const auto scheme_separator_pos = lance_resource.dataset_uri.find("://");
    auto storage_scheme = lance_resource.dataset_uri.substr(0, scheme_separator_pos);
    std::ranges::transform(storage_scheme, storage_scheme.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (scheme_separator_pos == std::string::npos || storage_scheme.empty() ||
        storage_scheme == "file" || storage_scheme == "memory") {
        return Status::NotSupported("Lance FILE resources require remote storage");
    }
    for (const auto& [option_name, option_value] : lance_resource.storage_options) {
        if (option_name.find('\0') != std::string::npos ||
            option_value.find('\0') != std::string::npos) {
            return Status::InvalidArgument("NUL in Lance FILE storage options");
        }
    }
    return Status::OK();
}

Status LanceFileContentReader::_get_or_open_dataset(RuntimeState* runtime_state,
                                                    const TLanceBlobResource& lance_resource,
                                                    std::shared_ptr<LanceDataset>* dataset_handle) {
    DatasetKey dataset_key {.uri = lance_resource.dataset_uri,
                            .version = lance_resource.version,
                            .storage_options = lance_resource.storage_options};
    {
        std::lock_guard dataset_cache_lock(_dataset_cache_mutex);
        const auto dataset_iterator = _datasets_by_open_spec.find(dataset_key);
        if (dataset_iterator != _datasets_by_open_spec.end()) {
            *dataset_handle = dataset_iterator->second;
            return Status::OK();
        }
    }
    RETURN_IF_CANCELLED(runtime_state);
    std::vector<std::string> storage_options;
    RETURN_IF_ERROR(build_lance_storage_options(lance_resource.storage_options, &storage_options));
    std::vector<const char*> storage_option_pointers;
    for (const auto& storage_option : storage_options) {
        storage_option_pointers.push_back(storage_option.c_str());
    }
    storage_option_pointers.push_back(nullptr);
    LanceDataset* raw_dataset = nullptr;
    RETURN_IF_ERROR(LanceSessionManager::instance().open_dataset(
            lance_resource.dataset_uri.c_str(),
            storage_options.empty() ? nullptr : storage_option_pointers.data(),
            static_cast<uint64_t>(lance_resource.version), &raw_dataset));
    std::shared_ptr<LanceDataset> opened_dataset(raw_dataset, lance_dataset_close);
    // Concurrent expressions may open the same source; retain one and safely release the other.
    std::lock_guard dataset_cache_lock(_dataset_cache_mutex);
    *dataset_handle =
            _datasets_by_open_spec.try_emplace(std::move(dataset_key), std::move(opened_dataset))
                    .first->second;
    return Status::OK();
}

Status LanceFileContentReader::read_blob(RuntimeState* runtime_state,
                                         const TFileReadResource& file_read_resource,
                                         const std::vector<FileReadRequest>& read_requests,
                                         ColumnVarbinary* payload_column) {
    OutputRowsByBlobRowId output_rows_by_blob_row_id;
    for (const auto& read_request : read_requests) {
        output_rows_by_blob_row_id[read_request.row_id].push_back(read_request.output_row_index);
    }
    return _read_source_blobs(runtime_state, file_read_resource.lance, output_rows_by_blob_row_id,
                              payload_column);
}

Status LanceFileContentReader::_read_source_blobs(
        RuntimeState* runtime_state, const TLanceBlobResource& lance_resource,
        const OutputRowsByBlobRowId& output_rows_by_blob_row_id, ColumnVarbinary* payload_column) {
    std::shared_ptr<LanceDataset> dataset_handle;
    RETURN_IF_ERROR(_get_or_open_dataset(runtime_state, lance_resource, &dataset_handle));
    std::vector<uint64_t> blob_row_ids;
    blob_row_ids.reserve(output_rows_by_blob_row_id.size());
    for (const auto& blob_row_entry : output_rows_by_blob_row_id) {
        blob_row_ids.push_back(blob_row_entry.first);
    }
    std::vector<LanceBlobFile*> blob_handles(blob_row_ids.size(), nullptr);
    Defer close_blob_handles([&] {
        for (auto* blob_handle : blob_handles) {
            if (blob_handle != nullptr) {
                lance_blob_file_close(blob_handle);
            }
        }
    });
    if (lance_dataset_take_blobs(dataset_handle.get(), blob_row_ids.data(), blob_row_ids.size(),
                                 lance_resource.column_name.c_str(), blob_handles.data()) != 0) {
        return lance_error("resolve FILE payload handles");
    }
    // Validate each Blob's length before allocating and reading its payload.
    for (size_t blob_index = 0; blob_index < blob_handles.size(); ++blob_index) {
        RETURN_IF_CANCELLED(runtime_state);
        if (blob_handles[blob_index] == nullptr) {
            return Status::Corruption("Non-NULL FILE resolved to NULL in its pinned snapshot");
        }
        const auto blob_size = lance_blob_file_size(blob_handles[blob_index]);
        if (lance_last_error_code() != LANCE_OK) {
            return lance_error("get FILE payload size");
        }
        if (blob_size > std::numeric_limits<uint32_t>::max()) {
            return Status::InvalidArgument("FILE payload size {} exceeds VARBINARY length capacity",
                                           blob_size);
        }
        const auto payload_size = static_cast<size_t>(blob_size);
        char* payload_data = payload_size == 0 ? nullptr : payload_column->alloc(payload_size);
        if (payload_size != 0 &&
            lance_blob_file_read_range(blob_handles[blob_index], 0,
                                       reinterpret_cast<uint8_t*>(payload_data),
                                       payload_size) != 0) {
            return lance_error("read FILE payload");
        }
        const StringView payload_value(payload_data, static_cast<uint32_t>(payload_size));
        for (const auto output_row_index :
             output_rows_by_blob_row_id.at(blob_row_ids[blob_index])) {
            payload_column->get_data()[output_row_index] = payload_value;
        }
    }
    return Status::OK();
}

} // namespace doris::format::lance
