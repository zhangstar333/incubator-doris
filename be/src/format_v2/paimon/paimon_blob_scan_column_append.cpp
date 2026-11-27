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

#include "format_v2/paimon/paimon_blob_scan_column_append.h"

#include <algorithm>
#include <ranges>

#include "format_v2/paimon/paimon_append_blob_file.h"
#include "format_v2/paimon/paimon_file_content_reader_range.h"
#include "io/io_common.h"

namespace doris::format::paimon {

Status PaimonBlobScanColumnAppend::validate_blob_files(
        std::vector<const TPaimonBlobFileDesc*>* blob_file_descriptors, int64_t anchor_first_row_id,
        int64_t anchor_row_count) {
    if (blob_file_descriptors->empty()) {
        return Status::InvalidArgument("Cannot validate an empty managed BLOB file group");
    }
    for (const auto* blob_file : *blob_file_descriptors) {
        if (!blob_file->__isset.path || !blob_file->__isset.file_size ||
            !blob_file->__isset.first_row_id || !blob_file->__isset.row_count ||
            blob_file->path.empty() || blob_file->file_size < 5 || blob_file->row_count <= 0 ||
            blob_file->first_row_id < anchor_first_row_id ||
            blob_file->first_row_id > anchor_first_row_id + anchor_row_count ||
            blob_file->row_count >
                    anchor_first_row_id + anchor_row_count - blob_file->first_row_id) {
            return Status::InvalidArgument("Invalid managed BLOB file descriptor");
        }
    }
    std::ranges::sort(*blob_file_descriptors, {},
                      [](const auto* blob_file) { return blob_file->first_row_id; });
    int64_t next_expected_row_id = anchor_first_row_id;
    for (const auto* blob_file : *blob_file_descriptors) {
        if (blob_file->first_row_id != next_expected_row_id) {
            return Status::Corruption("BLOB column {} has overlapping or missing row ranges",
                                      blob_file->field_id);
        }
        next_expected_row_id += blob_file->row_count;
    }
    if (next_expected_row_id != anchor_first_row_id + anchor_row_count) {
        return Status::Corruption("BLOB column {} does not cover its Parquet anchor",
                                  blob_file_descriptors->front()->field_id);
    }
    return Status::OK();
}

PaimonBlobScanColumnAppend::PaimonBlobScanColumnAppend(GlobalIndex table_output_position,
                                                       const ColumnDefinition& column_definition,
                                                       PaimonBlobOutputMode output_mode)
        : PaimonBlobScanColumn(table_output_position, column_definition, output_mode),
          _field_id(column_definition.get_identifier_field_id()) {}

PaimonBlobScanColumnAppend::~PaimonBlobScanColumnAppend() = default;

Status PaimonBlobScanColumnAppend::prepare_split_impl(const TPaimonFileDesc& split_params) {
    reset_split();
    _anchor_first_row_id = split_params.anchor_first_row_id;
    _anchor_row_count = split_params.anchor_row_count;
    for (const auto& blob_file : split_params.blob_files) {
        if (blob_file.field_id == _field_id) {
            _blob_files.push_back(
                    AppendBlobFileState {.file_descriptor = blob_file, .file_reader = nullptr});
        }
    }
    std::ranges::sort(_blob_files, {},
                      [](const auto& blob_file) { return blob_file.file_descriptor.first_row_id; });
    return Status::OK();
}

void PaimonBlobScanColumnAppend::reset_split() {
    _blob_files.clear();
    _anchor_first_row_id = -1;
    _anchor_row_count = 0;
}

Status PaimonBlobScanColumnAppend::read_values(const ColumnPtr& input_column,
                                               const DataTypePtr& input_column_type,
                                               const ReadContext& read_context,
                                               ColumnPtr* result_column) {
    if (input_column_type->is_nullable() ||
        input_column_type->get_primitive_type() != TYPE_BIGINT) {
        return Status::InternalError("Paimon BLOB ROW_POSITION must be a non-nullable BIGINT");
    }
    const auto& row_positions = assert_cast<const ColumnInt64&>(*input_column);
    for (const auto row_position : row_positions.get_data()) {
        if (row_position < 0 || row_position >= _anchor_row_count) {
            return Status::Corruption("Parquet row position is outside its Paimon row range");
        }
    }
    MutableColumnPtr mutable_result_column;
    RETURN_IF_ERROR(
            read_rows(row_positions, _anchor_first_row_id, read_context, &mutable_result_column));
    *result_column = std::move(mutable_result_column);
    return Status::OK();
}

Status PaimonBlobScanColumnAppend::_open_blob_file(AppendBlobFileState* blob_file,
                                                   const ReadContext& read_context) {
    if (blob_file->file_reader) {
        return Status::OK();
    }
    auto open_result = PaimonFileContentRangeReader::open_file(
            blob_file->file_descriptor.path, blob_file->file_descriptor.file_size, read_context);
    if (!open_result.has_value()) {
        return std::move(open_result).error();
    }
    auto blob_file_reader = std::make_unique<PaimonAppendBlobFileReader>();
    RETURN_IF_ERROR(blob_file_reader->open(std::move(open_result).value(),
                                           blob_file->file_descriptor.row_count,
                                           read_context.io_ctx));
    blob_file->file_reader = std::move(blob_file_reader);
    return Status::OK();
}

Status PaimonBlobScanColumnAppend::read_rows(const ColumnInt64& row_positions,
                                             int64_t anchor_first_row_id,
                                             const ReadContext& read_context,
                                             MutableColumnPtr* result_column) {
    PaimonFileColumnBuilder result_builder(_column_definition.type, _output_mode,
                                           read_context.blob_max_size, row_positions.size(),
                                           read_context.read_source_id);
    AppendBlobFileState* blob_file = nullptr;
    for (const auto row_position : row_positions.get_data()) {
        if (read_context.io_ctx != nullptr && read_context.io_ctx->should_stop) {
            return Status::Cancelled("Paimon BLOB read cancelled");
        }
        if (_blob_files.empty()) {
            // Schema evolution: absent nullable columns are NULL, not empty payloads.
            RETURN_IF_ERROR(result_builder.append_null());
            continue;
        }
        const int64_t logical_row_id = anchor_first_row_id + row_position;
        if (blob_file == nullptr || logical_row_id < blob_file->file_descriptor.first_row_id ||
            logical_row_id - blob_file->file_descriptor.first_row_id >=
                    blob_file->file_descriptor.row_count) {
            auto blob_file_it = std::ranges::upper_bound(
                    _blob_files, logical_row_id, {}, [](const auto& blob_file_entry) {
                        return blob_file_entry.file_descriptor.first_row_id;
                    });
            if (blob_file_it == _blob_files.begin()) {
                return Status::Corruption("BLOB column {} has no file for row {}",
                                          _column_definition.name, logical_row_id);
            }
            blob_file = &*--blob_file_it;
            RETURN_IF_ERROR(_open_blob_file(blob_file, read_context));
        }
        std::optional<PaimonAppendBlobFileReader::ByteRange> payload_range;
        RETURN_IF_ERROR(blob_file->file_reader->get_range(
                logical_row_id - blob_file->file_descriptor.first_row_id, &payload_range));
        if (!payload_range) {
            RETURN_IF_ERROR(result_builder.append_null());
            continue;
        }
        PaimonBlobLocation blob_location {.uri = blob_file->file_descriptor.path,
                                          .offset = payload_range->offset,
                                          .length = payload_range->size};
        RETURN_IF_ERROR(result_builder.append(blob_location, [&](PODArray<char>* payload_buffer) {
            // Retain record framing and CRC verification on the indexed-file path.
            return blob_file->file_reader->read_payload(*payload_range, payload_buffer,
                                                        read_context.io_ctx);
        }));
    }
    *result_column = result_builder.finish();
    return Status::OK();
}

} // namespace doris::format::paimon
