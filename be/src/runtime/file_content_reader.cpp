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

#include "runtime/file_content_reader.h"

#include "common/logging.h"
#include "core/column/column_file.h"
#include "core/data_type/data_type_file.h"
#include "format_v2/lance/lance_file_content_reader.h"
#include "format_v2/paimon/paimon_file_content_reader.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris {
FileContentReader::Type FileContentReader::get_type(const TFileReadResource& file_read_resource) {
    if (file_read_resource.__isset.lance) {
        return Type::LANCE;
    }
    DORIS_CHECK(file_read_resource.__isset.paimon);
    return Type::PAIMON;
}

std::unique_ptr<FileContentReader> FileContentReader::create(Type reader_type) {
    if (reader_type == Type::LANCE) {
        return std::make_unique<format::lance::LanceFileContentReader>();
    }
    DORIS_CHECK(reader_type == Type::PAIMON);
    return std::make_unique<format::paimon::PaimonFileContentReader>();
}

Status FileContentReader::validate_resources(
        const std::map<int32_t, TFileReadResource>& resources_by_source_id) {
    for (const auto& [source_id, file_read_resource] : resources_by_source_id) {
        if (source_id <= 0 || !file_read_resource.__isset.source_id ||
            file_read_resource.source_id != source_id ||
            file_read_resource.__isset.lance == file_read_resource.__isset.paimon) {
            return Status::InvalidArgument("Incomplete FILE resource {}", source_id);
        }
        if (file_read_resource.__isset.lance) {
            RETURN_IF_ERROR(format::lance::LanceFileContentReader::validate_resource(
                    file_read_resource.lance));
        } else if (!file_read_resource.paimon.__isset.file_type ||
                   !file_read_resource.paimon.__isset.table_type) {
            return Status::InvalidArgument(
                    "Paimon FILE resource requires filesystem and table types");
        }
    }
    return Status::OK();
}

FileReadRequest FileContentReader::request_at(const ColumnFile& file_column, size_t row_index) {
    const auto& uri_column = file_column.get_column(DataTypeFile::URI_FIELD_INDEX);
    const auto& offset_column = file_column.get_column(DataTypeFile::OFFSET_FIELD_INDEX);
    const auto& size_column = file_column.get_column(DataTypeFile::SIZE_FIELD_INDEX);
    const auto uri_value = uri_column.is_null_at(row_index)
                                   ? StringRef()
                                   : uri_column.get_nested_column().get_data_at(row_index);
    return {.source_id = file_column.blob_source_id_at(row_index),
            .row_id = file_column.blob_row_id_at(row_index),
            .uri = std::string_view(uri_value.data, uri_value.size),
            .offset = offset_column.is_null_at(row_index)
                              ? -1
                              : offset_column.get_nested_column().get_int(row_index),
            .length = size_column.is_null_at(row_index)
                              ? -1
                              : size_column.get_nested_column().get_int(row_index),
            .output_row_index = row_index};
}

} // namespace doris
