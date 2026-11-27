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

#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string_view>
#include <vector>

#include "common/status.h"

namespace doris {
class ColumnFile;
class ColumnVarbinary;
class RuntimeState;
class TFileReadResource;
// Call-local views into a FILE column; providers must not retain them after read_blob().
struct FileReadRequest {
    uint32_t source_id;
    uint64_t row_id;
    std::string_view uri;
    int64_t offset;
    int64_t length;
    size_t output_row_index;
};

// A provider reader owned by fl_get_binary's FRAGMENT_LOCAL state and shared by its clones.
// Resource definitions stay query-owned; read_blob() borrows them only for the current call.
class FileContentReader {
public:
    enum class Type { LANCE, PAIMON };

    virtual ~FileContentReader() = default;
    static Type get_type(const TFileReadResource& file_read_resource);
    static std::unique_ptr<FileContentReader> create(Type reader_type);
    static Status validate_resources(
            const std::map<int32_t, TFileReadResource>& resources_by_source_id);
    virtual Status read_blob(RuntimeState* runtime_state,
                             const TFileReadResource& file_read_resource,
                             const std::vector<FileReadRequest>& read_requests,
                             ColumnVarbinary* payload_column) = 0;
    static FileReadRequest request_at(const ColumnFile& file_column, size_t row_index);
};

} // namespace doris
