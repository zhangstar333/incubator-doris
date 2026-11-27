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

#include <cstdint>
#include <utility>

#include "core/column/column.h"
#include "core/data_type/data_type.h"
#include "core/pod_array.h"
#include "format_v2/paimon/paimon_blob_location.h"

namespace doris::format::paimon {

// CONTENT returns a bound reference for remote payloads and inline bytes for local payloads.
// Only fl_get_binary reads bound content; explicit DESCRIPTOR stays unbound and metadata-only.
enum class PaimonBlobOutputMode { CONTENT, DESCRIPTOR };

// Builds one FILE leaf column for both Append and PK inputs: references, inline bytes and NULLs.
// Size limits apply when content is read. The batch buffer is reused.
class PaimonFileColumnBuilder {
public:
    PaimonFileColumnBuilder(const DataTypePtr& result_column_type, PaimonBlobOutputMode output_mode,
                            int64_t max_inline_size, size_t row_count, uint32_t read_source_id = 0);
    Status append_null();

    // Keep physical read policy at the source: indexed records validate framing/CRC;
    // descriptors read a byte range. No virtual call or serialized intermediate column.
    template <typename ReadPayload>
    Status append(const PaimonBlobLocation& blob_location, ReadPayload&& read_payload) {
        if (_output_mode == PaimonBlobOutputMode::DESCRIPTOR ||
            (_read_source_id != 0 && is_remote_location(blob_location))) {
            _append_file_reference(blob_location);
        } else {
            RETURN_IF_ERROR(_check_inline_size(blob_location.length));
            RETURN_IF_ERROR(read_payload(&_payload_buffer));
            RETURN_IF_ERROR(_append_inline(blob_location.length));
        }
        return Status::OK();
    }

    MutableColumnPtr finish() { return std::move(_result_column); }

private:
    static bool is_remote_location(const PaimonBlobLocation& blob_location);
    uint32_t _read_source_id;
    Status _check_inline_size(int64_t payload_length) const;
    Status _append_inline(int64_t payload_length);
    void _append_file_reference(const PaimonBlobLocation& blob_location);

    PaimonBlobOutputMode _output_mode;
    int64_t _max_inline_size;
    bool _result_nullable;
    MutableColumnPtr _result_column;
    Field _file_value;
    PODArray<char> _payload_buffer;
};

} // namespace doris::format::paimon
