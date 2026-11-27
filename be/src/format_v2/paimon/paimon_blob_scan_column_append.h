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
#include <memory>
#include <vector>

#include "core/column/column_vector.h"
#include "format_v2/paimon/paimon_blob_scan_column.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris::format::paimon {

class PaimonAppendBlobFileReader;

// Append branch: resolve anchor row positions through companion .blob indexes.
class PaimonBlobScanColumnAppend final : public PaimonBlobScanColumn {
public:
    PaimonBlobScanColumnAppend(GlobalIndex table_output_position,
                               const ColumnDefinition& column_definition,
                               PaimonBlobOutputMode output_mode);
    ~PaimonBlobScanColumnAppend() override;

    // Sort one field's descriptors and validate full anchor coverage without opening its files.
    static Status validate_blob_files(
            std::vector<const TPaimonBlobFileDesc*>* blob_file_descriptors,
            int64_t anchor_first_row_id, int64_t anchor_row_count);
    // Positions are zero-based in the complete anchor file and survive filtering.
    Status read_rows(const ColumnInt64& row_positions, int64_t anchor_first_row_id,
                     const ReadContext& read_context, MutableColumnPtr* result_column);

protected:
    Status prepare_split_impl(const TPaimonFileDesc& split_params) override;
    void reset_split() override;
    Status read_values(const ColumnPtr& input_column, const DataTypePtr& input_column_type,
                       const ReadContext& read_context, ColumnPtr* result_column) override;

private:
    struct AppendBlobFileState {
        TPaimonBlobFileDesc file_descriptor;
        std::unique_ptr<PaimonAppendBlobFileReader> file_reader;
    };
    Status _open_blob_file(AppendBlobFileState* blob_file, const ReadContext& read_context);

    int32_t _field_id;
    int64_t _anchor_first_row_id = -1;
    int64_t _anchor_row_count = 0;
    std::vector<AppendBlobFileState> _blob_files;
};

} // namespace doris::format::paimon
