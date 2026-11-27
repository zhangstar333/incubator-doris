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

#include <memory>
#include <optional>

#include "format_v2/column_mapper.h"
#include "format_v2/paimon/paimon_blob_read_context.h"
#include "format_v2/paimon/paimon_file_column_builder.h"

namespace doris {
class Block;
class TPaimonFileDesc;
} // namespace doris

namespace doris::format::paimon {

class PaimonFileContentRangeReader;

enum class PaimonBlobInputLayout { ROW_POSITION, DESCRIPTOR_COLUMN };

// Second step after PaimonBlobScan: bind one physical input to one table FILE column.
// The common path owns mapping and result placement. Append/PK subclasses only interpret values;
// for example, Append consumes anchor row positions while PK consumes descriptor bytes.
class PaimonBlobScanColumn {
public:
    using ReadContext = PaimonBlobReadContext;
    virtual ~PaimonBlobScanColumn() = default;

    static std::unique_ptr<PaimonBlobScanColumn> create(PaimonBlobInputLayout input_layout,
                                                        GlobalIndex table_output_position,
                                                        const ColumnDefinition& column_definition,
                                                        PaimonBlobOutputMode output_mode,
                                                        PaimonFileContentRangeReader& range_reader);

    GlobalIndex table_output_position() const { return _table_output_position; }
    Status prepare_split(const TPaimonFileDesc& split_params);
    Status prepare_file_input(const ColumnMapping& column_mapping,
                              const FileScanRequest& file_scan_request,
                              PaimonBlobInputLayout input_layout);
    Status read_column(const Block& file_block, Block* table_block,
                       const ReadContext& read_context);
    // Reset the current input binding and Split resources; keep schema and implementation type.
    void reset();

protected:
    PaimonBlobScanColumn(GlobalIndex table_output_position,
                         const ColumnDefinition& column_definition,
                         PaimonBlobOutputMode output_mode);

    virtual Status prepare_split_impl(const TPaimonFileDesc& split_params) = 0;
    virtual void reset_split() = 0;
    virtual Status read_values(const ColumnPtr& input_column, const DataTypePtr& input_column_type,
                               const ReadContext& read_context, ColumnPtr* result_column) = 0;

    ColumnDefinition _column_definition;
    PaimonBlobOutputMode _output_mode;

private:
    GlobalIndex _table_output_position;
    // File-local Block position, distinct from a table output position or field ID.
    std::optional<LocalIndex> _file_input_position;
    DataTypePtr _input_column_type;
    bool _file_input_prepared = false;
};

} // namespace doris::format::paimon
