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

#include <map>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <vector>

#include "format_v2/column_mapper.h"
#include "format_v2/paimon/paimon_blob_scan_column.h"
#include "format_v2/paimon/paimon_file_content_reader_range.h"
#include "runtime/runtime_profile.h"

namespace doris {
class Block;
}

namespace doris::format::paimon {

// Scan entry point: validate Split metadata, select Append/PK column implementations and
// coordinate physical input bindings and FILE results. Follow paimon_blob_scan_column next.
// The content-consumption entry point is PaimonFileContentReader; it does not own scan state.
class PaimonBlobScan {
public:
    using ReadContext = PaimonBlobReadContext;

    PaimonBlobScan();
    ~PaimonBlobScan();
    Status init(const std::vector<ColumnDefinition>& projected_columns,
                const std::map<std::string, std::string>& options,
                RuntimeProfile* profile = nullptr);
    Status prepare_split(const TPaimonFileDesc& split_params);
    void configure_mapper_options(TableColumnMapperOptions* options) const;
    std::span<const LocalColumnId> additional_row_position_columns() const;
    Status prepare_file_inputs(const std::vector<ColumnMapping>& column_mappings,
                               const FileScanRequest& file_scan_request);
    Status read_columns(const Block& file_block, Block* table_block,
                        const ReadContext& read_context);
    // Release current split readers, caches and input positions; retain query configuration.
    void reset();
    bool has_blob_columns() const { return _split_active && !_blob_columns.empty(); }
    bool requires_content_binding() const { return _output_mode == PaimonBlobOutputMode::CONTENT; }

private:
    struct ProjectedBlobColumn {
        GlobalIndex table_output_position;
        ColumnDefinition column_definition;
    };
    // Retained only until the first BLOB Split selects concrete column implementations.
    std::vector<ProjectedBlobColumn> _projected_blob_columns;
    // PK columns borrow this range reader; destroy column implementations before it.
    PaimonFileContentRangeReader _range_reader;
    std::vector<std::unique_ptr<PaimonBlobScanColumn>> _blob_columns;

    // Query-level configuration. Layout information currently arrives on each FE split.
    std::optional<PaimonBlobInputLayout> _input_layout;
    std::set<int32_t> _descriptor_field_ids;
    PaimonBlobOutputMode _output_mode = PaimonBlobOutputMode::CONTENT;
    RuntimeProfile::Counter* _materialize_timer = nullptr;
    bool _initialized = false;

    // Current split/file lifecycle; clearing it never changes the query columns/layout.
    bool _split_active = false;
    bool _file_inputs_prepared = false;
};

} // namespace doris::format::paimon
