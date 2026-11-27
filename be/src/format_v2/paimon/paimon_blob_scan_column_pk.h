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

#include "format_v2/paimon/paimon_blob_scan_column.h"

namespace doris::format::paimon {

class PaimonFileContentRangeReader;

// PK branch: decode descriptors from scalar/array/map physical inputs and build FILE values.
// The Split-wide range reader is borrowed from PaimonBlobScan and outlives this column.
class PaimonBlobScanColumnPk final : public PaimonBlobScanColumn {
public:
    PaimonBlobScanColumnPk(GlobalIndex table_output_position,
                           const ColumnDefinition& column_definition,
                           PaimonBlobOutputMode output_mode,
                           PaimonFileContentRangeReader& range_reader)
            : PaimonBlobScanColumn(table_output_position, column_definition, output_mode),
              _range_reader(range_reader) {}

protected:
    Status prepare_split_impl(const TPaimonFileDesc&) override { return Status::OK(); }
    void reset_split() override {}
    Status read_values(const ColumnPtr& input_column, const DataTypePtr& input_column_type,
                       const ReadContext& read_context, ColumnPtr* result_column) override;

private:
    Status _convert(const ColumnPtr& input_column, const DataTypePtr& input_column_type,
                    const DataTypePtr& result_column_type, const ReadContext& read_context,
                    ColumnPtr* result_column);

    PaimonFileContentRangeReader& _range_reader;
};

} // namespace doris::format::paimon
