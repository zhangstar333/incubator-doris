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
#include <mutex>
#include <string>
#include <tuple>
#include <vector>

#include "runtime/file_content_reader.h"

struct LanceDataset;

namespace doris {
class ColumnVarbinary;
class RuntimeState;
class TLanceBlobResource;

} // namespace doris

namespace doris::format::lance {

// Owned by the expression's FRAGMENT_LOCAL reader. Clones share opened dataset handles, whose
// lifetime is independent of Scanner EOF. Input/output positions remain call-local.
class LanceFileContentReader final : public FileContentReader {
public:
    static Status validate_resource(const TLanceBlobResource& lance_resource);
    Status read_blob(RuntimeState* runtime_state, const TFileReadResource& file_read_resource,
                     const std::vector<FileReadRequest>& read_requests,
                     ColumnVarbinary* payload_column) override;

private:
    using OutputRowsByBlobRowId = std::map<uint64_t, std::vector<size_t>>;

    struct DatasetKey {
        std::string uri;
        int64_t version;
        std::map<std::string, std::string> storage_options;

        bool operator<(const DatasetKey& other_dataset_key) const {
            return std::tie(uri, version, storage_options) <
                   std::tie(other_dataset_key.uri, other_dataset_key.version,
                            other_dataset_key.storage_options);
        }
    };

    Status _get_or_open_dataset(RuntimeState* runtime_state,
                                const TLanceBlobResource& lance_resource,
                                std::shared_ptr<LanceDataset>* dataset_handle);
    Status _read_source_blobs(RuntimeState* runtime_state, const TLanceBlobResource& lance_resource,
                              const OutputRowsByBlobRowId& output_rows_by_blob_row_id,
                              ColumnVarbinary* payload_column);
    std::mutex _dataset_cache_mutex;
    // URI, pinned version and access options define dataset identity, independently of the column.
    std::map<DatasetKey, std::shared_ptr<LanceDataset>> _datasets_by_open_spec;
};

} // namespace doris::format::lance
