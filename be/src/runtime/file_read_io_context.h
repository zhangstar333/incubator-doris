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

#include "io/io_common.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"

namespace doris {
// Query IO policy is shared by scanner reads and deferred FILE consumers. The supplied context is
// owned by the caller; none of its borrowed pointers may be retained by query-scoped providers.
inline void init_file_read_io_context(RuntimeState* state, io::IOContext* io_ctx) {
    io_ctx->query_id = &state->query_id();
    if (state->query_options().query_type == TQueryType::SELECT) {
        io_ctx->reader_type = ReaderType::READER_QUERY;
        // Missing options from an older FE must not enable a new remote-read policy.
        io_ctx->enable_file_cache_io_adaptor =
                state->query_options().__isset.enable_file_cache_io_adaptor &&
                state->query_options().enable_file_cache_io_adaptor;
        if (auto* query_ctx = state->get_query_ctx(); query_ctx != nullptr) {
            io_ctx->remote_scan_cache_write_limiter = query_ctx->remote_scan_cache_write_limiter();
        }
    }
}

} // namespace doris
