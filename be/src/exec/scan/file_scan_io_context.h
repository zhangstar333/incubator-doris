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

#include "io/io_common.h"
#include "runtime/file_read_io_context.h"

namespace doris {

inline void init_file_scan_io_context(RuntimeState* state, io::IOContext* io_ctx) {
    init_file_read_io_context(state, io_ctx);
}

inline std::shared_ptr<io::IOContext> create_file_scan_io_context(RuntimeState* state) {
    auto io_ctx = std::make_shared<io::IOContext>();
    init_file_scan_io_context(state, io_ctx.get());
    return io_ctx;
}

} // namespace doris
