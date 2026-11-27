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

#include <cstring>
#include <map>
#include <memory>
#include <utility>

#include "core/column/column_const.h"
#include "core/column/column_file.h"
#include "core/column/column_nullable.h"
#include "core/column/column_varbinary.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_varbinary.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"
#include "runtime/file_content_reader.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"

namespace doris {

class FunctionFlGetBinary : public IFunction {
    struct FileContentReadState {
        void init(const std::map<int32_t, TFileReadResource>& resources_by_source_id) {
            // Build the provider set before publishing FRAGMENT_LOCAL state to expression clones.
            for (const auto& [source_id, file_read_resource] : resources_by_source_id) {
                const auto reader_type = FileContentReader::get_type(file_read_resource);
                auto [reader_iterator, reader_type_inserted] =
                        readers_by_type.try_emplace(reader_type);
                if (reader_type_inserted) {
                    reader_iterator->second = FileContentReader::create(reader_type);
                }
            }
        }

        Status read_blob(RuntimeState* runtime_state,
                         const std::map<int32_t, TFileReadResource>& resources_by_source_id,
                         const std::vector<FileReadRequest>& read_requests,
                         ColumnVarbinary* payload_column) const {
            std::map<uint32_t, std::vector<FileReadRequest>> requests_by_source_id;
            for (const auto& read_request : read_requests) {
                if (read_request.output_row_index >= payload_column->size()) {
                    return Status::InternalError("FILE read output position is out of bounds");
                }
                requests_by_source_id[read_request.source_id].push_back(read_request);
            }
            for (const auto& [source_id, source_read_requests] : requests_by_source_id) {
                RETURN_IF_CANCELLED(runtime_state);
                const auto resource_iterator =
                        resources_by_source_id.find(static_cast<int32_t>(source_id));
                if (resource_iterator == resources_by_source_id.end()) {
                    return Status::InvalidArgument("FILE resource {} is not declared by FE",
                                                   source_id);
                }
                const auto reader_iterator = readers_by_type.find(
                        FileContentReader::get_type(resource_iterator->second));
                DORIS_CHECK(reader_iterator != readers_by_type.end());
                RETURN_IF_ERROR(
                        reader_iterator->second->read_blob(runtime_state, resource_iterator->second,
                                                           source_read_requests, payload_column));
            }
            return Status::OK();
        }

        // Immutable after init(); each provider owns synchronization for its shared caches.
        std::map<FileContentReader::Type, std::unique_ptr<FileContentReader>> readers_by_type;
    };

public:
    static constexpr auto name = "fl_get_binary";
    static FunctionPtr create() { return std::make_shared<FunctionFlGetBinary>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }
    bool use_default_implementation_for_constants() const override { return true; }
    bool use_default_implementation_for_nulls() const override { return false; }
    bool is_blockable() const override { return true; }

    Status open(FunctionContext* function_context,
                FunctionContext::FunctionStateScope state_scope) override {
        if (state_scope == FunctionContext::FunctionStateScope::FRAGMENT_LOCAL) {
            auto file_content_read_state = std::make_shared<FileContentReadState>();
            // Constant folding of inline FILE values does not have query resources.
            const auto* runtime_state = function_context->state();
            if (runtime_state != nullptr && runtime_state->get_query_ctx() != nullptr) {
                file_content_read_state->init(
                        runtime_state->get_query_ctx()->get_file_read_resources());
            }
            function_context->set_function_state(state_scope, std::move(file_content_read_state));
        }
        return IFunction::open(function_context, state_scope);
    }

    DataTypePtr get_return_type_impl(const DataTypes&) const override {
        return make_nullable(std::make_shared<DataTypeVarbinary>());
    }

    Status execute_impl(FunctionContext* function_context, Block& block,
                        const ColumnNumbers& arguments, uint32_t result_column_index,
                        size_t input_rows_count) const override {
        const auto& input_column = block.get_by_position(arguments[0]).column;
        if (input_column->only_null()) {
            block.replace_by_position(
                    result_column_index,
                    block.get_by_position(result_column_index)
                            .type->create_column_const(input_rows_count, Field()));
            return Status::OK();
        }
        const auto expanded_input_column = input_column->convert_to_full_column_if_const();
        const IColumn* nested_input_column = expanded_input_column.get();
        const NullMap* parent_null_map = nullptr;
        if (const auto* nullable_input_column =
                    check_and_get_column<ColumnNullable>(nested_input_column)) {
            parent_null_map = &nullable_input_column->get_null_map_data();
            nested_input_column = &nullable_input_column->get_nested_column();
        }
        const auto& file_column = assert_cast<const ColumnFile&>(*nested_input_column);
        const auto& inline_payload_column =
                file_column.get_column(DataTypeFile::INLINE_FIELD_INDEX);
        if (!file_column.has_blob_references()) {
            // Inline-only FILE values keep their existing zero-copy result, including payloads
            // beneath parent NULLs. Only the output null map needs to include the parent mask.
            if (parent_null_map == nullptr) {
                block.replace_by_position(
                        result_column_index,
                        file_column.get_column_ptr(DataTypeFile::INLINE_FIELD_INDEX));
            } else {
                auto result_null_map_column = ColumnUInt8::create(input_rows_count);
                for (size_t row_index = 0; row_index < input_rows_count; ++row_index) {
                    result_null_map_column->get_data()[row_index] =
                            (*parent_null_map)[row_index] ||
                            inline_payload_column.is_null_at(row_index);
                }
                block.replace_by_position(
                        result_column_index,
                        ColumnNullable::create(inline_payload_column.get_nested_column_ptr(),
                                               std::move(result_null_map_column)));
            }
            return Status::OK();
        }
        auto payload_column = ColumnVarbinary::create(input_rows_count);
        auto result_null_map_column = ColumnUInt8::create(input_rows_count, 0);
        std::vector<FileReadRequest> read_requests;
        for (size_t row_index = 0; row_index < input_rows_count; ++row_index) {
            if (parent_null_map && (*parent_null_map)[row_index]) {
                result_null_map_column->get_data()[row_index] = 1;
                payload_column->get_data()[row_index] = StringView();
            } else if (!inline_payload_column.is_null_at(row_index)) {
                const auto inline_payload =
                        inline_payload_column.get_nested_column().get_data_at(row_index);
                char* payload_data = inline_payload.size == 0
                                             ? nullptr
                                             : payload_column->alloc(inline_payload.size);
                if (inline_payload.size != 0) {
                    std::memcpy(payload_data, inline_payload.data, inline_payload.size);
                }
                payload_column->get_data()[row_index] =
                        StringView(payload_data, cast_set<uint32_t>(inline_payload.size));
            } else {
                const auto source_id = file_column.blob_source_id_at(row_index);
                if (source_id == 0) {
                    result_null_map_column->get_data()[row_index] = 1;
                    payload_column->get_data()[row_index] = StringView();
                    continue;
                }
                read_requests.push_back(FileContentReader::request_at(file_column, row_index));
            }
        }
        if (!read_requests.empty()) {
            if (function_context == nullptr || function_context->state() == nullptr ||
                function_context->state()->get_query_ctx() == nullptr) {
                return Status::InvalidArgument("External FILE content requires a query context");
            }
            auto* file_content_read_state =
                    static_cast<FileContentReadState*>(function_context->get_function_state(
                            FunctionContext::FunctionStateScope::FRAGMENT_LOCAL));
            DORIS_CHECK(file_content_read_state != nullptr);
            const auto& resources_by_source_id =
                    function_context->state()->get_query_ctx()->get_file_read_resources();
            RETURN_IF_ERROR(file_content_read_state->read_blob(
                    function_context->state(), resources_by_source_id, read_requests,
                    payload_column.get()));
        }
        block.replace_by_position(result_column_index,
                                  ColumnNullable::create(std::move(payload_column),
                                                         std::move(result_null_map_column)));
        return Status::OK();
    }
};

void register_function_fl_get_binary(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionFlGetBinary>();
}

} // namespace doris
