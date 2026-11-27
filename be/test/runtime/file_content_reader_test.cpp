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

#include "runtime/file_content_reader.h"

#include <gtest/gtest.h>

#include <string_view>

#include "core/column/column_file.h"
#include "core/column/column_nullable.h"
#include "core/column/column_varbinary.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "exec/sink/writer/vmysql_result_writer.h"
#include "exprs/function/array/function_array_element.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"
#include "exprs/mock_vexpr.h"
#include "exprs/table_function/vexplode_file.h"
#include "exprs/vexpr_context.h"
#include "format_v2/paimon/paimon_blob_test_util.h"
#include "runtime/runtime_state.h"
#include "testutil/mock/mock_query_context.h"

namespace doris {
namespace {
class FileContentReaderTest : public format::blob_test::PaimonBlobTest {
protected:
    static TFileReadResource range_resource(int id, bool record = false) {
        TPaimonFileReadResource paimon;
        // Production scanners retain inline for local files. Local resources here exercise the
        // exact FileFactory/provider path without requiring an object-store service in BE UT.
        paimon.__set_file_type(TFileType::FILE_LOCAL);
        paimon.__set_validate_record(record);
        TFileReadResource spec;
        spec.__set_source_id(id);
        spec.__set_paimon(paimon);
        return spec;
    }

    static TFileReadResource lance_resource(int id) {
        TLanceBlobResource lance;
        lance.__set_dataset_uri("s3://bucket/data.lance");
        lance.__set_version(7);
        lance.__set_column_name("image");
        TFileReadResource spec;
        spec.__set_source_id(id);
        spec.__set_lance(lance);
        return spec;
    }

    static FileReadRequest request(uint32_t source, std::string_view uri, int64_t offset,
                                   int64_t length, size_t output_row = 0) {
        return {.source_id = source,
                .row_id = 0,
                .uri = uri,
                .offset = offset,
                .length = length,
                .output_row_index = output_row};
    }

    static Field reference(uint32_t source, const std::string& uri, int64_t offset, int64_t size) {
        File file(DataTypeFile::FIELD_COUNT);
        file.blob_source_id = source;
        file[0] = Field::create_field<TYPE_STRING>(uri);
        file[1] = Field::create_field<TYPE_BIGINT>(offset);
        file[2] = Field::create_field<TYPE_BIGINT>(size);
        return Field::create_field<TYPE_FILE>(std::move(file));
    }
};

TEST_F(FileContentReaderTest, ReadsRawAndValidatedRecordsAfterScanningAndDeduplicatesRanges) {
    const auto blob = write_blob("payload.blob", 7, 0, {"hello", ""});
    const std::map<int32_t, TFileReadResource> specs {{1, range_resource(1)},
                                                      {2, range_resource(2, true)}};
    FileContentReader producer;
    ASSERT_TRUE(producer.init(specs).ok());
    auto files = ColumnFile::create();
    files->insert(reference(1, blob.path, 4, 5));
    files->insert(reference(2, blob.path, 4, 5));
    files->insert(reference(1, blob.path, 4, 5));
    files->insert(reference(2, blob.path, 25, 0));
    // A separate consumer needs only query specifications plus the FILE value. No producer
    // scanner, index reader, stream or local reference-ID registry is consulted.
    FileContentReader consumer;
    ASSERT_TRUE(consumer.init(specs).ok());
    std::vector<FileReadRequest> requests;
    for (size_t row = 0; row < files->size(); ++row) {
        requests.push_back(FileContentReader::request_at(*files, row));
    }
    // Exchange/spill transport must preserve the two sidecars and the public byte locator.
    DataTypeFile type;
    std::vector<char> wire(type.get_uncompressed_serialized_bytes(*files, 21));
    type.serialize(*files, wire.data(), 21);
    MutableColumnPtr restored = type.create_column();
    type.deserialize(wire.data(), &restored, 21);
    requests.clear();
    const auto& received = assert_cast<const ColumnFile&>(*restored);
    for (size_t row = 0; row < received.size(); ++row) {
        requests.push_back(FileContentReader::request_at(received, row));
    }
    RuntimeState state;
    auto values = ColumnVarbinary::create(files->size());
    ASSERT_TRUE(consumer.read(&state, requests, values.get()).ok());
    EXPECT_EQ(values->get_data_at(0).to_string(), "hello");
    EXPECT_EQ(values->get_data_at(1).to_string(), "hello");
    EXPECT_EQ(values->get_data()[0].data(), values->get_data()[2].data());
    EXPECT_EQ(values->get_data_at(3).size, 0);
    auto options = state.query_options();
    options.__set_paimon_blob_max_size(4);
    state.set_query_options(options);
    EXPECT_FALSE(consumer.read(&state, requests, values.get()).ok());
}

TEST_F(FileContentReaderTest, RejectsMissingSourcesInvalidRangesAndCorruptAppendRecords) {
    const auto blob = write_blob("payload.blob", 7, 0, {"hello"});
    FileContentReader reader;
    ASSERT_TRUE(reader.init({{1, range_resource(1, true)}}).ok());
    RuntimeState state;
    auto values = ColumnVarbinary::create(1);
    EXPECT_FALSE(reader.read(&state, {request(9, blob.path, 4, 5, 0)}, values.get()).ok());
    EXPECT_FALSE(reader.read(&state, {request(1, blob.path, -1, 5, 0)}, values.get()).ok());
    EXPECT_FALSE(reader.read(&state, {request(1, blob.path, 4, 500, 0)}, values.get()).ok());
    EXPECT_FALSE(reader.read(&state, {request(1, blob.path, 4, 5, 1)}, values.get()).ok());
    auto bytes = format::blob_test::make_blob_file({"hello"});
    bytes[4] ^= 1;
    const auto corrupt = write_blob_bytes("corrupt.blob", 7, 0, 1, bytes);
    EXPECT_FALSE(reader.read(&state, {request(1, corrupt.path, 4, 5, 0)}, values.get()).ok());
}

TEST_F(FileContentReaderTest, MysqlOutputKeepsPaimonAndLanceReferencesWithoutReading) {
    auto query = MockQueryContext::create();
    ASSERT_TRUE(
            query->init_file_read_resources({{1, range_resource(1)}, {2, lance_resource(2)}}).ok());
    RuntimeState state;
    state._query_ctx = query.get();
    auto options = state.query_options();
    options.__set_paimon_blob_max_size(1);
    state.set_query_options(options);
    auto files = ColumnFile::create();
    files->insert(reference(1, "/does/not/exist", 4, 100));
    files->insert(reference(2, "lance-blob://1", 0, 100));
    files->insert(reference(0, "s3://bucket/metadata-only", 0, 100));
    auto type = std::make_shared<DataTypeFile>();
    Block block({{std::move(files), type, "payload"}});
    auto expression = std::make_shared<MockVExpr>();
    expression->data_type() = type;
    const std::string name = "payload";
    EXPECT_CALL(*expression, expr_name()).WillRepeatedly(testing::ReturnRef(name));
    EXPECT_CALL(*expression,
                execute_column_impl(testing::_, testing::_, testing::_, testing::_, testing::_))
            .WillRepeatedly(testing::Invoke([](VExprContext*, const Block* input, const Selector*,
                                               size_t, ColumnPtr& result) {
                result = input->get_by_position(0).column;
                return Status::OK();
            }));
    VExprContextSPtrs expressions {std::make_shared<VExprContext>(expression)};
    auto sinker = std::make_shared<MySQLResultBlockBuffer>(TUniqueId(), &state, 1024);
    VMysqlResultWriter writer(sinker, expressions, nullptr, false);
    ASSERT_TRUE(writer.init(&state).ok());
    // Missing payloads and a low content limit must not affect reference output.
    ASSERT_TRUE(writer.write(&state, block).ok());
    ASSERT_EQ(sinker->_result_batch_queue.size(), 1);
    const auto& rows = sinker->_result_batch_queue.front()->result_batch.rows;
    ASSERT_EQ(rows.size(), 3);
    for (size_t row = 0; row < rows.size(); ++row) {
        EXPECT_NE(rows[row].find("\"inline\":null"), std::string::npos);
        EXPECT_NE(rows[row].find("\"size\":100"), std::string::npos);
    }
    EXPECT_NE(rows[0].find("/does/not/exist"), std::string::npos);
    EXPECT_NE(rows[1].find("lance-blob://1"), std::string::npos);
    EXPECT_NE(rows[2].find("s3://bucket/metadata-only"), std::string::npos);
}

TEST_F(FileContentReaderTest, BinaryGetterReadsBoundContentAndPreservesNullsAndDescriptors) {
    const auto blob = write_blob("payload.blob", 7, 0, {"hello"});
    auto query = MockQueryContext::create();
    ASSERT_TRUE(query->init_file_read_resources({{1, range_resource(1)}}).ok());
    RuntimeState state;
    state._query_ctx = query.get();
    auto files = ColumnFile::create();
    files->insert(reference(1, blob.path, 4, 5));
    files->insert(reference(0, blob.path, 4, 5));
    files->insert(reference(999, "/does/not/exist", 0, 7));
    File inline_value(DataTypeFile::FIELD_COUNT);
    inline_value[DataTypeFile::INLINE_FIELD_INDEX] =
            Field::create_field<TYPE_VARBINARY>(StringView());
    files->insert(Field::create_field<TYPE_FILE>(std::move(inline_value)));
    auto nulls = ColumnUInt8::create(4, 0);
    nulls->get_data()[2] = 1;
    auto type = std::make_shared<DataTypeFile>();
    auto input_type = make_nullable(type);
    auto output_type = type->get_element(DataTypeFile::INLINE_FIELD_INDEX);
    Block block({{ColumnNullable::create(std::move(files), std::move(nulls)), input_type, "file"},
                 {nullptr, output_type, "payload"}});
    auto function = SimpleFunctionFactory::instance().get_function(
            "fl_get_binary", {block.get_by_position(0)}, output_type);
    ASSERT_NE(function, nullptr);
    EXPECT_TRUE(function->is_blockable());
    auto context = FunctionContext::create_context(&state, output_type, {input_type});
    ASSERT_TRUE(function->execute(context.get(), block, {0}, 1, 4).ok());
    const auto& result = block.get_by_position(1).column;
    EXPECT_EQ(result->get_data_at(0).to_string(), "hello");
    EXPECT_TRUE(result->is_null_at(1));
    EXPECT_TRUE(result->is_null_at(2));
    EXPECT_FALSE(result->is_null_at(3));
    EXPECT_EQ(result->get_data_at(3).size, 0);
}

TEST_F(FileContentReaderTest, FieldAccessExposesReferencesWithoutReading) {
    auto query = MockQueryContext::create();
    ASSERT_TRUE(
            query->init_file_read_resources({{1, range_resource(1)}, {2, lance_resource(2)}}).ok());
    RuntimeState state;
    state._query_ctx = query.get();
    auto file_type = std::make_shared<DataTypeFile>();
    auto string_type = std::make_shared<DataTypeString>();
    for (size_t index : {DataTypeFile::INLINE_FIELD_INDEX, DataTypeFile::URI_FIELD_INDEX}) {
        auto files = ColumnFile::create();
        files->insert(reference(1, "/does/not/exist", 4, 5));
        files->insert(reference(2, "lance-blob://1", 0, 5));
        files->insert(reference(0, "s3://bucket/metadata", 0, 5));
        const auto selector =
                Field::create_field<TYPE_STRING>(file_type->get_element_names()[index]);
        Block block({{std::move(files), file_type, "file"},
                     {string_type->create_column_const(3, selector), string_type, "key"},
                     {nullptr, file_type->get_element(index), "value"}});
        auto context = FunctionContext::create_context(&state, file_type->get_element(index),
                                                       {file_type, string_type});
        FunctionArrayElement function;
        ASSERT_TRUE(function.execute_impl(context.get(), block, {0, 1}, 2, 3).ok());
        const auto& result = block.get_by_position(2).column;
        if (index == DataTypeFile::INLINE_FIELD_INDEX) {
            for (size_t row = 0; row < 3; ++row) EXPECT_TRUE(result->is_null_at(row));
        } else {
            EXPECT_EQ(result->get_data_at(0).to_string(), "/does/not/exist");
            EXPECT_EQ(result->get_data_at(1).to_string(), "lance-blob://1");
            EXPECT_EQ(result->get_data_at(2).to_string(), "s3://bucket/metadata");
        }
    }
    auto implementation = std::make_shared<FunctionArrayElement>();
    DefaultFunction file(implementation, {file_type, string_type}, file_type->get_element(0));
    EXPECT_FALSE(file.is_blockable());
    auto integer = std::make_shared<DataTypeInt64>();
    DefaultFunction array(implementation, {std::make_shared<DataTypeArray>(integer), integer},
                          make_nullable(integer));
    EXPECT_FALSE(array.is_blockable());
}

TEST_F(FileContentReaderTest, ExplodeExposesReferencesWithoutReading) {
    auto query = MockQueryContext::create();
    ASSERT_TRUE(
            query->init_file_read_resources({{1, range_resource(1)}, {2, lance_resource(2)}}).ok());
    RuntimeState state;
    state._query_ctx = query.get();
    auto files = ColumnFile::create();
    files->insert(reference(1, "/does/not/exist", 4, 5));
    files->insert(reference(2, "lance-blob://1", 0, 5));
    files->insert(reference(0, "s3://bucket/metadata", 0, 5));
    auto file_type = std::make_shared<DataTypeFile>();
    Block block({{std::move(files), file_type, "file"}});
    auto root = std::make_shared<MockVExpr>();
    auto child = std::make_shared<MockVExpr>();
    EXPECT_CALL(*child,
                execute_column_impl(testing::_, testing::_, testing::_, testing::_, testing::_))
            .WillRepeatedly(testing::Invoke([](VExprContext*, const Block* input, const Selector*,
                                               size_t, ColumnPtr& result) {
                result = input->get_by_position(0).column;
                return Status::OK();
            }));
    root->add_child(child);
    auto context = std::make_shared<VExprContext>(root);
    VExplodeFileTableFunction explode;
    explode.set_expr_context(context);
    ASSERT_TRUE(explode.process_init(&block, &state).ok());
    auto struct_type = std::make_shared<DataTypeStruct>(file_type->get_elements(),
                                                        file_type->get_element_names());
    auto output = struct_type->create_column();
    for (size_t row = 0; row < 3; ++row) {
        explode.process_row(row);
        ASSERT_EQ(explode.get_value(output, 1), 1);
        const auto value = (*output)[row];
        const auto& fields = value.get<TYPE_STRUCT>();
        EXPECT_EQ(fields[DataTypeFile::SIZE_FIELD_INDEX].get<TYPE_BIGINT>(), 5);
        EXPECT_TRUE(fields[DataTypeFile::INLINE_FIELD_INDEX].is_null());
    }
    EXPECT_EQ((*output)[0].get<TYPE_STRUCT>()[0].get<TYPE_STRING>(), "/does/not/exist");
    EXPECT_EQ((*output)[1].get<TYPE_STRUCT>()[0].get<TYPE_STRING>(), "lance-blob://1");
    EXPECT_EQ((*output)[2].get<TYPE_STRUCT>()[0].get<TYPE_STRING>(), "s3://bucket/metadata");
    explode.process_close();
}

TEST_F(FileContentReaderTest, CancellationStopsBeforeOpeningThePayload) {
    FileContentReader reader;
    ASSERT_TRUE(reader.init({{1, range_resource(1)}}).ok());
    RuntimeState state;
    state.cancel(Status::Cancelled("test cancelled"));
    auto values = ColumnVarbinary::create(1);
    const auto status = reader.read(&state, {request(1, "/does/not/exist", 0, 7, 0)}, values.get());
    EXPECT_TRUE(status.is<ErrorCode::CANCELLED>());
}

TEST_F(FileContentReaderTest, RejectsAmbiguousAndMismatchedResourceDeclarations) {
    FileContentReader reader;
    EXPECT_FALSE(reader.init({{2, range_resource(1)}}).ok());
    auto ambiguous = range_resource(1);
    ambiguous.__set_lance(lance_resource(1).lance);
    EXPECT_FALSE(reader.init({{1, ambiguous}}).ok());
    auto unpinned = lance_resource(1);
    unpinned.lance.__set_version(0);
    EXPECT_FALSE(reader.init({{1, unpinned}}).ok());
}
} // namespace
} // namespace doris
