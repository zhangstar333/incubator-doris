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

#include <gtest/gtest.h>

#include "core/column/column_file.h"
#include "core/data_type/data_type_file.h"
#include "format_v2/paimon/paimon_blob_column_builder.h"

namespace doris::format::paimon {
TEST(PaimonBlobDeferredTest, BindsRemoteRangesWithoutReadingButPreservesLocalInline) {
    auto type = std::make_shared<DataTypeFile>();
    PaimonBlobColumnBuilder builder(type, PaimonBlobOutputMode::CONTENT, 1, 4, 42);
    int reads = 0;
    auto payload = [&](PODArray<char>* bytes) {
        ++reads;
        bytes->resize(1);
        (*bytes)[0] = 'x';
        return Status::OK();
    };
    ASSERT_TRUE(builder.append({"s3://bucket/a", 9, 500}, payload).ok());
    ASSERT_TRUE(builder.append({"/tmp/a", 0, 1}, payload).ok());
    ASSERT_TRUE(builder.append({"hdfs://nn/b", 4, 0}, payload).ok());
    ASSERT_TRUE(builder.append({"FiLe:///tmp/a", 0, 1}, payload).ok());
    EXPECT_EQ(reads, 2);
    const auto column = builder.finish();
    const auto& files = assert_cast<const ColumnFile&>(*column);
    EXPECT_EQ(files.blob_source_id_at(0), 42);
    EXPECT_EQ(files.blob_source_id_at(1), 0);
    EXPECT_EQ(files.blob_source_id_at(2), 42);
    EXPECT_EQ(files.blob_source_id_at(3), 0);
    EXPECT_TRUE(files.get_column(DataTypeFile::URI_FIELD_INDEX).is_null_at(1));
    EXPECT_FALSE(files.get_column(DataTypeFile::INLINE_FIELD_INDEX).is_null_at(1));
    EXPECT_TRUE(files.get_column(DataTypeFile::INLINE_FIELD_INDEX).is_null_at(2));
}

TEST(PaimonBlobDeferredTest, ExplicitDescriptorsNeverAcquireReadAuthority) {
    PaimonBlobColumnBuilder builder(std::make_shared<DataTypeFile>(),
                                    PaimonBlobOutputMode::DESCRIPTOR, 1, 1, 42);
    ASSERT_TRUE(builder.append({"s3://bucket/a", 0, 100},
                               [](PODArray<char>*) {
                                   ADD_FAILURE() << "Metadata-only FILE must not read content";
                                   return Status::InternalError("unexpected read");
                               })
                        .ok());
    const auto column = builder.finish();
    EXPECT_FALSE(assert_cast<const ColumnFile&>(*column).has_blob_references());
}
} // namespace doris::format::paimon
