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

#include "format/table/table_format_reader.h"

#include <gtest/gtest.h>

#include <memory>

namespace doris {

class MockBatchAwareReader : public GenericReader {
public:
    void set_batch_size(size_t batch_size) override {
        last_batch_size = batch_size;
        ++set_batch_size_calls;
    }

    Status get_next_block(Block*, size_t*, bool*) override { return Status::OK(); }

    size_t last_batch_size = 0;
    int set_batch_size_calls = 0;
};

class MockTableFormatReader : public TableFormatReader {
public:
    using TableFormatReader::TableFormatReader;

    Status get_next_block_inner(Block*, size_t*, bool*) override { return Status::OK(); }

    Status init_row_filters() override { return Status::OK(); }
};

TEST(TableFormatReaderTest, SetBatchSizePassesThroughToInnerReader) {
    TFileScanRangeParams params;
    TFileRangeDesc range;
    auto inner_reader = std::make_unique<MockBatchAwareReader>();
    auto* inner_reader_ptr = inner_reader.get();
    MockTableFormatReader reader(std::move(inner_reader), nullptr, nullptr, params, range, nullptr,
                                 nullptr);

    reader.set_batch_size(128);
    EXPECT_EQ(inner_reader_ptr->last_batch_size, 128u);
    EXPECT_EQ(inner_reader_ptr->set_batch_size_calls, 1);

    reader.set_batch_size(512);
    EXPECT_EQ(inner_reader_ptr->last_batch_size, 512u);
    EXPECT_EQ(inner_reader_ptr->set_batch_size_calls, 2);
}

} // namespace doris
