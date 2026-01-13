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
#include <vector>

#include "vec/core/block.h"
#include "vec/runtime/partitioner.h"
#include "vec/runtime/writer_assigner.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
class ScaleWriterPartitioner final : public PartitionerBase {
public:
    ScaleWriterPartitioner(int channel_size, int partition_count, int task_count,
                           int task_bucket_count,
                           long min_partition_data_processed_rebalance_threshold,
                           long min_data_processed_rebalance_threshold)
            : PartitionerBase(partition_count),
              _channel_size(channel_size),
              _task_count(task_count),
              _task_bucket_count(task_bucket_count),
              _min_partition_data_processed_rebalance_threshold(
                      min_partition_data_processed_rebalance_threshold),
              _min_data_processed_rebalance_threshold(min_data_processed_rebalance_threshold) {
        _crc_partitioner =
                std::make_unique<vectorized::Crc32HashPartitioner<vectorized::ShuffleChannelIds>>(
                        _partition_count);
        _writer_assigner = std::make_unique<SkewedWriterAssigner>(
                static_cast<int>(_partition_count), _task_count, _task_bucket_count,
                _min_partition_data_processed_rebalance_threshold,
                _min_data_processed_rebalance_threshold);
    }

    ~ScaleWriterPartitioner() override = default;

    Status init(const std::vector<TExpr>& texprs) override {
        return _crc_partitioner->init(texprs);
    }

    Status prepare(RuntimeState* state, const RowDescriptor& row_desc) override {
        return _crc_partitioner->prepare(state, row_desc);
    }

    Status open(RuntimeState* state) override { return _crc_partitioner->open(state); }

    Status close(RuntimeState* state) override { return _crc_partitioner->close(state); }

    Status do_partitioning(RuntimeState* state, Block* block) const override {
        RETURN_IF_ERROR(_crc_partitioner->do_partitioning(state, block));
        const auto& channel_ids = _crc_partitioner->get_channel_ids();
        _writer_assigner->assign(channel_ids, nullptr, block->rows(), block->bytes(), _hash_vals);

        return Status::OK();
    }

    const std::vector<HashValType>& get_channel_ids() const override { return _hash_vals; }

    Status clone(RuntimeState* state, std::unique_ptr<PartitionerBase>& partitioner) override {
        partitioner = std::make_unique<ScaleWriterPartitioner>(
                _channel_size, (int)_partition_count, _task_count, _task_bucket_count,
                _min_partition_data_processed_rebalance_threshold,
                _min_data_processed_rebalance_threshold);
        return Status::OK();
    }

private:
    int _channel_size;
    std::unique_ptr<PartitionerBase> _crc_partitioner;
    mutable std::unique_ptr<SkewedWriterAssigner> _writer_assigner;
    mutable std::vector<HashValType> _hash_vals;
    const int _task_count;
    const int _task_bucket_count;
    const long _min_partition_data_processed_rebalance_threshold;
    const long _min_data_processed_rebalance_threshold;
};
#include "common/compile_check_end.h"
} // namespace doris::vectorized
