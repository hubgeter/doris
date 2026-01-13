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

#include "partitioner.h"

#include "common/cast_set.h"
#include "common/status.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <typename ChannelIds>
Status HashPartitionFunction<ChannelIds>::get_partitions(
        RuntimeState* /*state*/, Block* block, size_t partition_count,
        std::vector<PartitionFunction::HashValType>& partitions) const {
    const size_t rows = block->rows();

    if (rows == 0) {
        partitions.clear();
        return Status::OK();
    }
    if (partition_count == 0) {
        return Status::InternalError("Partition count is zero");
    }

    auto column_to_keep = block->columns();
    const int result_size = cast_set<int>(_partition_expr_ctxs.size());
    std::vector<int> result(result_size);

    initialize_shuffle_hashes(partitions, rows, _hash_method);
    auto* __restrict hashes = partitions.data();
    for (int i = 0; i < result_size; ++i) {
        RETURN_IF_ERROR(_partition_expr_ctxs[i]->execute(block, &result[i]));
    }
    for (int i = 0; i < result_size; ++i) {
        const auto& col_info = block->get_by_position(result[i]);
        const auto& [col, is_const] = unpack_if_const(col_info.column);
        if (is_const) {
            continue;
        }
        update_shuffle_hashes(col, col_info.type, hashes, _hash_method);
    }

    for (size_t i = 0; i < rows; ++i) {
        hashes[i] = ChannelIds()(hashes[i], partition_count);
    }

    Block::erase_useless_column(block, column_to_keep);
    return Status::OK();
}

template <typename ChannelIds>
Status HashPartitionFunction<ChannelIds>::clone(
        RuntimeState* state, std::unique_ptr<PartitionFunction>& function) const {
    auto* new_function = new HashPartitionFunction<ChannelIds>(_partition_count, _hash_method);
    function.reset(new_function);
    return _clone_expr_ctxs(state, new_function->_partition_expr_ctxs);
}

template <typename ChannelIds>
Status Crc32HashPartitioner<ChannelIds>::do_partitioning(RuntimeState* state, Block* block) const {
    RETURN_IF_ERROR(
            _partition_function->get_partitions(state, block, _partition_count, _hash_vals));
    _writer_assigner->assign(_hash_vals, nullptr, block->rows(), block->bytes(), _hash_vals);
    return Status::OK();
}

template <typename ChannelIds>
Status Crc32HashPartitioner<ChannelIds>::clone(RuntimeState* state,
                                               std::unique_ptr<PartitionerBase>& partitioner) {
    auto* new_partitioner = new Crc32HashPartitioner<ChannelIds>(_partition_count, _hash_method);
    partitioner.reset(new_partitioner);
    return _partition_function->clone(state, new_partitioner->_partition_function);
}

Status Crc32CHashPartitioner::clone(RuntimeState* state,
                                    std::unique_ptr<PartitionerBase>& partitioner) {
    auto* new_partitioner = new Crc32CHashPartitioner(_partition_count);
    partitioner.reset(new_partitioner);
    return _partition_function->clone(state, new_partitioner->_partition_function);
}

template class HashPartitionFunction<ShuffleChannelIds>;
template class HashPartitionFunction<SpillPartitionChannelIds>;
template class HashPartitionFunction<ShiftChannelIds>;

template class Crc32HashPartitioner<ShuffleChannelIds>;
template class Crc32HashPartitioner<SpillPartitionChannelIds>;

} // namespace doris::vectorized
