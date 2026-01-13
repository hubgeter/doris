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

#include <algorithm>
#include <memory>
#include <vector>

#include "common/cast_set.h"
#include "vec/columns/column_const.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/runtime/writer_assigner.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

class PartitionerBase {
public:
    using HashValType = uint32_t;

    PartitionerBase(HashValType partition_count) : _partition_count(partition_count) {}
    virtual ~PartitionerBase() = default;

    virtual Status init(const std::vector<TExpr>& texprs) = 0;

    virtual Status prepare(RuntimeState* state, const RowDescriptor& row_desc) = 0;

    virtual Status open(RuntimeState* state) = 0;

    virtual Status close(RuntimeState* state) = 0;

    virtual Status do_partitioning(RuntimeState* state, Block* block) const = 0;

    virtual const std::vector<HashValType>& get_channel_ids() const = 0;

    virtual Status clone(RuntimeState* state, std::unique_ptr<PartitionerBase>& partitioner) = 0;

    // use _partition_count as invalid sentinel value. since modulo operation result is [0, partition_count-1]
    HashValType partition_count() const { return _partition_count; }
    // use a individual function to highlight its special meaning
    HashValType invalid_sentinel() const { return partition_count(); }

protected:
    const HashValType _partition_count;
};

class PartitionFunction {
public:
    using HashValType = PartitionerBase::HashValType;

    virtual ~PartitionFunction() = default;

    virtual Status init(const std::vector<TExpr>& texprs) = 0;

    virtual Status prepare(RuntimeState* state, const RowDescriptor& row_desc) = 0;

    virtual Status open(RuntimeState* state) = 0;

    virtual Status close(RuntimeState* state) = 0;

    virtual Status get_partitions(RuntimeState* state, Block* block, size_t partition_count,
                                  std::vector<HashValType>& partitions) const = 0;

    virtual HashValType partition_count() const = 0;

    virtual Status clone(RuntimeState* state,
                         std::unique_ptr<PartitionFunction>& function) const = 0;
};

enum class ShuffleHashMethod {
    CRC32,
    CRC32C,
};

inline void initialize_shuffle_hashes(std::vector<PartitionerBase::HashValType>& hashes,
                                      size_t rows, ShuffleHashMethod method) {
    hashes.resize(rows);
    if (method == ShuffleHashMethod::CRC32C) {
        // Golden ratio seed reduces collision with other hash functions (e.g. hash tables).
        constexpr PartitionerBase::HashValType kShuffleSeed = 0x9E3779B9U;
        std::fill(hashes.begin(), hashes.end(), kShuffleSeed);
    } else {
        std::fill(hashes.begin(), hashes.end(), 0);
    }
}

inline void update_shuffle_hashes(const ColumnPtr& column, const DataTypePtr& type,
                                  PartitionerBase::HashValType* hashes, ShuffleHashMethod method) {
    if (method == ShuffleHashMethod::CRC32C) {
        column->update_crc32c_batch(hashes, nullptr);
    } else {
        column->update_crcs_with_value(hashes, type->get_primitive_type(),
                                       cast_set<PartitionerBase::HashValType>(column->size()));
    }
}

template <typename ChannelIds>
class HashPartitionFunction final : public PartitionFunction {
public:
    HashPartitionFunction(HashValType partition_count, ShuffleHashMethod method)
            : _partition_count(partition_count), _hash_method(method) {}
    ~HashPartitionFunction() override = default;

    Status init(const std::vector<TExpr>& texprs) override {
        return VExpr::create_expr_trees(texprs, _partition_expr_ctxs);
    }

    Status prepare(RuntimeState* state, const RowDescriptor& row_desc) override {
        return VExpr::prepare(_partition_expr_ctxs, state, row_desc);
    }

    Status open(RuntimeState* state) override { return VExpr::open(_partition_expr_ctxs, state); }

    Status close(RuntimeState* state) override { return Status::OK(); }

    Status get_partitions(RuntimeState* state, Block* block, size_t partition_count,
                          std::vector<HashValType>& partitions) const override;

    HashValType partition_count() const override { return _partition_count; }

    Status clone(RuntimeState* state, std::unique_ptr<PartitionFunction>& function) const override;

#ifdef BE_TEST
    VExprContextSPtrs& mutable_partition_expr_ctxs_for_test() { return _partition_expr_ctxs; }
#endif

protected:
    Status _clone_expr_ctxs(RuntimeState* state, VExprContextSPtrs& dst) const {
        dst.resize(_partition_expr_ctxs.size());
        for (size_t i = 0; i < _partition_expr_ctxs.size(); ++i) {
            RETURN_IF_ERROR(_partition_expr_ctxs[i]->clone(state, dst[i]));
        }
        return Status::OK();
    }

    const HashValType _partition_count;
    const ShuffleHashMethod _hash_method;
    VExprContextSPtrs _partition_expr_ctxs;
};

struct ShuffleChannelIds {
    using HashValType = PartitionerBase::HashValType;
    HashValType operator()(HashValType l, size_t r) { return l % r; }
};

struct SpillPartitionChannelIds {
    using HashValType = PartitionerBase::HashValType;
    HashValType operator()(HashValType l, size_t r) { return ((l >> 16) | (l << 16)) % r; }
};

static inline PartitionerBase::HashValType crc32c_shuffle_mix(PartitionerBase::HashValType h) {
    // Step 1: fold high entropy into low bits
    h ^= h >> 16;
    // Step 2: odd multiplicative scramble (cheap avalanche)
    h *= 0xA5B35705U;
    // Step 3: final fold to break remaining linearity
    h ^= h >> 13;
    return h;
}

// use high 16 bits as channel id to avoid conflict with crc32c hash table
// shuffle hash function same with crc32c hash table(eg join hash table) will lead bad performance
// hash table offten use low 16 bits as bucket index, so we shift 16 bits to high bits to avoid conflict
struct ShiftChannelIds {
    using HashValType = PartitionerBase::HashValType;
    HashValType operator()(HashValType l, size_t r) { return crc32c_shuffle_mix(l) % r; }
};

template <typename ChannelIds>
class Crc32HashPartitioner : public PartitionerBase {
public:
    Crc32HashPartitioner(int partition_count, ShuffleHashMethod method = ShuffleHashMethod::CRC32)
            : PartitionerBase(partition_count),
              _hash_method(method),
              _partition_function(std::make_unique<HashPartitionFunction<ChannelIds>>(
                      static_cast<HashValType>(partition_count), method)),
              _writer_assigner(std::make_unique<IdentityWriterAssigner>()) {}

    ~Crc32HashPartitioner() override = default;

    Status init(const std::vector<TExpr>& texprs) override {
        return _partition_function->init(texprs);
    }

    Status prepare(RuntimeState* state, const RowDescriptor& row_desc) override {
        return _partition_function->prepare(state, row_desc);
    }

    Status open(RuntimeState* state) override { return _partition_function->open(state); }

    Status close(RuntimeState* state) override { return _partition_function->close(state); }

    Status do_partitioning(RuntimeState* state, Block* block) const override;

    const std::vector<HashValType>& get_channel_ids() const override { return _hash_vals; }

    Status clone(RuntimeState* state, std::unique_ptr<PartitionerBase>& partitioner) override;

#ifdef BE_TEST
    VExprContextSPtrs& mutable_partition_expr_ctxs_for_test() {
        auto* function = static_cast<HashPartitionFunction<ChannelIds>*>(_partition_function.get());
        return function->mutable_partition_expr_ctxs_for_test();
    }
#endif

protected:
    const ShuffleHashMethod _hash_method;
    std::unique_ptr<PartitionFunction> _partition_function;
    std::unique_ptr<WriterAssigner> _writer_assigner;
    mutable std::vector<HashValType> _hash_vals;
};

inline void apply_shuffle_channel_ids(std::vector<PartitionerBase::HashValType>& hashes,
                                      size_t partition_count, ShuffleHashMethod method) {
    if (partition_count == 0) {
        return;
    }
    if (method == ShuffleHashMethod::CRC32C) {
        for (auto& hash : hashes) {
            hash = ShiftChannelIds()(hash, partition_count);
        }
    } else {
        for (auto& hash : hashes) {
            hash = ShuffleChannelIds()(hash, partition_count);
        }
    }
}

class Crc32CHashPartitioner : public Crc32HashPartitioner<ShiftChannelIds> {
public:
    Crc32CHashPartitioner(int partition_count)
            : Crc32HashPartitioner<ShiftChannelIds>(partition_count, ShuffleHashMethod::CRC32C) {}

    Status clone(RuntimeState* state, std::unique_ptr<PartitionerBase>& partitioner) override;
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized
