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

#include "format/table/paimon_reader.h"

#include <vector>

#include "common/status.h"
#include "format/table/deletion_vector_reader.h"
#include "runtime/runtime_state.h"

namespace doris {

// ============================================================================
// PaimonOrcReader
// ============================================================================
void PaimonOrcReader::_init_paimon_profile() {
    static const char* paimon_profile = "PaimonProfile";
    ADD_TIMER(get_profile(), paimon_profile);
    _paimon_profile.num_delete_rows =
            ADD_CHILD_COUNTER(get_profile(), "NumDeleteRows", TUnit::UNIT, paimon_profile);
    _paimon_profile.delete_files_read_time =
            ADD_CHILD_TIMER(get_profile(), "DeleteFileReadTime", paimon_profile);
    _paimon_profile.parse_deletion_vector_time =
            ADD_CHILD_TIMER(get_profile(), "ParseDeletionVectorTime", paimon_profile);
}

Status PaimonOrcReader::on_before_init_reader(ReaderInitContext* ctx) {
    _column_descs = ctx->column_descs;
    _fill_col_name_to_block_idx = ctx->col_name_to_block_idx;
    const orc::Type* orc_type_ptr = nullptr;
    RETURN_IF_ERROR(get_file_type(&orc_type_ptr));

    RETURN_IF_ERROR(gen_table_info_node_by_field_id(
            get_scan_params(), get_scan_range().table_format_params.paimon_params.schema_id,
            get_tuple_descriptor(), orc_type_ptr));
    ctx->table_info_node = table_info_node_ptr;

    bool has_partition_from_path = false;
    std::unordered_set<std::string> partition_col_names;
    if (ctx->range->__isset.columns_from_path_keys) {
        partition_col_names.insert(ctx->range->columns_from_path_keys.begin(),
                                   ctx->range->columns_from_path_keys.end());
    }
    std::map<std::string, uint64_t> file_column_name_idx_map;
    for (uint64_t idx = 0; idx < orc_type_ptr->getSubtypeCount(); idx++) {
        file_column_name_idx_map.emplace(orc_type_ptr->getFieldName(idx), idx);
    }

    for (auto& desc : *ctx->column_descs) {
        if (desc.category == ColumnCategory::REGULAR) {
            // `table_info_node` is built from the table/history schema. For normal Paimon
            // writes, partition columns are physically materialized in data files, so the
            // schema usually matches the file layout and these columns can be read from the
            // file as regular columns. For Hive-migrated Paimon tables, legacy files may not
            // contain those partition columns even though they still exist in schema. When
            // the mapped file column is absent, reclassify the column as PARTITION_KEY and
            // fill it from `columns_from_path` instead of reading it from the file.
            // This is similar to Iceberg partition fallback.
            if (partition_col_names.contains(desc.name) &&
                table_info_node_ptr->children_column_exists(desc.name)) {
                if (!file_column_name_idx_map.contains(
                            table_info_node_ptr->children_file_column_name(desc.name))) {
                    desc.category = ColumnCategory::PARTITION_KEY;
                    has_partition_from_path = true;
                    continue;
                }
            }
            ctx->column_names.push_back(desc.name);
        } else if (desc.category == ColumnCategory::GENERATED) {
            ctx->column_names.push_back(desc.name);
        }
    }
    if (has_partition_from_path) {
        RETURN_IF_ERROR(_extract_partition_values(*ctx->range, ctx->tuple_descriptor,
                                                  _fill_partition_values));
    }
    return Status::OK();
}

Status PaimonOrcReader::on_after_init_reader(ReaderInitContext* /*ctx*/) {
    return _init_deletion_vector();
}

Status PaimonOrcReader::_init_deletion_vector() {
    const auto& table_desc = get_scan_range().table_format_params.paimon_params;
    if (!table_desc.__isset.deletion_file) {
        return Status::OK();
    }

    // Cannot do count push down if there are delete files
    if (!get_scan_range().table_format_params.paimon_params.__isset.row_count) {
        set_push_down_agg_type(TPushAggOp::NONE);
    }
    const auto& deletion_file = table_desc.deletion_file;

    Status create_status = Status::OK();

    std::string key;
    key.resize(deletion_file.path.size() + sizeof(deletion_file.offset));
    memcpy(key.data(), deletion_file.path.data(), deletion_file.path.size());
    memcpy(key.data() + deletion_file.path.size(), &deletion_file.offset,
           sizeof(deletion_file.offset));

    SCOPED_TIMER(_paimon_profile.delete_files_read_time);
    using DeleteRows = std::vector<int64_t>;
    _delete_rows = _kv_cache->get<DeleteRows>(key, [&]() -> DeleteRows* {
        auto* delete_rows = new DeleteRows;

        TFileRangeDesc delete_range;
        delete_range.__set_fs_name(get_scan_range().fs_name);
        delete_range.path = deletion_file.path;
        delete_range.start_offset = deletion_file.offset;
        delete_range.size = deletion_file.length + 4;
        delete_range.file_size = -1;

        DeletionVectorReader dv_reader(get_state(), get_profile(), get_scan_params(), delete_range,
                                       get_io_ctx());
        create_status = dv_reader.open();
        if (!create_status.ok()) [[unlikely]] {
            return nullptr;
        }

        size_t bytes_read = deletion_file.length + 4;
        std::vector<char> buffer(bytes_read);
        create_status = dv_reader.read_at(deletion_file.offset, {buffer.data(), bytes_read});
        if (!create_status.ok()) [[unlikely]] {
            return nullptr;
        }

        const char* buf = buffer.data();
        uint32_t actual_length;
        std::memcpy(reinterpret_cast<char*>(&actual_length), buf, 4);
        std::reverse(reinterpret_cast<char*>(&actual_length),
                     reinterpret_cast<char*>(&actual_length) + 4);
        buf += 4;
        if (actual_length != bytes_read - 4) [[unlikely]] {
            create_status = Status::RuntimeError(
                    "DeletionVector deserialize error: length not match, "
                    "actual length: {}, expect length: {}",
                    actual_length, bytes_read - 4);
            return nullptr;
        }
        uint32_t magic_number;
        std::memcpy(reinterpret_cast<char*>(&magic_number), buf, 4);
        std::reverse(reinterpret_cast<char*>(&magic_number),
                     reinterpret_cast<char*>(&magic_number) + 4);
        buf += 4;
        const static uint32_t MAGIC_NUMBER = 1581511376;
        if (magic_number != MAGIC_NUMBER) [[unlikely]] {
            create_status = Status::RuntimeError(
                    "DeletionVector deserialize error: invalid magic number {}", magic_number);
            return nullptr;
        }

        roaring::Roaring roaring_bitmap;
        SCOPED_TIMER(_paimon_profile.parse_deletion_vector_time);
        try {
            roaring_bitmap = roaring::Roaring::readSafe(buf, bytes_read - 4);
        } catch (const std::runtime_error& e) {
            create_status = Status::RuntimeError(
                    "DeletionVector deserialize error: failed to deserialize roaring bitmap, {}",
                    e.what());
            return nullptr;
        }
        delete_rows->reserve(roaring_bitmap.cardinality());
        for (auto it = roaring_bitmap.begin(); it != roaring_bitmap.end(); it++) {
            delete_rows->push_back(*it);
        }
        COUNTER_UPDATE(_paimon_profile.num_delete_rows, delete_rows->size());
        return delete_rows;
    });
    RETURN_IF_ERROR(create_status);
    if (!_delete_rows->empty()) [[likely]] {
        set_position_delete_rowids(_delete_rows);
    }
    return Status::OK();
}

// ============================================================================
// PaimonParquetReader
// ============================================================================
void PaimonParquetReader::_init_paimon_profile() {
    static const char* paimon_profile = "PaimonProfile";
    ADD_TIMER(get_profile(), paimon_profile);
    _paimon_profile.num_delete_rows =
            ADD_CHILD_COUNTER(get_profile(), "NumDeleteRows", TUnit::UNIT, paimon_profile);
    _paimon_profile.delete_files_read_time =
            ADD_CHILD_TIMER(get_profile(), "DeleteFileReadTime", paimon_profile);
    _paimon_profile.parse_deletion_vector_time =
            ADD_CHILD_TIMER(get_profile(), "ParseDeletionVectorTime", paimon_profile);
}

Status PaimonParquetReader::on_before_init_reader(ReaderInitContext* ctx) {
    _column_descs = ctx->column_descs;
    _fill_col_name_to_block_idx = ctx->col_name_to_block_idx;
    const FieldDescriptor* field_desc = nullptr;
    RETURN_IF_ERROR(get_file_metadata_schema(&field_desc));
    DCHECK(field_desc != nullptr);

    RETURN_IF_ERROR(gen_table_info_node_by_field_id(
            get_scan_params(), get_scan_range().table_format_params.paimon_params.schema_id,
            get_tuple_descriptor(), *field_desc));
    ctx->table_info_node = table_info_node_ptr;

    bool has_partition_from_path = false;
    std::unordered_set<std::string> partition_col_names;
    if (ctx->range->__isset.columns_from_path_keys) {
        partition_col_names.insert(ctx->range->columns_from_path_keys.begin(),
                                   ctx->range->columns_from_path_keys.end());
    }

    auto parquet_fields_schema = field_desc->get_fields_schema();
    std::map<std::string, size_t> file_column_name_idx_map;
    for (size_t idx = 0; idx < parquet_fields_schema.size(); idx++) {
        file_column_name_idx_map.emplace(parquet_fields_schema[idx].name, idx);
    }

    for (auto& desc : *ctx->column_descs) {
        if (desc.category == ColumnCategory::REGULAR) {
            // `table_info_node` is built from the table/history schema. For normal Paimon
            // writes, partition columns are physically materialized in data files, so the
            // schema usually matches the file layout and these columns can be read from the
            // file as regular columns. For Hive-migrated Paimon tables, legacy files may not
            // contain those partition columns even though they still exist in schema. When
            // the mapped file column is absent, reclassify the column as PARTITION_KEY and
            // fill it from `columns_from_path` instead of reading it from the file.
            // This is similar to Iceberg partition fallback.
            if (partition_col_names.contains(desc.name) &&
                table_info_node_ptr->children_column_exists(desc.name)) {
                if (!file_column_name_idx_map.contains(
                            table_info_node_ptr->children_file_column_name(desc.name))) {
                    desc.category = ColumnCategory::PARTITION_KEY;
                    has_partition_from_path = true;
                    continue;
                }
            }
            ctx->column_names.push_back(desc.name);
        } else if (desc.category == ColumnCategory::GENERATED) {
            ctx->column_names.push_back(desc.name);
        }
    }
    if (has_partition_from_path) {
        RETURN_IF_ERROR(_extract_partition_values(*ctx->range, ctx->tuple_descriptor,
                                                  _fill_partition_values));
    }

    return Status::OK();
}

Status PaimonParquetReader::on_after_init_reader(ReaderInitContext* /*ctx*/) {
    return _init_deletion_vector();
}

Status PaimonParquetReader::_init_deletion_vector() {
    const auto& table_desc = get_scan_range().table_format_params.paimon_params;
    if (!table_desc.__isset.deletion_file) {
        return Status::OK();
    }

    if (!get_scan_range().table_format_params.paimon_params.__isset.row_count) {
        set_push_down_agg_type(TPushAggOp::NONE);
    }
    const auto& deletion_file = table_desc.deletion_file;

    Status create_status = Status::OK();

    std::string key;
    key.resize(deletion_file.path.size() + sizeof(deletion_file.offset));
    memcpy(key.data(), deletion_file.path.data(), deletion_file.path.size());
    memcpy(key.data() + deletion_file.path.size(), &deletion_file.offset,
           sizeof(deletion_file.offset));

    SCOPED_TIMER(_paimon_profile.delete_files_read_time);
    using DeleteRows = std::vector<int64_t>;
    _delete_rows = _kv_cache->get<DeleteRows>(key, [&]() -> DeleteRows* {
        auto* delete_rows = new DeleteRows;

        TFileRangeDesc delete_range;
        delete_range.__set_fs_name(get_scan_range().fs_name);
        delete_range.path = deletion_file.path;
        delete_range.start_offset = deletion_file.offset;
        delete_range.size = deletion_file.length + 4;
        delete_range.file_size = -1;

        DeletionVectorReader dv_reader(get_state(), get_profile(), get_scan_params(), delete_range,
                                       get_io_ctx());
        create_status = dv_reader.open();
        if (!create_status.ok()) [[unlikely]] {
            return nullptr;
        }

        size_t bytes_read = deletion_file.length + 4;
        std::vector<char> buffer(bytes_read);
        create_status = dv_reader.read_at(deletion_file.offset, {buffer.data(), bytes_read});
        if (!create_status.ok()) [[unlikely]] {
            return nullptr;
        }

        const char* buf = buffer.data();
        uint32_t actual_length;
        std::memcpy(reinterpret_cast<char*>(&actual_length), buf, 4);
        std::reverse(reinterpret_cast<char*>(&actual_length),
                     reinterpret_cast<char*>(&actual_length) + 4);
        buf += 4;
        if (actual_length != bytes_read - 4) [[unlikely]] {
            create_status = Status::RuntimeError(
                    "DeletionVector deserialize error: length not match, "
                    "actual length: {}, expect length: {}",
                    actual_length, bytes_read - 4);
            return nullptr;
        }
        uint32_t magic_number;
        std::memcpy(reinterpret_cast<char*>(&magic_number), buf, 4);
        std::reverse(reinterpret_cast<char*>(&magic_number),
                     reinterpret_cast<char*>(&magic_number) + 4);
        buf += 4;
        const static uint32_t MAGIC_NUMBER = 1581511376;
        if (magic_number != MAGIC_NUMBER) [[unlikely]] {
            create_status = Status::RuntimeError(
                    "DeletionVector deserialize error: invalid magic number {}", magic_number);
            return nullptr;
        }

        roaring::Roaring roaring_bitmap;
        SCOPED_TIMER(_paimon_profile.parse_deletion_vector_time);
        try {
            roaring_bitmap = roaring::Roaring::readSafe(buf, bytes_read - 4);
        } catch (const std::runtime_error& e) {
            create_status = Status::RuntimeError(
                    "DeletionVector deserialize error: failed to deserialize roaring bitmap, {}",
                    e.what());
            return nullptr;
        }
        delete_rows->reserve(roaring_bitmap.cardinality());
        for (auto it = roaring_bitmap.begin(); it != roaring_bitmap.end(); it++) {
            delete_rows->push_back(*it);
        }
        COUNTER_UPDATE(_paimon_profile.num_delete_rows, delete_rows->size());
        return delete_rows;
    });
    RETURN_IF_ERROR(create_status);
    if (!_delete_rows->empty()) [[likely]] {
        ParquetReader::set_delete_rows(_delete_rows);
    }
    return Status::OK();
}

} // namespace doris
