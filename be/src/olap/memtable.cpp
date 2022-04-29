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

#include "olap/memtable.h"

#include "common/logging.h"
#include "olap/row.h"
#include "olap/rowset/column_data_writer.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/schema.h"
#include "runtime/tuple.h"
#include "util/doris_metrics.h"
#include "vec/aggregate_functions/aggregate_function_reader.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/core/field.h"

namespace doris {

MemTable::MemTable(int64_t tablet_id, Schema* schema, const TabletSchema* tablet_schema,
                   const std::vector<SlotDescriptor*>* slot_descs, TupleDescriptor* tuple_desc,
                   KeysType keys_type, RowsetWriter* rowset_writer,
                   const std::shared_ptr<MemTracker>& parent_tracker, bool support_vec)
        : _tablet_id(tablet_id),
          _schema(schema),
          _tablet_schema(tablet_schema),
          _slot_descs(slot_descs),
          _keys_type(keys_type),
          _mem_tracker(MemTracker::create_tracker(-1, "MemTable", parent_tracker)),
          _buffer_mem_pool(new MemPool(_mem_tracker.get())),
          _table_mem_pool(new MemPool(_mem_tracker.get())),
          _schema_size(_schema->schema_size()),
          _rowset_writer(rowset_writer),
          _is_first_insertion(true),
          _agg_functions(schema->num_columns()),
          _mem_usage(0) {
    if (support_vec) {
        _skip_list = nullptr;
        _vec_row_comparator = std::make_shared<RowInBlockComparator>(_schema);
        // TODO: Support ZOrderComparator in the future
        _vec_skip_list = std::make_unique<VecTable>(
                _vec_row_comparator.get(), _table_mem_pool.get(), _keys_type == KeysType::DUP_KEYS);
        _init_columns_offset_by_slot_descs(slot_descs, tuple_desc);
        _block_aggregator =
                std::make_unique<vectorized::BlockAggregator>(_schema, _tablet_schema, true);
        _init_profile();
    } else {
        _vec_skip_list = nullptr;
        if (_keys_type == KeysType::DUP_KEYS) {
            _insert_fn = &MemTable::_insert_dup;
        } else {
            _insert_fn = &MemTable::_insert_agg;
        }
        if (_tablet_schema->has_sequence_col()) {
            _aggregate_two_row_fn = &MemTable::_aggregate_two_row_with_sequence;
        } else {
            _aggregate_two_row_fn = &MemTable::_aggregate_two_row;
        }
        if (tablet_schema->sort_type() == SortType::ZORDER) {
            _row_comparator = std::make_shared<TupleRowZOrderComparator>(
                    _schema, tablet_schema->sort_col_num());
        } else {
            _row_comparator = std::make_shared<RowCursorComparator>(_schema);
        }
        _skip_list = std::make_unique<Table>(_row_comparator.get(), _table_mem_pool.get(),
                                             _keys_type == KeysType::DUP_KEYS);
    }
}
void MemTable::_init_columns_offset_by_slot_descs(const std::vector<SlotDescriptor*>* slot_descs,
                                                  const TupleDescriptor* tuple_desc) {
    for (auto slot_desc : *slot_descs) {
        const auto& slots = tuple_desc->slots();
        for (int j = 0; j < slots.size(); ++j) {
            if (slot_desc->id() == slots[j]->id()) {
                _column_offset.emplace_back(j);
                break;
            }
        }
    }
}

void MemTable::_init_agg_functions(const vectorized::Block* block) {
    for (uint32_t cid = _schema->num_key_columns(); cid < _schema->num_columns(); ++cid) {
        vectorized::AggregateFunctionPtr function =
                _tablet_schema->column(cid).get_aggregate_function({block->get_data_type(cid)},
                                                                   vectorized::AGG_LOAD_SUFFIX);

        DCHECK(function != nullptr);
        _agg_functions[cid] = function;
    }
}

MemTable::~MemTable() {
    std::for_each(_row_in_blocks.begin(), _row_in_blocks.end(), std::default_delete<RowInBlock>());
    _mem_tracker->release(_mem_usage);
    _buffer_mem_pool->free_all();
    _table_mem_pool->free_all();
    MemTracker::memory_leak_check(_mem_tracker.get(), true);
    print_profile();
}

MemTable::RowCursorComparator::RowCursorComparator(const Schema* schema) : _schema(schema) {}

int MemTable::RowCursorComparator::operator()(const char* left, const char* right) const {
    ContiguousRow lhs_row(_schema, left);
    ContiguousRow rhs_row(_schema, right);
    return compare_row(lhs_row, rhs_row);
}

int MemTable::RowInBlockComparator::operator()(const RowInBlock* left,
                                               const RowInBlock* right) const {
    return _pblock->compare_at(left->_row_pos, right->_row_pos, _schema->num_key_columns(),
                               *_pblock, -1);
}

bool MemTable::insert(const vectorized::Block* block, size_t row_pos, size_t num_rows) {
    {
        SCOPED_TIMER(_insert_time);
        if (_is_first_insertion) {
            _is_first_insertion = false;
            auto cloneBlock = block->clone_without_columns();
            _block = std::make_shared<vectorized::MutableBlock>(&cloneBlock);
            if (_keys_type != KeysType::DUP_KEYS) {
                _init_agg_functions(block);
            }
        }
        _block->add_rows(block, row_pos, num_rows);
        _block_bytes_usage += block->allocated_bytes() * num_rows / block->rows();
        // Memtalbe is full, do not flush immediately
        // First try to merge these blocks
        // If the merged memtable is still full or we can not benefit a lot from merge at first
        // Then flush the memtable into disk.
        bool is_flush = false;
        if (is_full()) {
            size_t before_merge_bytes = bytes_allocated();
            _merge();
            size_t after_merged_bytes = bytes_allocated();
            // TODO(weixiang): magic number here, make it configurable later.
            if (is_full() ||
                (after_merged_bytes >= before_merge_bytes * 2 / 3 && _merge_count == 1)) {
                is_flush = true;
            }
        }
        return is_flush;
    }
}

size_t MemTable::bytes_allocated() const {
    return _block_bytes_usage + _block_aggregator->get_bytes_usage();
}

bool MemTable::is_full() const {
    return bytes_allocated() > config::write_buffer_size;
}

void MemTable::_merge() {
    if (_block == nullptr || _keys_type == KeysType::DUP_KEYS) {
        return;
    }
    _sort(false);
    _agg(false);
    _merge_count++;
}

void MemTable::_agg(const bool finalize) {
    {
        SCOPED_TIMER(_agg_time);
        // note that the _block had been sorted before.
        if (_sorted_block == nullptr || _sorted_block->rows() <= 0) {
            return;
        }
        vectorized::Block sorted_block = _sorted_block->to_block();
        _block_aggregator->append_block(&sorted_block);
        _block_aggregator->partial_sort_merged_aggregate();
        if (finalize) {
            _sorted_block.reset();
        } else {
            _sorted_block->clear_column_data();
        }
    }
}

void MemTable::_sort(const bool finalize) {
    {
        SCOPED_TIMER(_sort_time);
        _index_for_sort.resize(_block->rows());
        for (uint32_t i = 0; i < _block->rows(); i++) {
            _index_for_sort[i] = {i, i};
        }

        _sort_block_by_rows();
        _sorted_block = _block->create_same_struct_block(0);
        _append_sorted_block(_block.get(), _sorted_block.get());
        if (finalize) {
            _block.reset();
        } else {
            _block->clear_column_data();
        }
        _block_bytes_usage = 0;
    }
}

void MemTable::_sort_block_by_rows() {
    std::sort(_index_for_sort.begin(), _index_for_sort.end(),
              [this](const MemTable::OrderedIndexItem& left,
                     const MemTable::OrderedIndexItem& right) {
                  int res = _block->compare_at(left.index_in_block, right.index_in_block,
                                               _schema->num_key_columns(), *_block.get(), -1);
                  if (res != 0) {
                      return res < 0;
                  }
                  return left.incoming_index < right.incoming_index;
              });
}

void MemTable::_append_sorted_block(vectorized::MutableBlock* src, vectorized::MutableBlock* dst) {
    size_t row_num = src->rows();
    _sorted_index_in_block.clear();
    _sorted_index_in_block.reserve(row_num);
    for (size_t i = 0; i < row_num; i++) {
        _sorted_index_in_block.push_back(_index_for_sort[i].index_in_block);
    }
    vectorized::Block src_block = src->to_block();
    dst->add_rows(&src_block, _sorted_index_in_block.data(),
                  _sorted_index_in_block.data() + row_num);
}

void MemTable::finalize() {
    {
        SCOPED_TIMER(_finalize_time);
        //TODO(weixiang): check here
        if (_block == nullptr) {
            return;
        }

        if (_keys_type != KeysType::DUP_KEYS) {
            // agg mode
            if (_block->rows() > 0) {
                _merge();
            }
            if (_merge_count > 1) {
                _block = _block_aggregator->get_partial_agged_block();
                _block_aggregator->reset_aggregator();
                _sort(true);
                _agg(true);
            } else {
                _block.reset();
                _sorted_block.reset();
            }

            _block_bytes_usage = 0;
            _sorted_block = _block_aggregator->get_partial_agged_block();

        } else {
            // dup mode
            _sort(true);
        }
    }
}

void MemTable::_insert_one_row_from_block(RowInBlock* row_in_block) {
    _rows++;
    bool overwritten = false;
    if (_keys_type == KeysType::DUP_KEYS) {
        // TODO: dup keys only need sort opertaion. Rethink skiplist is the beat way to sort columns?
        _vec_skip_list->Insert(row_in_block, &overwritten);
        DCHECK(!overwritten) << "Duplicate key model meet overwrite in SkipList";
        return;
    }

    bool is_exist = _vec_skip_list->Find(row_in_block, &_vec_hint);
    if (is_exist) {
        _aggregate_two_row_in_block(row_in_block, _vec_hint.curr->key);
    } else {
        row_in_block->init_agg_places(_agg_functions, _schema->num_key_columns());
        for (auto cid = _schema->num_key_columns(); cid < _schema->num_columns(); cid++) {
            auto col_ptr = _input_mutable_block.mutable_columns()[cid].get();
            auto place = row_in_block->_agg_places[cid];
            _agg_functions[cid]->add(place,
                                     const_cast<const doris::vectorized::IColumn**>(&col_ptr),
                                     row_in_block->_row_pos, nullptr);
        }

        _vec_skip_list->InsertWithHint(row_in_block, is_exist, &_vec_hint);
    }
}

// For non-DUP models, for the data rows passed from the upper layer, when copying the data,
// we first allocate from _buffer_mem_pool, and then check whether it already exists in
// _skiplist.  If it exists, we aggregate the new row into the row in skiplist.
// otherwise, we need to copy it into _table_mem_pool before we can insert it.
void MemTable::_insert_agg(const Tuple* tuple) {
    _rows++;
    uint8_t* tuple_buf = _buffer_mem_pool->allocate(_schema_size);
    ContiguousRow src_row(_schema, tuple_buf);
    _tuple_to_row(tuple, &src_row, _buffer_mem_pool.get());

    bool is_exist = _skip_list->Find((TableKey)tuple_buf, &_hint);
    if (is_exist) {
        (this->*_aggregate_two_row_fn)(src_row, _hint.curr->key);
    } else {
        tuple_buf = _table_mem_pool->allocate(_schema_size);
        ContiguousRow dst_row(_schema, tuple_buf);
        _agg_object_pool.acquire_data(&_agg_buffer_pool);
        copy_row_in_memtable(&dst_row, src_row, _table_mem_pool.get());
        _skip_list->InsertWithHint((TableKey)tuple_buf, is_exist, &_hint);
    }

    // Make MemPool to be reusable, but does not free its memory
    _buffer_mem_pool->clear();
    _agg_buffer_pool.clear();
}

void MemTable::_insert_dup(const Tuple* tuple) {
    _rows++;
    bool overwritten = false;
    uint8_t* tuple_buf = _table_mem_pool->allocate(_schema_size);
    ContiguousRow row(_schema, tuple_buf);
    _tuple_to_row(tuple, &row, _table_mem_pool.get());
    _skip_list->Insert((TableKey)tuple_buf, &overwritten);
    DCHECK(!overwritten) << "Duplicate key model meet overwrite in SkipList";
}

void MemTable::_tuple_to_row(const Tuple* tuple, ContiguousRow* row, MemPool* mem_pool) {
    for (size_t i = 0; i < _slot_descs->size(); ++i) {
        auto cell = row->cell(i);
        const SlotDescriptor* slot = (*_slot_descs)[i];

        bool is_null = tuple->is_null(slot->null_indicator_offset());
        const auto* value = (const char*)tuple->get_slot(slot->tuple_offset());
        _schema->column(i)->consume(&cell, value, is_null, mem_pool, &_agg_buffer_pool);
    }
}

void MemTable::_aggregate_two_row(const ContiguousRow& src_row, TableKey row_in_skiplist) {
    ContiguousRow dst_row(_schema, row_in_skiplist);
    agg_update_row(&dst_row, src_row, _table_mem_pool.get());
}

void MemTable::_aggregate_two_row_with_sequence(const ContiguousRow& src_row,
                                                TableKey row_in_skiplist) {
    ContiguousRow dst_row(_schema, row_in_skiplist);
    agg_update_row_with_sequence(&dst_row, src_row, _tablet_schema->sequence_col_idx(),
                                 _table_mem_pool.get());
}

void MemTable::_aggregate_two_row_in_block(RowInBlock* new_row, RowInBlock* row_in_skiplist) {
    if (_tablet_schema->has_sequence_col()) {
        auto sequence_idx = _tablet_schema->sequence_col_idx();
        auto res = _input_mutable_block.compare_at(row_in_skiplist->_row_pos, new_row->_row_pos,
                                                   sequence_idx, _input_mutable_block, -1);
        // dst sequence column larger than src, don't need to update
        if (res > 0) {
            return;
        }
    }
    // dst is non-sequence row, or dst sequence is smaller
    for (uint32_t cid = _schema->num_key_columns(); cid < _schema->num_columns(); ++cid) {
        auto place = row_in_skiplist->_agg_places[cid];
        auto col_ptr = _input_mutable_block.mutable_columns()[cid].get();
        _agg_functions[cid]->add(place, const_cast<const doris::vectorized::IColumn**>(&col_ptr),
                                 new_row->_row_pos, nullptr);
    }
}
template <bool is_final>
void MemTable::_collect_vskiplist_results() {
    VecTable::Iterator it(_vec_skip_list.get());
    vectorized::Block in_block = _input_mutable_block.to_block();
    if (_keys_type == KeysType::DUP_KEYS) {
        std::vector<int> row_pos_vec;
        DCHECK(in_block.rows() <= std::numeric_limits<int>::max());
        row_pos_vec.reserve(in_block.rows());
        for (it.SeekToFirst(); it.Valid(); it.Next()) {
            row_pos_vec.emplace_back(it.key()->_row_pos);
        }
        _output_mutable_block.add_rows(&in_block, row_pos_vec.data(),
                                       row_pos_vec.data() + in_block.rows());
    } else {
        size_t idx = 0;
        for (it.SeekToFirst(); it.Valid(); it.Next()) {
            auto& block_data = in_block.get_columns_with_type_and_name();
            // move key columns
            for (size_t i = 0; i < _schema->num_key_columns(); ++i) {
                _output_mutable_block.get_column_by_position(i)->insert_from(
                        *block_data[i].column.get(), it.key()->_row_pos);
            }
            // get value columns from agg_places
            for (size_t i = _schema->num_key_columns(); i < _schema->num_columns(); ++i) {
                auto function = _agg_functions[i];
                function->insert_result_into(it.key()->_agg_places[i],
                                             *(_output_mutable_block.get_column_by_position(i)));
                if constexpr (is_final) {
                    function->destroy(it.key()->_agg_places[i]);
                }
            }
            if constexpr (!is_final) {
                // re-index the row_pos in VSkipList
                it.key()->_row_pos = idx;
                idx++;
            }
        }
        if constexpr (!is_final) {
            // if is not final, we collect the agg results to input_block and then continue to insert
            size_t shrunked_after_agg = _output_mutable_block.allocated_bytes();
            _mem_tracker->consume(shrunked_after_agg - _mem_usage);
            _mem_usage = shrunked_after_agg;
            _input_mutable_block.swap(_output_mutable_block);
            //TODO(weixang):opt here.
            std::unique_ptr<vectorized::Block> empty_input_block =
                    in_block.create_same_struct_block(0);
            _output_mutable_block =
                    vectorized::MutableBlock::build_mutable_block(empty_input_block.get());
            _output_mutable_block.clear_column_data();
        }
    }
}

void MemTable::shrink_memtable_by_agg() {
    if (_keys_type == KeysType::DUP_KEYS) {
        return;
    }
    _collect_vskiplist_results<false>();
}

bool MemTable::is_flush() const {
    return memory_usage() >= config::write_buffer_size;
}

bool MemTable::need_to_agg() {
    return _keys_type == KeysType::DUP_KEYS ? is_flush()
                                            : memory_usage() >= config::memtable_max_buffer_size;
}

Status MemTable::flush() {
    if (!_skip_list) {
        finalize();
        if (_sorted_block == nullptr) {
            return Status::OK();
        }
    }

    VLOG_CRITICAL << "begin to flush memtable for tablet: " << _tablet_id
                  << ", memsize: " << memory_usage() << ", rows: " << _rows;
    int64_t duration_ns = 0;
    RETURN_NOT_OK(_do_flush(duration_ns));
    DorisMetrics::instance()->memtable_flush_total->increment(1);
    DorisMetrics::instance()->memtable_flush_duration_us->increment(duration_ns / 1000);
    VLOG_CRITICAL << "after flush memtable for tablet: " << _tablet_id
                  << ", flushsize: " << _flush_size;
    return Status::OK();
}

Status MemTable::_do_flush(int64_t& duration_ns) {
    SCOPED_RAW_TIMER(&duration_ns);
    if (_skip_list) {
        Status st = _rowset_writer->flush_single_memtable(this, &_flush_size);
        if (st.precise_code() == OLAP_ERR_FUNC_NOT_IMPLEMENTED) {
            // For alpha rowset, we do not implement "flush_single_memtable".
            // Flush the memtable like the old way.
            Table::Iterator it(_skip_list.get());
            for (it.SeekToFirst(); it.Valid(); it.Next()) {
                char* row = (char*)it.key();
                ContiguousRow dst_row(_schema, row);
                agg_finalize_row(&dst_row, _table_mem_pool.get());
                RETURN_NOT_OK(_rowset_writer->add_row(dst_row));
            }
            RETURN_NOT_OK(_rowset_writer->flush());
        } else {
            RETURN_NOT_OK(st);
        }
    } else {
        vectorized::Block block = _sorted_block->to_block();
        // beta rowset flush parallel, segment write add block is not
        // thread safe, so use tmp variable segment_write instead of
        // member variable
        RETURN_NOT_OK(_rowset_writer->flush_single_memtable(&block));
        _flush_size = block.allocated_bytes();
    }
    return Status::OK();
}

Status MemTable::close() {
    return flush();
}

MemTable::Iterator::Iterator(MemTable* memtable)
        : _mem_table(memtable), _it(memtable->_skip_list.get()) {}

void MemTable::Iterator::seek_to_first() {
    _it.SeekToFirst();
}

bool MemTable::Iterator::valid() {
    return _it.Valid();
}

void MemTable::Iterator::next() {
    _it.Next();
}

ContiguousRow MemTable::Iterator::get_current_row() {
    char* row = (char*)_it.key();
    ContiguousRow dst_row(_mem_table->_schema, row);
    agg_finalize_row(&dst_row, _mem_table->_table_mem_pool.get());
    return dst_row;
}

} // namespace doris
