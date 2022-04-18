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

#include "block_aggregator.h"

namespace doris::vectorized {

BlockAggregator::BlockAggregator(const Schema* schema, bool src_sorted)
        : _schema(schema), _src_sorted(src_sorted) {
    _init_agg_functions();
}

BlockAggregator::~BlockAggregator() {
}

void BlockAggregator::_init_agg_functions() {
    _cols_num = _schema->num_columns();
    _key_cols_num = _schema->num_key_columns();
    _value_cols_num = _cols_num - _key_cols_num;
    //TODO(weixiang): save memory just use value length.
    _agg_functions.resize(_schema->num_columns());
    _agg_places.resize(_value_cols_num);
    for (uint32_t cid = _schema->num_key_columns(); cid < _schema->num_columns(); ++cid) {
        FieldAggregationMethod agg_method = _tablet_schema->column(cid).aggregation();
        std::string agg_name = TabletColumn::get_string_by_aggregation_type(agg_method);
        if (agg_name == "REPLACE") {
            agg_name = "last_value";
        } else {
            agg_name += "_reader";
        }

        std::transform(agg_name.begin(), agg_name.end(), agg_name.begin(),
                       [](unsigned char c) { return std::tolower(c); });

        // create aggregate function
        DataTypes argument_types;
        // TODO(weixiang): 检查这块这么写是否有隐患
        DataTypePtr dtptr = Schema::get_data_type_ptr(_schema->column(cid)->type());
        argument_types.push_back(dtptr);
        Array params;
        AggregateFunctionPtr function =
                AggregateFunctionSimpleFactory::instance().get(
                        agg_name, argument_types, params, dtptr->is_nullable());

        DCHECK(function != nullptr);
        _agg_functions[cid] = function;
    }
}


void BlockAggregator::append_block(Block* block) {
    if (block == nullptr || block->rows() <= 0){
        return;
    }
    if (_is_first_append) {
        // this means it is appending block for the first time
        _aggregated_block = make_shared<MutableBlock>(block);
        _is_first_append = false;
    }
    _agg_data_counters.reserve(_agg_data_counters.size() + block->rows());
    size_t key_num = _schema->num_key_columns();

    size_t same_rows = 1;
    for (size_t i = 0; i < block->rows(); i++) {
        if ( i+i == block->rows() || block->compare_at(i, i+1, key_num, *block, -1) != 0) {
            _agg_data_counters.push_back(same_rows);
            same_rows = 0;
        }
        same_rows++;
    }
    _aggregated_block->add_rows(block, 0, block->rows());
}

/**
 * @brief aggregate sorted block
 * 1. _agg_data_counters save the following N rows to agg in partial sort block
 * 2. first_row_idx records the first row num of rows with the same keys.
 *
 * 
 * TODO(weixiang):
 *  1. refactor function partial_sort_merged_aggregate, 拆成多个函数：init等
 */

void BlockAggregator::partial_sort_merged_aggregate() {
    DCHECK(!_agg_data_counters.empty());
    std::vector<int> first_row_idx; // TODO(weixiang): add into member variables
    std::vector<MutableColumnPtr> aggregated_cols;
    first_row_idx.resize(_agg_data_counters.size());
    int row_pos = _cumulative_agg_num;
    for (size_t i = 0; i < _agg_data_counters.size(); i++) {
        first_row_idx.push_back(row_pos);
        row_pos += _agg_data_counters[i];    
    }
    auto col_ids = _schema->column_ids();
    size_t agged_row_num = first_row_idx.size();
    // for keys:
    for (size_t cid = 0; cid < _key_cols_num; cid++) {
        
        MutableColumnPtr key_col =
                _schema->get_data_type_ptr(*_schema->column(col_ids[cid]))->create_column();
        key_col->insert_indices_from(_aggregated_block->mutable_columns()[cid],
                                     first_row_idx.data(),
                                     first_row_idx.data() + agged_row_num);
        aggregated_cols.push_back(key_col);
    }

    // init agged place for values:
    for (size_t cid = _key_cols_num; cid < _cols_num; cid++) {
        size_t place_size = _agg_functions[cid]->size_of_data();
        _agg_places[cid - _key_cols_num] = new char[place_size * agged_row_num];
        for (auto i = 0; i < agged_row_num; i++) {
           AggregateDataPtr place = _agg_places[cid - _key_cols_num] + place_size*i;
           _agg_functions[cid]->create(place);
        }
        
    }
    
    // do agg
    for (size_t cid = _key_cols_num; cid < _cols_num; cid++) {
        size_t place_size = _agg_functions[cid]->size_of_data();
        auto* src_value_col_ptr = _aggregated_block->mutable_columns()[cid].get();
        
        for (size_t i = 0; i < agged_row_num; i++) {
            AggregateDataPtr place = _agg_places[cid - _key_cols_num] + place * i;
            _agg_functions[cid - _key_cols_num]->add_batch_single_place(
                    _agg_data_counters[i], place,
                    const_cast<const doris::vectorized::IColumn**>(src_value_col_ptr), nullptr);
        }
    }

    // move to result column
    for (size_t value_col_idx = 0; i < _value_cols_num; value_col_idx++) {
        size_t place_size = _agg_functions[cid]->size_of_data();
        MutableColumnPtr dst_value_col_ptr =
                _schema->get_data_type_ptr(*_schema->column(col_ids[value_col_idx + _key_cols_num]))
                        ->create_column();
        for (size_t i = 0; i < first_row_idx.size(); i++) {
            _agg_functions[value_col_idx + _key_cols_num]->insert_result_into(
                    _agg_places[value_col_idx] + i * place_size,
                    *reinterpret_cast<doris::vectorized::IColumn*> dst_value_col_ptr.get());
        }
        aggregated_cols.push_back(dst_value_col_ptr);
    }
    
    _aggregated_block->append_from_columns(aggregated_cols, agged_row_num);
    _agg_data_counters.clear();
    _cumulative_agg_num += agged_row_num;

    for(auto place : _agg_places) {
        // free aggregated memory
        delete[] place;
    }
    

}



size_t BlockAggregator::get_bytes_usage() const{
    return _aggregated_block->allocated_bytes();
}

} // namespace doris::vectorized
