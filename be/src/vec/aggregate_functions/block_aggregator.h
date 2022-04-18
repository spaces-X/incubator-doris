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
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/core/block.h"
#include "olap/schema.h"

namespace doris::vectorized {

class BlockAggregator {

using BlockPtr = std::shard_ptr<Block>;
using MutableBlockPtr = std::shard_ptr<MutableBlock>;


public:
    BlockAggregator(const Schema* schema, bool src_sorted);
    ~BlockAggregator();
    void append_block(Block* block);
    void partial_sort_merged_aggregate();
    void _init_agg_functions();
    size_t get_bytes_usage() const;

    MutableBlockPtr get_partial_agged_block() {
        return _aggregated_block;
    }

    void reset_aggregator() {
        _aggregated_block.reset();
        _agg_data_counters.clear();
        _cumulative_agg_num = 0;
        _is_first_append = true;
    }

private:
    bool _is_first_append = true;
    size_t _key_cols_num;
    size_t _value_cols_num;
    size_t _cumulative_agg_num = 0;
    size_t _cols_num;
    Schema* _schema;
    bool _src_sorted;
    MutableBlockPtr _aggregated_block;
    std::vector<int> _agg_data_counters;
    std::vector<AggregateFunctionPtr> _agg_functions;

    std::vector<AggregateDataPtr> _agg_places;
    
};

} // namespace
