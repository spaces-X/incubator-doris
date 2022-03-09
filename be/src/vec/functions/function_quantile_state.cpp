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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionBitmap.h
// and modified by Doris

#include "vec/functions/function_quantile_state.h"

#include "runtime/string_value.h"
#include "runtime/string_value.hpp"
#include "util/string_parser.hpp"
#include "vec/columns/column_const.h"
#include "vec/columns/column_set.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

Status FunctionQuantilePercentile::execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t /*input_rows_count*/) {
    const auto values_col = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();

    const auto* values = check_and_get_column<ColumnQuantileState>(values_col.get());

    float percentile_value;
    if (!context->is_arg_constant(1) || !values) {
        return Status::InternalError("Not supported input arguments types. It should be used like quantile_percent(col, percentage), where `col` is quantile_state type and percentage is a constant arg.");
    } else {
        percentile_value = reinterpret_cast<const FloatVal*>(context->get_constant_arg(1))->val;
        if (percentile_value > 1 || percentile_value <0)
        {
            return Status::InternalError("Invalid arguments in quantile_percent(column, percentage), where percentage should be in [0,1]");
        }
    }

    auto return_col_res = ColumnVector<Float64>::create();
    auto data = values->get_data();
    auto result_data = return_col_res->get_data();

    size_t size = data.size();
    result_data.reserve(size);
    for (size_t i = 0; i < size; i++) {
        result_data.push_back(data[i].get_explicit_value_by_percentile(percentile_value));
    }
    block.replace_by_position(result, std::move(return_col_res));
    return Status::OK();
}

Status FunctionToQuantileState::execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t /*input_rows_count*/) {
    const auto values_col = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
    const auto* values = check_and_get_column<ColumnString>(values_col.get());
    float compression;
    if (context->is_arg_constant(1)) {
        compression = reinterpret_cast<const FloatVal*>(context->get_constant_arg(1))->val;
    } else {
        compression = 2048;
    }
    if (float < 2048 || float > 10000) {
        return Status::InternalError("Invalid arguments in to_quantile_state(value, compression), where compression should be in [2048, 1000]");
    }

    auto return_col_res = ColumnQuantileState::create();
    auto data = values->get_chars();
    auto offset = values->get_offsets();
    auto result_data = return_col_res->get_data();
    
    size_t size = offset.size();
    result_data.reserve(size);
    for (size_t i = 0; i < size; i++) {
        const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
        size_t str_size = offsets[i] - offsets[i - 1] - 1;
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        double data_value = StringParser::string_to_float<double>(raw_str, str_size, &parse_result);
        if(parse_result != StringParser::PARSE_SUCCESS) {
             return Status::RuntimeError("Can not parser the origin data into doulbe type")
        }
        result_data.emplace_back({compression});
        result_data.back().add_value(data_value);
    }

    block.replace_by_position(result, return_col_res);
    return Status::OK();
}

}