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

#include <functional>
#include <memory>

#include "runtime/string_search.hpp"
#include "runtime/string_value.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_set.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_quantile_state.h"
#include "vec/data_types/data_type_number.h"
#include "vec/exprs/vexpr.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

class FunctionQuantilePercentile : public IFunction {
public:
    size_t get_number_of_arguments() const override {return 2;}

    DataTypePtr get_return_type_impl(const DataTypes& /*agruments*/) const override {
        return std::make_shared<DataTypeFloat64>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t /*input_rows_count*/) override;


protected:


};
class FunctionToQuantileState : public IFunction {
public:
    size_t get_number_of_arguments() const override {return 2;}
    DataTypePtr get_return_type_impl(const DataTypes& /*agruments*/) const override {
        return std::make_shared<DataTypeQuantileState>();
    }
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t /*input_rows_count*/) override;
}

}