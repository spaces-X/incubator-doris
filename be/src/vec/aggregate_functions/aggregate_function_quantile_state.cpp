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
#include "vec/aggregate_functions/aggregate_function_quantile_state.h"

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"

namespace doris::vectorized {

AggregateFunctionPtr create_aggregate_function_quantile_union(const std::string& name,
                                                            const DataTypes& argument_types,
                                                            const Array& parameters,
                                                            const bool result_is_nullable) {
    return std::make_shared<AggregateFunctionQuantileStateOp<AggregateFunctionQuantileStateUnionOp>>(
            argument_types);
}

void register_aggregate_function_quantile_state(AggregateFunctionSimpleFactory& factory) {
    factory.regist_function("quantile_union", create_aggregate_function_quantile_union);

}