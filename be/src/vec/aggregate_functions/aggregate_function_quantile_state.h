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

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/data_type_quantile_state.h"
#include "vec/io/io_helper.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
namespace doris::vectorized {
using DoubleQuantileState = QuantileState<double>;
struct AggregateFunctionQuantileStateUnionOp {
    static constexpr auto name = "quantile_union";

    //TODO(weixiang): here T is fixed as double
    template <typename T>
    static void add(QuantileState<double>& res, const T& data) {
        res.add(data);
    }

    static void add(QuantileState<double>& res, const QuantileState<double>& data) { res.merge(data); }

    static void merge(QuantileState<double>& res, const QuantileState<double>& data) { res.merge(data); }
};

template <typename Op>
struct AggregateFunctionQuantileStateData{
    
    DoubleQuantileState value;

    template <typename T>
    void add(const T& data) {
        Op::add(value, data);
    }

    void merge(const DoubleQuantileState& data) { Op::merge(value, data); }

    void write(BufferWritable& buf) const { DataTypeQuantileState::serialize_as_stream(value, buf); }

    void read(BufferReadable& buf) { DataTypeQuantileState::deserialize_as_stream(value, buf); }

    DoubleQuantileState& get() { return value; }

}

template <typename Op>
class AggregateFunctionQuantileStateOp final
        :public IAggregateFunctionDataHelper<AggregateFunctionQuantileStateData<Op>,
                                                AggregateFunctionQuantileStateOp<Op>> {
public:
    using ResultDataType = DoubleQuantileState;
    using ColVecType = ColumnQuantileState;
    using ColVecResult = ColumnQuantileState;

    String get_name() const override { return Op::name; }

    AggregateFunctionQuantileStateOp(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<AggregateFunctionQuantileStateData<Op>,
                                           AggregateFunctionQuantileStateOp<Op>>(argument_types_, {}) {}

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeQuantileState>(); }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, size_t row_num,
             Arena*) const override {
        const auto& column = static_cast<const ColVecType&>(*columns[0]);
        this->data(place).add(column.get_data()[row_num]);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena*) const override {
        this->data(place).merge(
                const_cast<AggregateFunctionQuantileStateData<Op>&>(this->data(rhs)).get());
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena*) const override {
        this->data(place).read(buf);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& column = static_cast<ColVecResult&>(to);
        column.get_data().push_back(
                const_cast<AggregateFunctionQuantileStateData<Op>&>(this->data(place)).get());
    }
                        
}

AggregateFunctionPtr create_aggregate_function_quantile_union(const std::string& name,
                                                            const DataTypes& argument_types,
                                                            const Array& parameters,
                                                            const bool result_is_nullable);


}