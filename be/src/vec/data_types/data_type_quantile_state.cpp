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

#include "vec/data_types/data_type_quantile_state.h"

#include "vec/columns/column_complex.h"
#include "vec/common/assert_cast.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

// binary: <size array> | <quantile_state array>
//  <size array>: column num | quantile_state size | quantile_state size | ...
//  <quantile_state array>: quantile_state1 | quantile_state2 | ...
int64_t DataTypeQuantileState::get_uncompressed_serialized_bytes(const IColumn& column) const {
    auto ptr = column.convert_to_full_column_if_const();
    auto& data_column = assert_cast<const ColumnQuantileState&>(*ptr);

    auto allocate_len_size = sizeof(size_t) * (column.size() + 1);
    auto allocate_content_size = 0;
    for (size_t i = 0; i < column.size(); ++i) {
        auto& quantile_state = const_cast<QuantileState<double>&>(data_column.get_element(i));
        allocate_content_size += quantile_state.get_serialized_size();
    }

    return allocate_len_size + allocate_content_size;
}

char* DataTypeQuantileState::serialize(const IColumn& column, char* buf) const {
    auto ptr = column.convert_to_full_column_if_const();
    auto& data_column = assert_cast<const ColumnQuantileState&>(*ptr);

    // serialize the quantile size array, column num saves at index 0
    const auto column_num = column.size();
    size_t quantile_size_array[column_num + 1];
    quantile_size_array[0] = column_num;
    for (size_t i = 0; i < column.size(); ++i) {
        auto& quantile_state = const_cast<QuantileState<double>&>(data_column.get_element(i));
        quantile_size_array[i + 1] = quantile_state.get_serialized_size();
    }
    auto allocate_len_size = sizeof(size_t) * (column_num + 1);
    memcpy(buf, quantile_size_array, allocate_len_size);
    buf += allocate_len_size;
    // serialize each quantile_state obj
    for (size_t i = 0; i < column_num; ++i) {
        auto& quantile_state = const_cast<QuantileState<double>&>(data_column.get_element(i));
        quantile_state.serialize((uint8_t*)buf);
        buf += quantile_size_array[i + 1];
    }

    return buf;
}

const char* DataTypeQuantileState::deserialize(const char* buf, IColumn* column) const {
    auto& data_column = assert_cast<ColumnQuantileState&>(*column);
    auto& data = data_column.get_data();

    // deserialize the quantile_state size array
    size_t column_num = *reinterpret_cast<const size_t*>(buf);
    buf += sizeof(size_t);
    size_t quantile_size_array[column_num];
    memcpy(quantile_size_array, buf, sizeof(size_t) * column_num);
    buf += sizeof(size_t) * column_num;
    // deserialize each quantile_state
    data.resize(column_num);
    for (int i = 0; i < column_num ; ++i) {
        data[i].deserialize(Slice(buf, quantile_size_array[i]));
        buf += quantile_size_array[i];
    }
    
    return buf;
}

MutableColumnPtr DataTypeQuantileState::create_column() const {
    return ColumnQuantileState::create();
}

void DataTypeQuantileState::serialize_as_stream(const QuantileState<double>& cvalue, BufferWritable& buf) {
    auto& value = const_cast<QuantileState<double>&>(cvalue);
    std::string memory_buffer;
    size_t bytesize = value.get_serialized_size();
    memory_buffer.resize(bytesize);
    value.serialize(reinterpret_cast<uint8_t*>(memory_buffer.data()));
    write_string_binary(memory_buffer, buf);
}

void DataTypeQuantileState::deserialize_as_stream(QuantileState<double>& value, BufferReadable& buf) {
    StringRef ref;
    read_string_binary(ref, buf);
    value.deserialize(Slice(ref.data, ref.size));
}

void DataTypeQuantileState::to_string(const class doris::vectorized::IColumn& column, size_t row_num,
        doris::vectorized::BufferWritable& ostr) const {
    // no use to use const_cast, use assert_cast instead
    auto& data = const_cast<QuantileState<double>&>(assert_cast<const ColumnQuantileState&>(column).get_element(row_num));
    std::string result(data.get_serialized_size(), '0');
    data.serialize((uint8_t*)result.data());

    ostr.write(result.data(), result.size());
}
} // namespace doris::vectorized
