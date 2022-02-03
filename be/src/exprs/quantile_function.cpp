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

#include "exprs/quantile_function.h"
#include "exprs/anyval_util.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/split.h"
#include "util/quantile_state.h"
#include "util/string_parser.hpp"
#include "util/slice.h"

namespace doris {

using doris_udf::DoubleVal;
using doris_udf::StringVal;

void QuantileStateFunctions::init(){}

void QuantileStateFunctions::quantile_state_init(FunctionContext* ctx, StringVal* dst) {
    dst->is_null = false;
    auto* state = new QuantileState<double>();
    dst->ptr = (uint8_t*) state;
    dst->len = state->get_serialized_size();
}

static StringVal serialize(FunctionContext* ctx, QuantileState<double>* value) {
    StringVal result(ctx, value->get_serialized_size());
    value->serialize(result.ptr);
    return result;
}

StringVal QuantileStateFunctions::to_quantile_state(FunctionContext* ctx, const StringVal& src) {
    QuantileState<double> quantile_state;
    if (ctx->get_num_constant_args() > 0) {
        float* arg = reinterpret_cast<float*>(ctx->get_constant_arg(0));
        quantile_state.set_compression(*arg);
    }
    
    if(!src.is_null) {
        StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
        //TODO(weixiang): support double here
        float float_value = StringParser::string_to_float<float>(
                reinterpret_cast<char*>(src.ptr), src.len, &parse_result);
        if (UNLIKELY(parse_result != StringParser::PARSE_SUCCESS)) {
            std::stringstream error_msg;
            error_msg << "The input: " << std::string(reinterpret_cast<char*>(src.ptr), src.len)
                      << " is not valid, to_bitmap only support bigint value from 0 to "
                         "18446744073709551615 currently";
            ctx->set_error(error_msg.str().c_str());
            return StringVal::null();
        }
        quantile_state.add_value((double)float_value);
    }
    return serialize(ctx, &quantile_state);
}

void QuantileStateFunctions::quantile_union(FunctionContext* ctx, const StringVal& src, StringVal* dst) {
    if(src.is_null) {
        return;
    }
    auto dst_quantile = reinterpret_cast<QuantileState<double>*>(dst->ptr);
    if(src.len == 0) {
        dst_quantile->merge(*reinterpret_cast<QuantileState<double>*>(src.ptr));
    } else {
        QuantileState<double> state(Slice(src.ptr, src.len));
        dst_quantile->merge(state);
    }
}

DoubleVal QuantileStateFunctions::quantile_percent(FunctionContext* ctx, StringVal& src) {
    if (ctx->get_num_constant_args() == 1) {
        //constant args start from index 2
        float arg = *reinterpret_cast<float*>(ctx->get_constant_arg(0));
        if (arg > 1 || arg <0)
        {
            std::stringstream error_msg;
            error_msg<< "The percentile must between 0 and 1, but input is:" << std::to_string(arg);
            ctx->set_error(error_msg.str().c_str());
        }
        
        if (src.len == 0) {
            auto quantile_state = reinterpret_cast<QuantileState<double>*>(src.ptr);
            return {static_cast<double>(quantile_state->get_value_by_percentile(arg))};
        } else {
            QuantileState<double> quantile_state(Slice(src.ptr, src.len));
            return {static_cast<double>(quantile_state.get_value_by_percentile(arg))};
        }


    } else {
        std::stringstream error_msg;
        error_msg << "The input: " << std::string(reinterpret_cast<char*>(src.ptr), src.len)
                    << " is not valid, quantile_percent function support only one constant arg"
                    << "but " <<ctx->get_num_constant_args() << "are given.";
        ctx->set_error(error_msg.str().c_str()); 
    }
    return  DoubleVal::null();
}

}