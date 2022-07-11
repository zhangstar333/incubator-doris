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

#include "factory_helpers.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {
struct Value {
public:
    bool is_null() const { return _is_null; }
    void set_null(bool is_null) { _is_null = is_null; }
    StringRef get_value() const { return _ptr->get_data_at(_offset); }

    void set_value(const IColumn* column, size_t row) {
        _ptr = column;
        _offset = row;
    }
    void reset() {
        _is_null = false;
        _ptr = nullptr;
        _offset = 0;
    }

protected:
    const IColumn* _ptr = nullptr;
    size_t _offset = 0;
    bool _is_null;
};

struct CopiedValue : public Value {
public:
    StringRef get_value() const { return _copied_value; }

    void set_value(const IColumn* column, size_t row) {
        _copied_value = column->get_data_at(row).to_string();
    }

private:
    std::string _copied_value;
};

template <typename T, bool result_is_nullable, bool is_string, typename StoreType = Value>
struct ReaderFirstAndLastData {
public:
    static constexpr bool nullable = result_is_nullable;

    void set_null_if_need() {
        if (!_has_value) {
            this->set_is_null();
        }
    }

    void reset() {
        _data_value.reset();
        _has_value = false;
    }

    void insert_result_into(IColumn& to) const {
        if constexpr (result_is_nullable) {
            if (_data_value.is_null()) {
                auto& col = assert_cast<ColumnNullable&>(to);
                col.insert_default();
            } else {
                auto& col = assert_cast<ColumnNullable&>(to);
                StringRef value = _data_value.get_value();
                col.insert_data(value.data, value.size);
            }
        } else {
            StringRef value = _data_value.get_value();
            to.insert_data(value.data, value.size);
        }
    }

    void set_value(const IColumn** columns, size_t pos) {
        if (columns[0]->is_nullable() &&
            assert_cast<const ColumnNullable*>(columns[0])->is_null_at(pos)) {
            _data_value.set_null(true);
        } else {
            _data_value.set_value(columns[0], pos);
            _data_value.set_null(false);
        }
        _has_value = true;
    }

    void set_is_null() { _data_value.set_null(true); }

    bool has_set_value() { return _has_value; }

private:
    StoreType _data_value;
    bool _has_value = false;
};

template <typename Data>
struct ReaderFunctionFirstData : Data {
    void add(int64_t row, const IColumn** columns) {
        if (this->has_set_value()) {
            return;
        }
        this->set_value(columns, row);
    }
    static const char* name() { return "first_value"; }
};

template <typename Data>
struct ReaderFunctionFirstNonNullData : Data {
    void add(int64_t row, const IColumn** columns) {
        if (this->has_set_value()) {
            return;
        }
        if constexpr (Data::nullable) {
            this->set_null_if_need();
            const auto* nullable_column = assert_cast<const ColumnNullable*>(columns[0]);
            if (nullable_column->is_null_at(row)) {
                return;
            }
        }
        this->set_value(columns, row);
    }
    static const char* name() { return "first_non_null_value"; }
};

template <typename Data>
struct ReaderFunctionLastData : Data {
    void add(int64_t row, const IColumn** columns) { this->set_value(columns, row); }
    static const char* name() { return "last_value"; }
};

template <typename Data>
struct ReaderFunctionLastNonNullData : Data {
    void add(int64_t row, const IColumn** columns) {
        if constexpr (Data::nullable) {
            this->set_null_if_need();
            const auto* nullable_column = assert_cast<const ColumnNullable*>(columns[0]);
            if (nullable_column->is_null_at(row)) {
                return;
            }
        }
        this->set_value(columns, row);
    }

    static const char* name() { return "last_non_null_value"; }
};

template <typename Data>
class ReaderFunctionData final
        : public IAggregateFunctionDataHelper<Data, ReaderFunctionData<Data>> {
public:
    ReaderFunctionData(const DataTypes& argument_types)
            : IAggregateFunctionDataHelper<Data, ReaderFunctionData<Data>>(argument_types, {}),
              _argument_type(argument_types[0]) {}

    String get_name() const override { return Data::name(); }
    DataTypePtr get_return_type() const override { return _argument_type; }

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena* arena) const override {
        LOG(FATAL) << "ReaderFunctionData do not support add_range_single_place";
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        this->data(place).insert_result_into(to);
    }

    void add(AggregateDataPtr place, const IColumn** columns, size_t row_num,
             Arena* arena) const override {
        this->data(place).add(row_num, columns);
    }
    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena*) const override {
        LOG(FATAL) << "ReaderFunctionData do not support merge";
    }
    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {
        LOG(FATAL) << "ReaderFunctionData do not support serialize";
    }
    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena*) const override {
        LOG(FATAL) << "ReaderFunctionData do not support deserialize";
    }

private:
    DataTypePtr _argument_type;
};

template <template <typename> class AggregateFunctionTemplate, template <typename> class Data,
          bool result_is_nullable, bool is_copy = false>
static IAggregateFunction* create_function_single_value(const String& name,
                                                        const DataTypes& argument_types,
                                                        const Array& parameters) {
    using StoreType = std::conditional_t<is_copy, CopiedValue, Value>;

    assert_arity_at_most<3>(name, argument_types);

    auto type = remove_nullable(argument_types[0]);
    WhichDataType which(*type);

#define DISPATCH(TYPE)                                                                     \
    if (which.idx == TypeIndex::TYPE)                                                      \
        return new AggregateFunctionTemplate<                                              \
                Data<ReaderFirstAndLastData<TYPE, result_is_nullable, false, StoreType>>>( \
                argument_types);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (which.is_decimal()) {
        return new AggregateFunctionTemplate<
                Data<ReaderFirstAndLastData<Int128, result_is_nullable, false, StoreType>>>(
                argument_types);
    }
    if (which.is_date_or_datetime()) {
        return new AggregateFunctionTemplate<
                Data<ReaderFirstAndLastData<Int64, result_is_nullable, false, StoreType>>>(
                argument_types);
    }
    if (which.is_date_v2()) {
        return new AggregateFunctionTemplate<
                Data<ReaderFirstAndLastData<UInt32, result_is_nullable, false, StoreType>>>(
                argument_types);
    }
    if (which.is_string_or_fixed_string()) {
        return new AggregateFunctionTemplate<
                Data<ReaderFirstAndLastData<StringRef, result_is_nullable, true, StoreType>>>(
                argument_types);
    }
    DCHECK(false) << "with unknowed type, failed in  create_aggregate_function_" << name;
    return nullptr;
}

template <bool is_nullable, bool is_copy>
AggregateFunctionPtr create_aggregate_function_first(const std::string& name,
                                                     const DataTypes& argument_types,
                                                     const Array& parameters,
                                                     bool result_is_nullable) {
    return AggregateFunctionPtr(
            create_function_single_value<ReaderFunctionData, ReaderFunctionFirstData, is_nullable,
                                         is_copy>(name, argument_types, parameters));
}

template <bool is_nullable, bool is_copy>
AggregateFunctionPtr create_aggregate_function_first_non_null_value(const std::string& name,
                                                                    const DataTypes& argument_types,
                                                                    const Array& parameters,
                                                                    bool result_is_nullable) {
    return AggregateFunctionPtr(
            create_function_single_value<ReaderFunctionData, ReaderFunctionFirstNonNullData,
                                         is_nullable, is_copy>(name, argument_types, parameters));
}

template <bool is_nullable, bool is_copy>
AggregateFunctionPtr create_aggregate_function_last(const std::string& name,
                                                    const DataTypes& argument_types,
                                                    const Array& parameters,
                                                    bool result_is_nullable) {
    return AggregateFunctionPtr(
            create_function_single_value<ReaderFunctionData, ReaderFunctionLastData, is_nullable,
                                         is_copy>(name, argument_types, parameters));
}

template <bool is_nullable, bool is_copy>
AggregateFunctionPtr create_aggregate_function_last_non_null_value(const std::string& name,
                                                                   const DataTypes& argument_types,
                                                                   const Array& parameters,
                                                                   bool result_is_nullable) {
    return AggregateFunctionPtr(
            create_function_single_value<ReaderFunctionData, ReaderFunctionLastNonNullData,
                                         is_nullable, is_copy>(name, argument_types, parameters));
}

} // namespace doris::vectorized
