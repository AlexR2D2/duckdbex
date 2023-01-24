#include "value_to_term.h"
#include "duckdb.hpp"
#include "term.h"
#include <iostream>

bool nif::value_to_term(ErlNifEnv* env, const duckdb::Value& value, ERL_NIF_TERM& sink) {
  auto type = value.type();

  if (value.IsNull()) {
    sink = make_atom(env, "nil");
    return true;
  }

  switch(type.id()) {
    case duckdb::LogicalTypeId::BIGINT: {
        auto bigint = value.GetValueUnsafe<int64_t>();
        sink = enif_make_int64(env, bigint);
        return true;
      }
    case duckdb::LogicalTypeId::BOOLEAN: {
        auto boolean = value.GetValueUnsafe<bool>();
        sink = enif_make_atom(env, boolean ? "true" : "false");
        return true;
      }
    case duckdb::LogicalTypeId::BLOB: {
        auto blob = value.GetValueUnsafe<duckdb::string_t>();
        sink = make_binary_term(env, blob.GetDataUnsafe(), blob.GetSize());
        return true;
      }
    case duckdb::LogicalTypeId::DATE: {
        auto date = value.GetValueUnsafe<duckdb::date_t>();
        int32_t out_year, out_month, out_day;
        duckdb::Date::Convert(date, out_year, out_month, out_day);
        sink = enif_make_tuple3(env,
          enif_make_int(env, out_year),
          enif_make_int(env, out_month),
          enif_make_int(env, out_day));
        return true;
      }
    case duckdb::LogicalTypeId::DOUBLE: {
        auto a_double = value.GetValueUnsafe<double>();
        sink = enif_make_double(env, a_double);
        return true;
      }
    case duckdb::LogicalTypeId::DECIMAL: {
        uint8_t width = duckdb::DecimalType::GetWidth(type);
        uint8_t scale = duckdb::DecimalType::GetScale(type);

        auto internal_type = type.InternalType();

        if (internal_type == duckdb::PhysicalType::INT16) {
          sink = enif_make_tuple3(env,
            enif_make_int(env, value.GetValueUnsafe<int16_t>()),
            enif_make_uint(env, width),
            enif_make_uint(env, scale));
          return true;
        } else if (internal_type == duckdb::PhysicalType::INT32) {
          sink = enif_make_tuple3(env,
            enif_make_int(env, value.GetValueUnsafe<int32_t>()),
            enif_make_uint(env, width),
            enif_make_uint(env, scale));
          return true;
        } else if (internal_type == duckdb::PhysicalType::INT64) {
          sink = enif_make_tuple3(env,
            enif_make_int(env, value.GetValueUnsafe<int64_t>()),
            enif_make_uint(env, width),
            enif_make_uint(env, scale));
          return true;
        } else {
          D_ASSERT(internal_type == duckdb::PhysicalType::INT128);
          duckdb::hugeint_t hugeint = value.GetValueUnsafe<duckdb::hugeint_t>();
          sink = enif_make_tuple3(env,
            enif_make_tuple2(env,
              enif_make_uint64(env, hugeint.lower),
              enif_make_int64(env, hugeint.upper)
            ),
            enif_make_uint(env, width),
            enif_make_uint(env, scale)
          );
          return true;
        }

        return false;
      }
    case duckdb::LogicalTypeId::HUGEINT: {
        duckdb::hugeint_t hugeint = value.GetValueUnsafe<duckdb::hugeint_t>();
        sink = enif_make_tuple2(env,
              enif_make_int64(env, hugeint.upper),
              enif_make_uint64(env, hugeint.lower)
            );
        return true;
      }
    case duckdb::LogicalTypeId::INTEGER: {
        auto integer = value.GetValueUnsafe<int32_t>();
        sink = enif_make_int(env, integer);
        return true;
      }
    case duckdb::LogicalTypeId::INTERVAL: {
        auto interval = value.GetValueUnsafe<duckdb::interval_t>();
        sink = enif_make_uint64(env, duckdb::Interval::GetMicro(interval));
        return true;
      }
    case duckdb::LogicalTypeId::FLOAT: {
        auto a_float = value.GetValueUnsafe<float>();
        sink = enif_make_double(env, a_float);
        return true;
      }
    case duckdb::LogicalTypeId::SMALLINT: {
        auto a_int16 = value.GetValueUnsafe<int16_t>();
        sink = enif_make_int(env, a_int16);
        return true;
      }
    case duckdb::LogicalTypeId::TIME: {
        auto time = value.GetValueUnsafe<duckdb::dtime_t>();
        int32_t out_hour, out_min, out_sec, out_micros;
        duckdb::Time::Convert(time, out_hour, out_min, out_sec, out_micros);
        sink = enif_make_tuple4(env,
          enif_make_int(env, out_hour),
          enif_make_int(env, out_min),
          enif_make_int(env, out_sec),
          enif_make_int(env, out_micros));
        return true;
      }
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ: {
        auto timestamp = value.GetValueUnsafe<duckdb::timestamp_t>();

        duckdb::date_t out_date;
        duckdb::dtime_t out_time;
        duckdb::Timestamp::Convert(timestamp, out_date, out_time);

        int32_t out_year, out_month, out_day;
        duckdb::Date::Convert(out_date, out_year, out_month, out_day);

        int32_t out_hour, out_min, out_sec, out_micros;
        duckdb::Time::Convert(out_time, out_hour, out_min, out_sec, out_micros);

        sink = enif_make_tuple2(env,
          enif_make_tuple3(env,
            enif_make_int(env, out_year),
            enif_make_int(env, out_month),
            enif_make_int(env, out_day)),
          enif_make_tuple4(env,
            enif_make_int(env, out_hour),
            enif_make_int(env, out_min),
            enif_make_int(env, out_sec),
            enif_make_int(env, out_micros))
        );

        return true;
      }
    case duckdb::LogicalTypeId::TINYINT: {
        auto tinyint = value.GetValueUnsafe<int8_t>();
        sink = enif_make_int(env, tinyint);
        return true;
      }
    case duckdb::LogicalTypeId::UBIGINT: {
        auto ubigint = value.GetValueUnsafe<uint64_t>();
        sink = enif_make_uint64(env, ubigint);
        return true;
      }
    case duckdb::LogicalTypeId::UINTEGER: {
        auto uinteger = value.GetValueUnsafe<uint32_t>();
        sink = enif_make_uint(env, uinteger);
        return true;
      }
    case duckdb::LogicalTypeId::USMALLINT: {
        auto an_uint16 = value.GetValueUnsafe<uint16_t>();
        sink = enif_make_uint(env, an_uint16);
        return true;
      }
    case duckdb::LogicalTypeId::UTINYINT: {
        auto utinyint = value.GetValueUnsafe<uint8_t>();
        sink = enif_make_uint(env, utinyint);
        return true;
      }
    case duckdb::LogicalTypeId::UUID: {
        auto uuid_as_hugeint_t = value.GetValueUnsafe<duckdb::hugeint_t>();
        auto uuid =  duckdb::UUID::ToString(uuid_as_hugeint_t);
        sink = make_binary_term(env, uuid.c_str(), uuid.size());
        return true;
      }
    case duckdb::LogicalTypeId::VARCHAR: {
        auto varchar = value.ToString();
        sink = make_binary_term(env, varchar.c_str(), varchar.size());
        return true;
      }
    case duckdb::LogicalTypeId::ENUM: {
        auto varchar = value.ToString();
        sink = make_binary_term(env, varchar.c_str(), varchar.size());
        return true;
      }
    case duckdb::LogicalTypeId::LIST: {
        auto& list = duckdb::ListValue::GetChildren(value);
        std::vector<ERL_NIF_TERM> term_array(list.size());

        for (size_t i = 0; i < list.size(); i++) {
          ERL_NIF_TERM val_term;
          if (!value_to_term(env, list[i], val_term))
            return false;
          term_array[i] = val_term;
        }

        if (term_array.size() > 0)
          sink = enif_make_list_from_array(env, &term_array[0], term_array.size());
        else
          sink = enif_make_list(env, 0);

        return true;
      }
    case duckdb::LogicalTypeId::MAP: {
        duckdb::Vector the_map(value);

        auto keys = duckdb::MapVector::GetKeys(the_map);
        auto values = duckdb::MapVector::GetValues(the_map);

        duckdb::idx_t size = duckdb::ListVector::GetListSize(the_map);

        std::vector<ERL_NIF_TERM> keys_terms(size);
        std::vector<ERL_NIF_TERM> values_terms(size);

        for (size_t i = 0; i < size; i++) {
          ERL_NIF_TERM key_term, val_term;
          if (!value_to_term(env, keys.GetValue(i), key_term)
           || !value_to_term(env, values.GetValue(i), val_term))
            return false;

          keys_terms[i] = key_term;
          values_terms[i] = val_term;
        }

        if (size)
          return enif_make_map_from_arrays(env, &keys_terms[0], &values_terms[0], size, &sink);
        else {
          sink = enif_make_new_map(env);
          return true;
        }
      }
    case duckdb::LogicalTypeId::STRUCT: {
        auto &names = duckdb::StructType::GetChildTypes(value.type());
        auto &values = duckdb::StructValue::GetChildren(value);

        std::vector<ERL_NIF_TERM> keys_array(names.size());
        std::vector<ERL_NIF_TERM> values_array(names.size());

        for (size_t i = 0; i < names.size(); i++) {
          ERL_NIF_TERM val_term;
          if (!value_to_term(env, values[i], val_term))
            return false;
          keys_array[i] = nif::make_binary_term(env, names[i].first);
          values_array[i] = val_term;
        }

        if (names.size())
          return enif_make_map_from_arrays(env, &keys_array[0], &values_array[0], names.size(), &sink);
        else {
          sink = enif_make_new_map(env);
          return true;
        }
      }
    case duckdb::LogicalTypeId::UNION: {
      std::vector<ERL_NIF_TERM> keys_array(1);
      std::vector<ERL_NIF_TERM> values_array(1);

      auto& member_value = duckdb::UnionValue::GetValue(value);
      auto member_name = duckdb::UnionType::GetMemberName(value.type(), duckdb::UnionValue::GetTag(value));

      ERL_NIF_TERM val_term;
      if (!value_to_term(env, member_value, val_term))
        return false;

      keys_array[0] = nif::make_binary_term(env, member_name);
      values_array[0] = val_term;

      return enif_make_map_from_arrays(env, &keys_array[0], &values_array[0], 1, &sink);
    }
    default:
      return false;
  };

  return false;
}
