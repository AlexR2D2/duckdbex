#include "value_to_term.h"
#include "duckdb.hpp"
#include "term.h"
#include <iostream>

bool nif::value_to_term(ErlNifEnv* env, const duckdb::Value& value, ERL_NIF_TERM& sink) {
  // <dbg>
  // std::cout << "value_to_term: value_type: " << value.type().ToString() << std::endl;
  // std::cout << "value_to_term: value enum code: " << unsigned(static_cast<std::underlying_type<duckdb::LogicalTypeId>::type>(value.type().id())) << std::endl;
  // </dbg>

  auto type = value.type();

  if (value.IsNull()) {
    sink = make_atom(env, "nil");
    return true;
  }

  switch(type.id()) {
    case duckdb::LogicalTypeId::BIGINT: {
        int64_t bigint = duckdb::BigIntValue::Get(value);
        sink = enif_make_int64(env, bigint);
        return true;
      }
    case duckdb::LogicalTypeId::UBIGINT: {
        uint64_t ubigint = duckdb::UBigIntValue::Get(value);
        sink = enif_make_uint64(env, ubigint);
        return true;
      }
    case duckdb::LogicalTypeId::BOOLEAN: {
        bool boolean = duckdb::BooleanValue::Get(value);
        sink = enif_make_atom(env, boolean ? "true" : "false");
        return true;
      }
    case duckdb::LogicalTypeId::BLOB: {
        auto blob = duckdb::StringValue::Get(value);
        sink = make_binary_term(env, blob);
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

        // Handle special floating-point cases
        if (std::isinf(a_double)) {
            if (a_double > 0) {
                sink = make_atom(env, "infinity"); // Positive infinity
            } else {
                sink = make_atom(env, "-infinity"); // Negative infinity
            }
        } else if (std::isnan(a_double)) {
            sink = make_atom(env, "nan"); // Handle NaN
        } else {
            sink = enif_make_double(env, a_double); // Regular double handling
        }
        return true;
      }
    case duckdb::LogicalTypeId::DECIMAL: {
        uint8_t width = duckdb::DecimalType::GetWidth(type);
        uint8_t scale = duckdb::DecimalType::GetScale(type);

        auto internal_type = type.InternalType();

        if (internal_type == duckdb::PhysicalType::INT16) {
          sink = enif_make_tuple3(env,
            enif_make_int(env, duckdb::SmallIntValue::Get(value)),
            enif_make_uint(env, width),
            enif_make_uint(env, scale));
          return true;
        } else if (internal_type == duckdb::PhysicalType::INT32) {
          sink = enif_make_tuple3(env,
            enif_make_int(env, duckdb::IntegerValue::Get(value)),
            enif_make_uint(env, width),
            enif_make_uint(env, scale));
          return true;
        } else if (internal_type == duckdb::PhysicalType::INT64) {
          sink = enif_make_tuple3(env,
            enif_make_int(env, duckdb::BigIntValue::Get(value)),
            enif_make_uint(env, width),
            enif_make_uint(env, scale));
          return true;
        } else {
          D_ASSERT(internal_type == duckdb::PhysicalType::INT128);
          duckdb::hugeint_t hugeint = duckdb::HugeIntValue::Get(value);
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
        duckdb::hugeint_t hugeint = duckdb::HugeIntValue::Get(value);
        sink = enif_make_tuple2(env,
              enif_make_int64(env, hugeint.upper),
              enif_make_uint64(env, hugeint.lower)
            );
        return true;
      }
    case duckdb::LogicalTypeId::UHUGEINT: {
        duckdb::uhugeint_t uhugeint = duckdb::UhugeIntValue::Get(value);
        sink = enif_make_tuple2(env,
              enif_make_uint64(env, uhugeint.upper),
              enif_make_uint64(env, uhugeint.lower)
            );
        return true;
      }
    case duckdb::LogicalTypeId::INTEGER: {
        int32_t integer = duckdb::IntegerValue::Get(value);
        sink = enif_make_int(env, integer);
        return true;
      }
    case duckdb::LogicalTypeId::UINTEGER: {
        uint32_t uinteger = duckdb::UIntegerValue::Get(value);
        sink = enif_make_uint(env, uinteger);
        return true;
      }

    case duckdb::LogicalTypeId::FLOAT: {
        auto a_float = value.GetValueUnsafe<float>();
        sink = enif_make_double(env, a_float);
        return true;
      }
    case duckdb::LogicalTypeId::SMALLINT: {
        int16_t a_int16 = duckdb::SmallIntValue::Get(value);
        sink = enif_make_int(env, a_int16);
        return true;
      }
    case duckdb::LogicalTypeId::USMALLINT: {
        uint16_t an_uint16 = duckdb::USmallIntValue::Get(value);
        sink = enif_make_uint(env, an_uint16);
        return true;
      }
    case duckdb::LogicalTypeId::TIME: {
        duckdb::dtime_t time = duckdb::TimeValue::Get(value);
        int32_t time_units[4];
        duckdb::Time::Convert(time, time_units[0], time_units[1], time_units[2], time_units[3]);
        sink = enif_make_tuple4(env,
          enif_make_int(env, time_units[0]),
          enif_make_int(env, time_units[1]),
          enif_make_int(env, time_units[2]),
          enif_make_int(env, time_units[3]));
        return true;
      }
    case duckdb::LogicalTypeId::TIME_TZ: {
        duckdb::dtime_tz_t time = value.GetValue<duckdb::dtime_tz_t>();

        int32_t time_units[4];
        duckdb::Time::Convert(time.time(), time_units[0], time_units[1], time_units[2], time_units[3]);

        int32_t offset_hour = int32_t(time.offset() / duckdb::Interval::SECS_PER_HOUR);
        int32_t offset_min = std::abs(int32_t((time.offset() % duckdb::Interval::SECS_PER_HOUR) / duckdb::Interval::SECS_PER_MINUTE));

        sink = enif_make_tuple5(env,
          enif_make_int(env, time_units[0]),
          enif_make_int(env, time_units[1]),
          enif_make_int(env, time_units[2]),
          enif_make_int(env, time_units[3]),
          enif_make_tuple2(env,
            enif_make_int(env, offset_hour),
            enif_make_int(env, offset_min)
          )
        );
        return true;
      }
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ: {
        duckdb::timestamp_t timestamp = duckdb::TimestampValue::Get(value);

        duckdb::date_t out_date = duckdb::Timestamp::GetDate(timestamp);
        duckdb::dtime_t out_time = duckdb::Timestamp::GetTime(timestamp);

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
    case duckdb::LogicalTypeId::TIMESTAMP_NS: {
        duckdb::timestamp_t timestamp_ns_really = duckdb::TimestampValue::Get(value);

        duckdb::timestamp_ns_t timestamp_ns;
        timestamp_ns.value = timestamp_ns_really.value;

        duckdb::date_t out_date;
        duckdb::dtime_t out_time;
        int32_t out_nanos = 0;

        duckdb::Timestamp::Convert(timestamp_ns, out_date, out_time, out_nanos);

        int32_t out_year, out_month, out_day;
        duckdb::Date::Convert(out_date, out_year, out_month, out_day);

        int32_t out_hour, out_min, out_sec, out_micros;
        duckdb::Time::Convert(out_time, out_hour, out_min, out_sec, out_micros);

        int32_t nanoseconds = out_micros * duckdb::Interval::NANOS_PER_MICRO + out_nanos;

        sink = enif_make_tuple2(env,
          enif_make_tuple3(env,
            enif_make_int(env, out_year),
            enif_make_int(env, out_month),
            enif_make_int(env, out_day)),
          enif_make_tuple4(env,
            enif_make_int(env, out_hour),
            enif_make_int(env, out_min),
            enif_make_int(env, out_sec),
            enif_make_int(env, nanoseconds))
        );

        return true;
      }
    case duckdb::LogicalTypeId::TIMESTAMP_MS: {
        auto date_value = value.DefaultCastAs(duckdb::LogicalType::DATE);
        auto time_value = value.DefaultCastAs(duckdb::LogicalType::TIME);

        duckdb::date_t out_date = duckdb::DateValue::Get(date_value);
        duckdb::dtime_t out_time = duckdb::TimeValue::Get(time_value);

        int32_t out_year, out_month, out_day;
        duckdb::Date::Convert(out_date, out_year, out_month, out_day);

        int32_t out_hour, out_min, out_sec, out_micros;
        duckdb::Time::Convert(out_time, out_hour, out_min, out_sec, out_micros);

        int32_t out_msec = out_micros / duckdb::Interval::MICROS_PER_MSEC;

        sink = enif_make_tuple2(env,
          enif_make_tuple3(env,
            enif_make_int(env, out_year),
            enif_make_int(env, out_month),
            enif_make_int(env, out_day)),
          enif_make_tuple4(env,
            enif_make_int(env, out_hour),
            enif_make_int(env, out_min),
            enif_make_int(env, out_sec),
            enif_make_int(env, out_msec))
        );

        return true;
      }
    case duckdb::LogicalTypeId::TIMESTAMP_SEC: {
        auto date_value = value.DefaultCastAs(duckdb::LogicalType::DATE);
        auto time_value = value.DefaultCastAs(duckdb::LogicalType::TIME);

        duckdb::date_t out_date = duckdb::DateValue::Get(date_value);
        duckdb::dtime_t out_time = duckdb::TimeValue::Get(time_value);

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
    case duckdb::LogicalTypeId::INTERVAL: {
        duckdb::interval_t interval = duckdb::IntervalValue::Get(value);

        int64_t months, days, micros;
        interval.Normalize(months, days, micros);

        sink = enif_make_tuple3(env,
              enif_make_int(env, months),
              enif_make_int(env, days),
              enif_make_int64(env, micros)
            );

        return true;
      }
    case duckdb::LogicalTypeId::TINYINT: {
        int8_t tinyint = duckdb::TinyIntValue::Get(value);
        sink = enif_make_int(env, tinyint);
        return true;
      }
    case duckdb::LogicalTypeId::UTINYINT: {
        uint8_t utinyint = duckdb::UTinyIntValue::Get(value);
        sink = enif_make_uint(env, utinyint);
        return true;
      }
    case duckdb::LogicalTypeId::UUID: {
        duckdb::hugeint_t hugeint = duckdb::HugeIntValue::Get(value);

        char buff[duckdb::UUID::STRING_SIZE];
        duckdb::UUID::ToString(hugeint, buff);

        sink = make_binary_term(env, buff, duckdb::UUID::STRING_SIZE);

        return true;
      }
    case duckdb::LogicalTypeId::CHAR:
    case duckdb::LogicalTypeId::VARCHAR: {
        auto varchar = value.ToString();
        sink = make_binary_term(env, varchar.c_str(), varchar.size());
        return true;
      }
    case duckdb::LogicalTypeId::ENUM: {
        std::string enum_value = duckdb::EnumType::GetValue(value);
        sink = make_binary_term(env, enum_value);
        return true;
      }
    case duckdb::LogicalTypeId::LIST: {
        auto& values = duckdb::ListValue::GetChildren(value);

        if (!values.size()) {
          sink = enif_make_list(env, 0);
          return true;
        }

        std::vector<ERL_NIF_TERM> terms(values.size());

        for (size_t i = 0; i < values.size(); i++) {
          ERL_NIF_TERM val_term;
          if (!value_to_term(env, values[i], val_term))
            return false;
          terms[i] = val_term;
        }

        sink = enif_make_list_from_array(env, &terms[0], terms.size());
        return true;
      }
    case duckdb::LogicalTypeId::ARRAY: {
        auto& values = duckdb::ArrayValue::GetChildren(value);

        if (!values.size()) {
          sink = enif_make_list(env, 0);
          return true;
        }

        std::vector<ERL_NIF_TERM> terms(values.size());

        for (size_t i = 0; i < values.size(); i++) {
          ERL_NIF_TERM val_term;
          if (!value_to_term(env, values[i], val_term))
            return false;
          terms[i] = val_term;
        }

        sink = enif_make_list_from_array(env, &terms[0], terms.size());
        return true;
      }
    case duckdb::LogicalTypeId::MAP: {
        std::vector<duckdb::Value> pairs = duckdb::MapValue::GetChildren(value);

        if (!pairs.size()) {
          sink = enif_make_list(env, 0);
          return true;
        }

        std::vector<ERL_NIF_TERM> tuples(pairs.size());

        for (size_t i = 0; i < pairs.size(); i++) {
          auto &pair = duckdb::StructValue::GetChildren(pairs[i]);

          ERL_NIF_TERM key_term, val_term;
          if (!value_to_term(env, pair[0], key_term) || !value_to_term(env, pair[1], val_term))
            return false;

          tuples[i] = enif_make_tuple2(env, key_term, val_term);
        }

        sink = enif_make_list_from_array(env, &tuples[0], tuples.size());

        return true;
      }
    case duckdb::LogicalTypeId::STRUCT: {
        auto &names = duckdb::StructType::GetChildTypes(value.type());

        if (!names.size()) {
          sink = enif_make_new_map(env);
          return true;
        }

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

        return enif_make_map_from_arrays(env, &keys_array[0], &values_array[0], names.size(), &sink);
      }
    case duckdb::LogicalTypeId::UNION: {
      std::vector<ERL_NIF_TERM> keys_array(1);
      std::vector<ERL_NIF_TERM> values_array(1);

      auto member_name = duckdb::UnionType::GetMemberName(value.type(), duckdb::UnionValue::GetTag(value));
      auto& member_value = duckdb::UnionValue::GetValue(value);

      ERL_NIF_TERM union_tag_name_term = nif::make_binary_term(env, member_name);

      ERL_NIF_TERM union_value_term;
      if (!value_to_term(env, member_value, union_value_term))
        return false;

      sink = enif_make_tuple2(env, union_tag_name_term, union_value_term);

      return true;
    }
    default:
      return false;
  };

  return false;
}
