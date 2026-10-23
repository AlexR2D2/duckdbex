#include "value_to_term.h"
#include "duckdb.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "term.h"
#include <cmath>

ERL_NIF_TERM nif::logical_type_to_term(ErlNifEnv* env, const duckdb::LogicalType& type) {
  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
      return make_atom(env, "boolean");
    case duckdb::LogicalTypeId::CHAR:
    case duckdb::LogicalTypeId::VARCHAR:
    case duckdb::LogicalTypeId::ENUM:
      return make_atom(env, "string");
    case duckdb::LogicalTypeId::TINYINT:
      return make_atom(env, "tinyint");
    case duckdb::LogicalTypeId::UTINYINT:
      return make_atom(env, "utinyint");
    case duckdb::LogicalTypeId::SMALLINT:
      return make_atom(env, "smallint");
    case duckdb::LogicalTypeId::USMALLINT:
      return make_atom(env, "usmallint");
    case duckdb::LogicalTypeId::INTEGER:
      return make_atom(env, "integer");
    case duckdb::LogicalTypeId::UINTEGER:
      return make_atom(env, "uinteger");
    case duckdb::LogicalTypeId::BIGINT:
      return make_atom(env, "bigint");
    case duckdb::LogicalTypeId::UBIGINT:
      return make_atom(env, "ubigint");
    case duckdb::LogicalTypeId::HUGEINT:
      return make_atom(env, "hugeint");
    case duckdb::LogicalTypeId::UHUGEINT:
      return make_atom(env, "uhugeint");
    case duckdb::LogicalTypeId::FLOAT:
      return make_atom(env, "float");
    case duckdb::LogicalTypeId::DOUBLE:
      return make_atom(env, "double");
    case duckdb::LogicalTypeId::DECIMAL:
      return make_atom(env, "decimal");
    case duckdb::LogicalTypeId::DATE:
      return make_atom(env, "date");
    case duckdb::LogicalTypeId::TIME:
      return make_atom(env, "time");
    case duckdb::LogicalTypeId::TIME_TZ:
      return make_atom(env, "time_tz");
    case duckdb::LogicalTypeId::TIMESTAMP:
      return make_atom(env, "timestamp");
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      return make_atom(env, "timestamp_tz");
    case duckdb::LogicalTypeId::TIMESTAMP_TZ_NS:
      return make_atom(env, "timestamp_tz_ns");
    case duckdb::LogicalTypeId::TIMESTAMP_NS:
      return make_atom(env, "timestamp_ns");
    case duckdb::LogicalTypeId::TIMESTAMP_MS:
      return make_atom(env, "timestamp_ms");
    case duckdb::LogicalTypeId::TIMESTAMP_SEC:
      return make_atom(env, "timestamp_sec");
    case duckdb::LogicalTypeId::INTERVAL:
      return make_atom(env, "interval");
    case duckdb::LogicalTypeId::BLOB:
      return make_atom(env, "blob");
    case duckdb::LogicalTypeId::UUID:
      return make_atom(env, "uuid");
    case duckdb::LogicalTypeId::LIST:
      return make_atom(env, "list");
    case duckdb::LogicalTypeId::ARRAY:
      return make_atom(env, "array");
    case duckdb::LogicalTypeId::MAP:
      return make_atom(env, "map");
    case duckdb::LogicalTypeId::STRUCT:
      return make_atom(env, "struct");
    case duckdb::LogicalTypeId::TUPLE:
      return make_atom(env, "tuple");
    case duckdb::LogicalTypeId::UNION:
      return make_atom(env, "union");
    case duckdb::LogicalTypeId::VARIANT:
      return make_atom(env, "variant");
    case duckdb::LogicalTypeId::GEOMETRY:
      return make_atom(env, "geometry");
    case duckdb::LogicalTypeId::ANY:
      return make_atom(env, "any");
    default:
      return make_atom(env, "unknown");
  }
}

static bool children_to_term(ErlNifEnv* env, const std::vector<duckdb::Value>& values, ERL_NIF_TERM& sink, bool raw) {
  if (values.empty()) {
    sink = enif_make_list(env, 0);
    return true;
  }

  std::vector<ERL_NIF_TERM> terms(values.size());

  for (size_t i = 0; i < values.size(); i++) {
    if (!nif::value_to_term(env, values[i], terms[i], raw))
      return false;
  }

  sink = enif_make_list_from_array(env, &terms[0], terms.size());
  return true;
}


template <class T>
static bool
infinite_value_to_term(ErlNifEnv* env, T value, ERL_NIF_TERM& sink) {
  if (value.IsFinite())
    return false;

  sink = value == T::infinity() ? nif::make_atom(env, "infinity") : nif::make_atom(env, "-infinity");
  return true;
}

static ERL_NIF_TERM
make_date_term(ErlNifEnv* env, int32_t year, int32_t month, int32_t day) {
  return nif::make_struct(env, "Elixir.Date", {
    {"calendar", nif::make_atom(env, "Elixir.Calendar.ISO")},
    {"year", enif_make_int(env, year)},
    {"month", enif_make_int(env, month)},
    {"day", enif_make_int(env, day)},
  });
}

static ERL_NIF_TERM
make_time_term(ErlNifEnv* env, int32_t hour, int32_t minute, int32_t second, int32_t micros) {
  return nif::make_struct(env, "Elixir.Time", {
    {"calendar", nif::make_atom(env, "Elixir.Calendar.ISO")},
    {"hour", enif_make_int(env, hour)},
    {"minute", enif_make_int(env, minute)},
    {"second", enif_make_int(env, second)},
    {"microsecond", enif_make_tuple2(env, enif_make_int(env, micros), enif_make_int(env, 6))},
  });
}

static ERL_NIF_TERM
make_naive_datetime_term(ErlNifEnv* env, int32_t year, int32_t month, int32_t day,
                         int32_t hour, int32_t minute, int32_t second, int32_t micros) {
  return nif::make_struct(env, "Elixir.NaiveDateTime", {
    {"calendar", nif::make_atom(env, "Elixir.Calendar.ISO")},
    {"year", enif_make_int(env, year)},
    {"month", enif_make_int(env, month)},
    {"day", enif_make_int(env, day)},
    {"hour", enif_make_int(env, hour)},
    {"minute", enif_make_int(env, minute)},
    {"second", enif_make_int(env, second)},
    {"microsecond", enif_make_tuple2(env, enif_make_int(env, micros), enif_make_int(env, 6))},
  });
}

static ERL_NIF_TERM
make_datetime_term(ErlNifEnv* env, int32_t year, int32_t month, int32_t day,
                   int32_t hour, int32_t minute, int32_t second, int32_t micros) {
  return nif::make_struct(env, "Elixir.DateTime", {
    {"calendar", nif::make_atom(env, "Elixir.Calendar.ISO")},
    {"year", enif_make_int(env, year)},
    {"month", enif_make_int(env, month)},
    {"day", enif_make_int(env, day)},
    {"hour", enif_make_int(env, hour)},
    {"minute", enif_make_int(env, minute)},
    {"second", enif_make_int(env, second)},
    {"microsecond", enif_make_tuple2(env, enif_make_int(env, micros), enif_make_int(env, 6))},
    {"time_zone", nif::make_binary_term(env, "Etc/UTC")},
    {"zone_abbr", nif::make_binary_term(env, "UTC")},
    {"utc_offset", enif_make_int(env, 0)},
    {"std_offset", enif_make_int(env, 0)},
  });
}

static ERL_NIF_TERM
make_hugeint_term(ErlNifEnv* env, int64_t upper, uint64_t lower) {
  return nif::make_struct(env, "Elixir.Duckdbex.Hugeint", {
    {"upper", enif_make_int64(env, upper)},
    {"lower", enif_make_uint64(env, lower)},
  });
}

bool nif::value_to_term(ErlNifEnv* env, const duckdb::Value& value, ERL_NIF_TERM& sink) {
  return nif::value_to_term(env, value, sink, false);
}

bool nif::value_to_term(ErlNifEnv* env, const duckdb::Value& value, ERL_NIF_TERM& sink, bool raw) {
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
    case duckdb::LogicalTypeId::GEOMETRY: {
        auto& geometry = duckdb::StringValue::Get(value);
        sink = make_binary_term(env, geometry.data(), geometry.size());
        return true;
      }
    case duckdb::LogicalTypeId::DATE: {
        auto date = value.GetValueUnsafe<duckdb::date_t>();
        if (infinite_value_to_term(env, date, sink))
          return true;

        if (raw) {
          sink = enif_make_int(env, date.days);
          return true;
        }

        int32_t out_year, out_month, out_day;
        duckdb::Date::Convert(date, out_year, out_month, out_day);
        sink = make_date_term(env, out_year, out_month, out_day);
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

        ERL_NIF_TERM unscaled;

        if (internal_type == duckdb::PhysicalType::INT16) {
          unscaled = enif_make_int(env, duckdb::SmallIntValue::Get(value));
        } else if (internal_type == duckdb::PhysicalType::INT32) {
          unscaled = enif_make_int(env, duckdb::IntegerValue::Get(value));
        } else if (internal_type == duckdb::PhysicalType::INT64) {
          unscaled = enif_make_int64(env, duckdb::BigIntValue::Get(value));
        } else {
          D_ASSERT(internal_type == duckdb::PhysicalType::INT128);
          duckdb::hugeint_t hugeint = duckdb::HugeIntValue::Get(value);
          unscaled = make_hugeint_term(env, hugeint.upper, hugeint.lower);
        }

        sink = nif::make_struct(env, "Elixir.Duckdbex.Decimal", {
          {"unscaled", unscaled},
          {"width", enif_make_uint(env, width)},
          {"scale", enif_make_uint(env, scale)},
        });

        return true;
      }
    case duckdb::LogicalTypeId::HUGEINT: {
        duckdb::hugeint_t hugeint = duckdb::HugeIntValue::Get(value);
        sink = make_hugeint_term(env, hugeint.upper, hugeint.lower);
        return true;
      }
    case duckdb::LogicalTypeId::UHUGEINT: {
        duckdb::uhugeint_t uhugeint = duckdb::UhugeIntValue::Get(value);
        sink = nif::make_struct(env, "Elixir.Duckdbex.Hugeint", {
          {"upper", enif_make_uint64(env, uhugeint.upper)},
          {"lower", enif_make_uint64(env, uhugeint.lower)},
        });
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

        if (std::isinf(a_float)) {
            if (a_float > 0) {
                sink = make_atom(env, "infinity");
            } else {
                sink = make_atom(env, "-infinity");
            }
        } else if (std::isnan(a_float)) {
            sink = make_atom(env, "nan");
        } else {
            sink = enif_make_double(env, a_float);
        }
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

        if (raw) {
          sink = enif_make_int64(env, time.value);
          return true;
        }

        int32_t time_units[4];
        duckdb::Time::Convert(time, time_units[0], time_units[1], time_units[2], time_units[3]);
        sink = make_time_term(env, time_units[0], time_units[1], time_units[2], time_units[3]);
        return true;
      }
    case duckdb::LogicalTypeId::TIME_TZ: {
        duckdb::dtime_tz_t time = value.GetValue<duckdb::dtime_tz_t>();

        if (raw) {
          sink = enif_make_tuple2(env, enif_make_int64(env, time.time().value), enif_make_int(env, int32_t(time.offset())));
          return true;
        }

        int32_t time_units[4];
        duckdb::Time::Convert(time.time(), time_units[0], time_units[1], time_units[2], time_units[3]);

        sink = nif::make_struct(env, "Elixir.Duckdbex.TimeTz", {
          {"time", make_time_term(env, time_units[0], time_units[1], time_units[2], time_units[3])},
          {"offset", enif_make_int(env, int32_t(time.offset()))},
        });
        return true;
      }
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ: {
        duckdb::timestamp_t timestamp = duckdb::TimestampValue::Get(value);
        if (infinite_value_to_term(env, timestamp, sink))
          return true;

        if (raw) {
          sink = enif_make_int64(env, timestamp.value);
          return true;
        }

        duckdb::date_t out_date = duckdb::Timestamp::GetDate(timestamp);
        duckdb::dtime_t out_time = duckdb::Timestamp::GetTime(timestamp);

        int32_t out_year, out_month, out_day;
        duckdb::Date::Convert(out_date, out_year, out_month, out_day);

        int32_t out_hour, out_min, out_sec, out_micros;
        duckdb::Time::Convert(out_time, out_hour, out_min, out_sec, out_micros);

        if (type.id() == duckdb::LogicalTypeId::TIMESTAMP_TZ)
          sink = make_datetime_term(env, out_year, out_month, out_day, out_hour, out_min, out_sec, out_micros);
        else
          sink = make_naive_datetime_term(env, out_year, out_month, out_day, out_hour, out_min, out_sec, out_micros);

        return true;
      }
    case duckdb::LogicalTypeId::TIMESTAMP_NS: {
        duckdb::timestamp_t timestamp_ns_really = duckdb::TimestampValue::Get(value);

        duckdb::timestamp_ns_t timestamp_ns;
        timestamp_ns.value = timestamp_ns_really.value;
        if (infinite_value_to_term(env, timestamp_ns, sink))
          return true;

        if (raw) {
          sink = enif_make_int64(env, timestamp_ns.value);
          return true;
        }

        duckdb::date_t out_date;
        duckdb::dtime_t out_time;
        int32_t out_nanos = 0;

        duckdb::Timestamp::Convert(timestamp_ns, out_date, out_time, out_nanos);

        int32_t out_year, out_month, out_day;
        duckdb::Date::Convert(out_date, out_year, out_month, out_day);

        int32_t out_hour, out_min, out_sec, out_micros;
        duckdb::Time::Convert(out_time, out_hour, out_min, out_sec, out_micros);

        sink = nif::make_struct(env, "Elixir.Duckdbex.TimestampNs", {
          {"datetime", make_naive_datetime_term(env, out_year, out_month, out_day, out_hour, out_min, out_sec, out_micros)},
          {"nanosecond", enif_make_int(env, out_nanos)},
        });

        return true;
      }
    case duckdb::LogicalTypeId::TIMESTAMP_TZ_NS: {
        duckdb::timestamp_ns_t timestamp_ns(duckdb::TimestampTZNSValue::Get(value));
        if (infinite_value_to_term(env, timestamp_ns, sink))
          return true;

        if (raw) {
          sink = enif_make_int64(env, timestamp_ns.value);
          return true;
        }

        duckdb::date_t out_date;
        duckdb::dtime_t out_time;
        int32_t out_nanos = 0;

        duckdb::Timestamp::Convert(timestamp_ns, out_date, out_time, out_nanos);

        int32_t out_year, out_month, out_day;
        duckdb::Date::Convert(out_date, out_year, out_month, out_day);

        int32_t out_hour, out_min, out_sec, out_micros;
        duckdb::Time::Convert(out_time, out_hour, out_min, out_sec, out_micros);

        sink = nif::make_struct(env, "Elixir.Duckdbex.TimestampTzNs", {
          {"datetime", make_datetime_term(env, out_year, out_month, out_day, out_hour, out_min, out_sec, out_micros)},
          {"nanosecond", enif_make_int(env, out_nanos)},
        });

        return true;
      }
    case duckdb::LogicalTypeId::TIMESTAMP_MS: {
        auto millisecond_value = duckdb::TimestampMSValue::Get(value);
        if (infinite_value_to_term(env, millisecond_value, sink))
          return true;

        if (raw) {
          sink = enif_make_int64(env, millisecond_value.value);
          return true;
        }

        auto date_value = value.DefaultCastAs(duckdb::LogicalType::DATE);
        auto time_value = value.DefaultCastAs(duckdb::LogicalType::TIME);

        duckdb::date_t out_date = duckdb::DateValue::Get(date_value);
        duckdb::dtime_t out_time = duckdb::TimeValue::Get(time_value);

        int32_t out_year, out_month, out_day;
        duckdb::Date::Convert(out_date, out_year, out_month, out_day);

        int32_t out_hour, out_min, out_sec, out_micros;
        duckdb::Time::Convert(out_time, out_hour, out_min, out_sec, out_micros);

        sink = make_naive_datetime_term(env, out_year, out_month, out_day, out_hour, out_min, out_sec, out_micros);

        return true;
      }
    case duckdb::LogicalTypeId::TIMESTAMP_SEC: {
        auto second_value = duckdb::TimestampSValue::Get(value);
        if (infinite_value_to_term(env, second_value, sink))
          return true;

        if (raw) {
          sink = enif_make_int64(env, second_value.value);
          return true;
        }

        auto date_value = value.DefaultCastAs(duckdb::LogicalType::DATE);
        auto time_value = value.DefaultCastAs(duckdb::LogicalType::TIME);

        duckdb::date_t out_date = duckdb::DateValue::Get(date_value);
        duckdb::dtime_t out_time = duckdb::TimeValue::Get(time_value);

        int32_t out_year, out_month, out_day;
        duckdb::Date::Convert(out_date, out_year, out_month, out_day);

        int32_t out_hour, out_min, out_sec, out_micros;
        duckdb::Time::Convert(out_time, out_hour, out_min, out_sec, out_micros);

        sink = make_naive_datetime_term(env, out_year, out_month, out_day, out_hour, out_min, out_sec, out_micros);

        return true;
      }
    case duckdb::LogicalTypeId::INTERVAL: {
        duckdb::interval_t interval = duckdb::IntervalValue::Get(value);

        int64_t months, days, micros;
        interval.Normalize(months, days, micros);

        sink = nif::make_struct(env, "Elixir.Duckdbex.Interval", {
          {"months", enif_make_int64(env, months)},
          {"days", enif_make_int64(env, days)},
          {"micros", enif_make_int64(env, micros)},
        });

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
        return children_to_term(env, duckdb::ListValue::GetChildren(value), sink, raw);
      }
    case duckdb::LogicalTypeId::ARRAY: {
        return children_to_term(env, duckdb::ArrayValue::GetChildren(value), sink, raw);
      }
    case duckdb::LogicalTypeId::TUPLE: {
        auto& values = duckdb::StructValue::GetChildren(value);

        if (values.empty()) {
          sink = enif_make_tuple(env, 0);
          return true;
        }

        std::vector<ERL_NIF_TERM> terms(values.size());

        for (size_t i = 0; i < values.size(); i++) {
          if (!value_to_term(env, values[i], terms[i], raw))
            return false;
        }

        sink = enif_make_tuple_from_array(env, &terms[0], terms.size());
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
          if (!value_to_term(env, pair[0], key_term, raw) || !value_to_term(env, pair[1], val_term, raw))
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
          if (!value_to_term(env, values[i], val_term, raw))
            return false;
          keys_array[i] = nif::make_binary_term(env, names[i].first.GetIdentifierName());
          values_array[i] = val_term;
        }

        return enif_make_map_from_arrays(env, &keys_array[0], &values_array[0], names.size(), &sink);
      }
    case duckdb::LogicalTypeId::VARIANT: {
        return nif::value_to_term(env, duckdb::VariantValue::GetValue(value), sink, raw);
      }
    case duckdb::LogicalTypeId::UNION: {
      std::vector<ERL_NIF_TERM> keys_array(1);
      std::vector<ERL_NIF_TERM> values_array(1);

      auto member_name = duckdb::UnionType::GetMemberName(value.type(), duckdb::UnionValue::GetTag(value));
      auto& member_value = duckdb::UnionValue::GetValue(value);

      ERL_NIF_TERM union_tag_name_term = nif::make_binary_term(env, member_name.GetIdentifierName());

      ERL_NIF_TERM union_value_term;
      if (!value_to_term(env, member_value, union_value_term, raw))
        return false;

      sink = nif::make_struct(env, "Elixir.Duckdbex.Union", {
        {"tag", union_tag_name_term},
        {"value", union_value_term},
      });

      return true;
    }
    default:
      return false;
  };

  return false;
}
