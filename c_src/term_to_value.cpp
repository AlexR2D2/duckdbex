#include "term_to_value.h"
#include "duckdb.hpp"
#include "map_iterator.h"
#include "term.h"
#include <iostream>

namespace {
  template <class T>
  inline bool try_convert_to_hugeint(const T& value, duckdb::Value& sink) {
    duckdb::hugeint_t result;
    if (!duckdb::Hugeint::TryConvert(value, result))
      return false;

    sink = move(duckdb::Value::HUGEINT(result));
    return true;
  }

  template <class T>
  inline bool try_convert_to_uhugeint(const T& value, duckdb::Value& sink) {
    duckdb::uhugeint_t result;
    if (!duckdb::Uhugeint::TryConvert(value, result))
      return false;

    sink = move(duckdb::Value::UHUGEINT(result));
    return true;
  }

  bool enif_get_date(ErlNifEnv* env, ERL_NIF_TERM date_term, int& year, int& month, int& day) {
    int arity = 0;
    const ERL_NIF_TERM* date_parts;

    return enif_get_tuple(env, date_term, &arity, &date_parts) && arity == 3 &&
           enif_get_int(env, date_parts[0], &year ) && year > 0  &&
           enif_get_int(env, date_parts[1], &month) && month > 0 && month < 13 &&
           enif_get_int(env, date_parts[2], &day) && day > 0 && day < 32;
  }

  bool enif_get_time(ErlNifEnv* env, ERL_NIF_TERM time_term, int& hour, int& minute, int& second, int& precision) {
    int arity = 0;
    const ERL_NIF_TERM* time_parts;

    return enif_get_tuple(env, time_term, &arity, &time_parts) && arity == 4 &&
           enif_get_int(env, time_parts[0], &hour) && hour > -1 && hour < 24 &&
           enif_get_int(env, time_parts[1], &minute) && minute > -1 && minute < 60 &&
           enif_get_int(env, time_parts[2], &second) && second > -1 && second < 60 &&
           enif_get_int(env, time_parts[3], &precision) && precision > -1; // precision can be different in timestamp's
  }

  bool enif_get_time_tz(ErlNifEnv* env,
    ERL_NIF_TERM time_term,
    int& hour, int& minute, int& second, int& precision,
    int& offset_hour, int& offset_minute) {

    int arity = 0;
    const ERL_NIF_TERM* time_parts;
    const ERL_NIF_TERM* offset_parts;

    return enif_get_tuple(env, time_term, &arity, &time_parts) && arity == 5 &&
           enif_get_int(env, time_parts[0], &hour) && hour > -1 && hour < 24 &&
           enif_get_int(env, time_parts[1], &minute) && minute > -1 && minute < 60 &&
           enif_get_int(env, time_parts[2], &second) && second > -1 && second < 60 &&
           enif_get_int(env, time_parts[3], &precision) && precision > -1 && // precision can be different
           enif_get_tuple(env, time_parts[4], &arity, &offset_parts) && arity == 2 &&
           enif_get_int(env, offset_parts[0], &offset_hour) && offset_hour > -17 && offset_hour < 17 &&
           enif_get_int(env, offset_parts[1], &offset_minute) && offset_minute > -1 && offset_minute < 60;
  }

  bool enif_get_date_time(ErlNifEnv* env,
    ERL_NIF_TERM date_time_term,
    int& year, int& month, int& day,
    int& hour, int& minute, int& second, int& precision) {

    int arity = 0;

    const ERL_NIF_TERM* date_time_tuple;
    if (!enif_get_tuple(env, date_time_term, &arity, &date_time_tuple) || arity != 2)
      return false;

    return enif_get_date(env, date_time_tuple[0], year, month, day) &&
           enif_get_time(env, date_time_tuple[1], hour, minute, second, precision);
  }

  bool enif_get_date_time_tz(ErlNifEnv* env,
    ERL_NIF_TERM date_time_term,
    int& year, int& month, int& day,
    int& hour, int& minute, int& second, int& precision,
    int& offset_hour, int& offset_minute) {

    int arity = 0;

    const ERL_NIF_TERM* date_time_tuple;
    if (!enif_get_tuple(env, date_time_term, &arity, &date_time_tuple) || arity != 2)
      return false;

    return enif_get_date(env, date_time_tuple[0], year, month, day) &&
           enif_get_time_tz(env, date_time_tuple[1], hour, minute, second, precision, offset_hour, offset_minute);
  }
}

bool nif::term_to_null(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  unsigned atom_len = 0;
  if (!enif_get_atom_length(env, term, &atom_len, ERL_NIF_LATIN1))
    return false;

  if(atom_len != 3)
    return false;

  std::vector<char> atom(4);
  if(!enif_get_atom(env, term, &atom[0], 4, ERL_NIF_LATIN1))
    return false;

  if (!std::strcmp(&atom[0], "nil")) {
    sink = move(duckdb::Value(duckdb::LogicalType::SQLNULL));
    return true;
  }

  return false;
}

bool nif::term_to_boolean(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  unsigned atom_len = 0;
  if (!enif_get_atom_length(env, term, &atom_len, ERL_NIF_LATIN1))
    return false;

  std::vector<char> atom(atom_len + 1);
  if(!enif_get_atom(env, term, &atom[0], atom.size(), ERL_NIF_LATIN1))
    return false;

  if (!std::strcmp(&atom[0], "true")) {
    sink = move(duckdb::Value::BOOLEAN(true));
    return true;
  }

  if (!std::strcmp(&atom[0], "false")) {
    sink = move(duckdb::Value::BOOLEAN(false));
    return true;
  }

  return false;
}

bool nif::term_to_string(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  ErlNifBinary bin;
  if (enif_inspect_binary(env, term, &bin)) {
    sink = move(duckdb::Value(std::string((const char*)bin.data, bin.size)));
    return true;
  }
  return false;
}

bool nif::term_to_enum(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  return term_to_string(env, term, sink);
}

bool nif::term_to_float(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  double a_double;
  if(enif_get_double(env, term, &a_double)) {
    sink = move(duckdb::Value::FLOAT(static_cast<float>(a_double)));
    return true;
  }

  ErlNifSInt64 an_int64;
  if (enif_get_int64(env, term, &an_int64)) {
    sink = move(duckdb::Value::FLOAT(static_cast<float>(an_int64)));
    return true;
  }

  return false;
}

bool nif::term_to_double(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  double a_double;
  if(enif_get_double(env, term, &a_double)) {
    sink = move(duckdb::Value::DOUBLE(a_double));
    return true;
  }

  ErlNifSInt64 an_int64;
  if (enif_get_int64(env, term, &an_int64)) {
    sink = move(duckdb::Value::DOUBLE(static_cast<double>(an_int64)));
    return true;
  }

  return false;
}

bool nif::term_to_integer(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  int an_int;
  if(enif_get_int(env, term, &an_int)) {
    sink = move(duckdb::Value::INTEGER(an_int));
    return true;
  }

  return false;
}

bool nif::term_to_uinteger(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  unsigned int an_uint;
  if(enif_get_uint(env, term, &an_uint)) {
    sink = move(duckdb::Value::UINTEGER(an_uint));
    return true;
  }

  return false;
}

bool nif::term_to_smallint(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  int an_int;
  if(enif_get_int(env, term, &an_int)) {
    sink = move(duckdb::Value::SMALLINT(an_int));
    return true;
  }

  return false;
}

bool nif::term_to_usmallint(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  unsigned int an_uint;
  if(enif_get_uint(env, term, &an_uint)) {
    sink = move(duckdb::Value::USMALLINT(an_uint));
    return true;
  }

  return false;
}

bool nif::term_to_tinyint(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  int an_int;
  if(enif_get_int(env, term, &an_int)) {
    sink = move(duckdb::Value::TINYINT(an_int));
    return true;
  }
  return false;
}

bool nif::term_to_bigint(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  ErlNifSInt64 an_int64;
  if (enif_get_int64(env, term, &an_int64)) {
    sink = move(duckdb::Value::BIGINT(static_cast<int64_t>(an_int64)));
    return true;
  }

  long int a_long_int;
  if(enif_get_long(env, term, &a_long_int)) {
    sink = move(duckdb::Value::BIGINT(static_cast<int64_t>(a_long_int)));
    return true;
  }

  return false;
}

bool nif::term_to_ubigint(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  ErlNifUInt64 an_uint64;
  if (enif_get_uint64(env, term, &an_uint64)) {
    sink = move(duckdb::Value::UBIGINT(an_uint64));
    return true;
  }

  unsigned long a_ulong_int;
  if(enif_get_ulong(env, term, &a_ulong_int)) {
    sink = move(duckdb::Value::UBIGINT(static_cast<int64_t>(a_ulong_int)));
    return true;
  }
  return false;
}

bool nif::term_to_utinyint(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  unsigned int an_uint;
  if(enif_get_uint(env, term, &an_uint)) {
    sink = move(duckdb::Value::UTINYINT(an_uint));
    return true;
  }
  return false;
}

bool nif::term_to_hugeint(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  if (enif_is_tuple(env, term)) {
    int arity = 0;
    const ERL_NIF_TERM* hugeint_tuple;
    if (!enif_get_tuple(env, term, &arity, &hugeint_tuple) || arity != 2)
      return false;

    ErlNifSInt64 hugeint_upper;
    if (!enif_get_int64(env, hugeint_tuple[0], &hugeint_upper))
      return false;

    ErlNifUInt64 hugeint_lower;
    if (!enif_get_uint64(env, hugeint_tuple[1], &hugeint_lower))
      return false;

    duckdb::hugeint_t hugeint = 0;
    hugeint.lower = hugeint_lower;
    hugeint.upper = hugeint_upper;

    duckdb::hugeint_t result;
    if (!duckdb::Hugeint::TryConvert(hugeint, result))
      return false;

    sink = move(duckdb::Value::HUGEINT(result));
    return true;
  }

  ErlNifBinary bin;
  if (enif_inspect_binary(env, term, &bin))
    return try_convert_to_hugeint(std::string((const char*)bin.data, bin.size).c_str(), sink);

  double a_double;
  if(enif_get_double(env, term, &a_double))
    return try_convert_to_hugeint(a_double, sink);

  unsigned int an_uint;
  if(enif_get_uint(env, term, &an_uint))
    return try_convert_to_hugeint(an_uint, sink);

  ErlNifUInt64 an_uint64;
  if (enif_get_uint64(env, term, &an_uint64))
    return try_convert_to_hugeint(static_cast<uint64_t>(an_uint64), sink);

  int an_int;
  if(enif_get_int(env, term, &an_int))
    return try_convert_to_hugeint(an_int, sink);

  ErlNifSInt64 an_int64;
  if (enif_get_int64(env, term, &an_int64))
    return try_convert_to_hugeint(static_cast<int64_t>(an_int64), sink);

  return false;
}

bool nif::term_to_uhugeint(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  if (enif_is_tuple(env, term)) {
    int arity = 0;
    const ERL_NIF_TERM* uhugeint_tuple;
    if (!enif_get_tuple(env, term, &arity, &uhugeint_tuple) || arity != 2)
      return false;

    ErlNifUInt64 uhugeint_upper;
    if (!enif_get_uint64(env, uhugeint_tuple[0], &uhugeint_upper))
      return false;

    ErlNifUInt64 uhugeint_lower;
    if (!enif_get_uint64(env, uhugeint_tuple[1], &uhugeint_lower))
      return false;

    duckdb::uhugeint_t uhugeint = 0;
    uhugeint.lower = uhugeint_lower;
    uhugeint.upper = uhugeint_upper;

    duckdb::uhugeint_t result;
    if (!duckdb::Uhugeint::TryConvert(uhugeint, result))
      return false;

    sink = move(duckdb::Value::UHUGEINT(result));
    return true;
  }

  ErlNifBinary bin;
  if (enif_inspect_binary(env, term, &bin))
    return try_convert_to_uhugeint(std::string((const char*)bin.data, bin.size).c_str(), sink);

  double a_double;
  if(enif_get_double(env, term, &a_double))
    return try_convert_to_uhugeint(a_double, sink);

  unsigned int an_uint;
  if(enif_get_uint(env, term, &an_uint))
    return try_convert_to_uhugeint(an_uint, sink);

  ErlNifUInt64 an_uint64;
  if (enif_get_uint64(env, term, &an_uint64))
    return try_convert_to_uhugeint(static_cast<uint64_t>(an_uint64), sink);

  int an_int;
  if(enif_get_int(env, term, &an_int))
    return try_convert_to_uhugeint(an_int, sink);

  ErlNifSInt64 an_int64;
  if (enif_get_int64(env, term, &an_int64))
    return try_convert_to_uhugeint(static_cast<int64_t>(an_int64), sink);

  return false;
}

bool nif::term_to_decimal(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  int arity = 0;
  const ERL_NIF_TERM* decimal_tuple;
  if (enif_get_tuple(env, term, &arity, &decimal_tuple) && arity == 3) {
    ErlNifSInt64 value;
    if (!enif_get_int64(env, decimal_tuple[0], &value))
      return false;

    unsigned int width;
    if(!enif_get_uint(env, decimal_tuple[1], &width))
      return false;

    unsigned int scale;
    if(!enif_get_uint(env, decimal_tuple[2], &scale))
      return false;

    sink = move(duckdb::Value::DECIMAL((int64_t)value, width, scale));

    return true;
  }

  if (enif_is_binary(env, term))
    return term_to_string(env, term, sink);

  if (enif_is_tuple(env, term))
    return term_to_hugeint(env, term, sink);

  if (enif_is_number(env, term))
    return term_to_double(env, term, sink) || term_to_integer(env, term, sink) || term_to_bigint(env, term, sink);

  return false;
}

bool nif::term_to_uuid(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  if (enif_is_binary(env, term)) {
    ErlNifBinary bin;
    if (enif_inspect_binary(env, term, &bin)) {
      duckdb::hugeint_t result;
      if(!duckdb::UUID::FromCString((const char*)bin.data, bin.size, result))
        return false;

      sink = move(duckdb::Value::UUID(result));
      return true;
    }
  }

  duckdb::Value uhugeint_val;
  if (nif::term_to_uhugeint(env, term, uhugeint_val)) {
    duckdb::uhugeint_t uhugeint = duckdb::HugeIntValue::Get(uhugeint_val);
    duckdb::hugeint_t hugeint = duckdb::UUID::FromUHugeint(uhugeint);
    sink = move(duckdb::Value::UUID(hugeint));
    return true;
  }

  return false;
}

bool nif::term_to_date(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  if (enif_is_tuple(env, term)) {
    int year, month, day;
    if (!enif_get_date(env, term, year, month, day))
      return false;

    duckdb::date_t date;
    if (!duckdb::Date::TryFromDate((int32_t)year, (int32_t)month, (int32_t)day, date))
      return false;

    sink = move(duckdb::Value::DATE(date));
    return true;
  }

  ErlNifBinary bin;
  if(enif_inspect_binary(env, term, &bin)) {
    duckdb::idx_t pos = 0;
    duckdb::date_t date;
    bool special = false;
    bool strict = false;

    duckdb::DateCastResult result = duckdb::Date::TryConvertDate((const char*)bin.data, bin.size, pos, date, special, strict);
    if (result == duckdb::DateCastResult::SUCCESS) {
      sink = move(duckdb::Value::DATE(date));
      return true;
    } else {
      // TODO: Forward cast error to the client
      // enum class DateCastResult : uint8_t { SUCCESS, ERROR_INCORRECT_FORMAT, ERROR_RANGE };
      // return false;
    }
  }

  return false;
}

bool nif::term_to_time(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  if (enif_is_tuple(env, term)) {
    int hour, minute, second, micros;
    if (!enif_get_time(env, term, hour, minute, second, micros))
      return false;

    sink = move(duckdb::Value::TIME((int32_t)hour, (int32_t)minute, (int32_t)second, (int32_t)micros));

    return true;
  }

  ErlNifBinary bin;
  if(enif_inspect_binary(env, term, &bin)) {
    duckdb::idx_t pos = 0;
    duckdb::dtime_t time;
    bool special = false;
    if (!duckdb::Time::TryConvertTime((const char*)bin.data, bin.size, pos, time, special))
      return false;

    sink = move(duckdb::Value::TIME(time));
    return true;
  }

  return false;
}

bool nif::term_to_time_tz(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  if (enif_is_tuple(env, term)) {
    int hour, minute, second, micros, offset_hour, offset_minute;
    if (!enif_get_time_tz(env, term, hour, minute, second, micros, offset_hour, offset_minute))
      return false;

    int64_t time_in_microseconds =
      hour * duckdb::Interval::MICROS_PER_HOUR +
      minute * duckdb::Interval::MICROS_PER_MINUTE +
      second * duckdb::Interval::MICROS_PER_SEC +
      micros;

    int32_t offset =
      (std::abs(offset_hour) * duckdb::Interval::SECS_PER_HOUR +
      offset_minute * duckdb::Interval::SECS_PER_MINUTE) * (offset_hour < 0 ? -1 : 1);

    sink = move(duckdb::Value::TIMETZ(duckdb::dtime_tz_t(duckdb::dtime_t(time_in_microseconds), offset)));

    return true;
  }

  ErlNifBinary bin;
  if(enif_inspect_binary(env, term, &bin)) {
    duckdb::idx_t pos = 0;
    duckdb::dtime_tz_t result;
    bool has_offset = false;
    if (!duckdb::Time::TryConvertTimeTZ((const char*)bin.data, bin.size, pos, result, has_offset))
      return false;

    sink = move(duckdb::Value::TIMETZ(result));
    return true;
  }

  return false;
}

bool nif::term_to_timestamp(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  ErlNifBinary bin;

  if (enif_is_tuple(env, term)) {
    int year, month, day, hour, minute, second, micros = 0;
    if (!enif_get_date_time(env, term, year, month, day, hour, minute, second, micros))
      return false;

    sink = move(duckdb::Value::TIMESTAMP(year, month, day, hour, minute, second, micros));

    return true;
  } else if(enif_inspect_binary(env, term, &bin)) {
    duckdb::timestamp_t result;
    if (duckdb::TimestampCastResult::SUCCESS == duckdb::Timestamp::TryConvertTimestamp((const char*)bin.data, bin.size, result)) {
      sink = move(duckdb::Value::TIMESTAMP(result));
      return true;
    }
  }

  return false;
}

bool nif::term_to_timestamp_tz(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  if (enif_is_tuple(env, term)) {
    int year, month, day;
    int hour, minute, second, micros;
    int offset_hour, offset_minute;
    if (!enif_get_date_time_tz(env, term, year, month, day, hour, minute, second, micros, offset_hour, offset_minute))
      return false;

    duckdb::date_t date;
    if (!duckdb::Date::TryFromDate((int32_t)year, (int32_t)month, (int32_t)day, date))
      return false;

    int64_t time_in_microseconds =
      hour * duckdb::Interval::MICROS_PER_HOUR +
      minute * duckdb::Interval::MICROS_PER_MINUTE +
      second * duckdb::Interval::MICROS_PER_SEC +
      micros;

    int32_t offset =
      (std::abs(offset_hour) * duckdb::Interval::SECS_PER_HOUR +
      offset_minute * duckdb::Interval::SECS_PER_MINUTE) * (offset_hour < 0 ? -1 : 1);

    duckdb::dtime_tz_t timez(duckdb::dtime_t(time_in_microseconds), offset);

    duckdb::timestamp_t timestamp;
    if (!duckdb::Timestamp::TryFromDatetime(date, timez, timestamp))
      return false;

    sink = move(duckdb::Value::TIMESTAMPTZ(duckdb::timestamp_tz_t(timestamp)));
    return true;
  }

  ErlNifBinary bin;
  if(enif_inspect_binary(env, term, &bin)) {
    bool has_offset = false;
    duckdb::string_t tz;
    duckdb::timestamp_t timestamp;

    duckdb::TimestampCastResult result = duckdb::Timestamp::TryConvertTimestampTZ((const char*)bin.data, bin.size, timestamp, has_offset, tz);
    if (result == duckdb::TimestampCastResult::SUCCESS) {
      sink = move(duckdb::Value::TIMESTAMPTZ(duckdb::timestamp_tz_t(timestamp)));
      return true;
    } else {
      // TODO: Return the cast result to client
      // enum class TimestampCastResult : uint8_t { SUCCESS, ERROR_INCORRECT_FORMAT, ERROR_NON_UTC_TIMEZONE, ERROR_RANGE };
      // return false;
    }
  }

  return false;
}

bool nif::term_to_timestamp_ns(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  ErlNifBinary bin;

  if (enif_is_tuple(env, term)) {
    int year, month, day, hour, minute, second, nanosecond = 0;
    if (!enif_get_date_time(env, term, year, month, day, hour, minute, second, nanosecond))
      return false;

    int32_t micros = nanosecond / duckdb::Interval::NANOS_PER_MICRO;
    int32_t nanos = nanosecond % duckdb::Interval::NANOS_PER_MICRO;

    const duckdb::timestamp_t timestamp = duckdb::Timestamp::FromDatetime(
      duckdb::Date::FromDate(year, month, day),
      duckdb::Time::FromTime(hour, minute, second, micros));

    duckdb::timestamp_ns_t timestamp_ns;
    if (!duckdb::Timestamp::TryFromTimestampNanos(timestamp, nanos, timestamp_ns))
      return false;

    sink = move(duckdb::Value::CreateValue(timestamp_ns));

    return true;
  } else if(enif_inspect_binary(env, term, &bin)) {
    duckdb::timestamp_ns_t result;
    if (duckdb::TimestampCastResult::SUCCESS == duckdb::Timestamp::TryConvertTimestamp((const char*)bin.data, bin.size, result)) {
      sink = move(duckdb::Value::CreateValue(result));
      return true;
    }
  }

  return false;
}

bool nif::term_to_timestamp_ms(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  ErlNifBinary bin;

  if (enif_is_tuple(env, term)) {
    int year, month, day, hour, minute, second, milliseconds = 0;
    if (!enif_get_date_time(env, term, year, month, day, hour, minute, second, milliseconds))
      return false;

    int32_t micros = (int32_t)(milliseconds * duckdb::Interval::MICROS_PER_MSEC);

    sink = move(duckdb::Value::TIMESTAMP(year, month, day, hour, minute, second, micros));

    return true;
  } else if(enif_inspect_binary(env, term, &bin)) {
    duckdb::timestamp_t result;
    if (duckdb::TimestampCastResult::SUCCESS == duckdb::Timestamp::TryConvertTimestamp((const char*)bin.data, bin.size, result)) {
      sink = move(duckdb::Value::TIMESTAMP(result));
      return true;
    }
  }

  return false;
}

bool nif::term_to_timestamp_sec(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  ErlNifBinary bin;

  if (enif_is_tuple(env, term)) {
    int year, month, day, hour, minute, second, precision = 0;
    if (!enif_get_date_time(env, term, year, month, day, hour, minute, second, precision))
      return false;

    sink = move(duckdb::Value::TIMESTAMP(year, month, day, hour, minute, second, 0));

    return true;
  } else if(enif_inspect_binary(env, term, &bin)) {
    duckdb::timestamp_t timestamp;
    if (duckdb::TimestampCastResult::SUCCESS == duckdb::Timestamp::TryConvertTimestamp((const char*)bin.data, bin.size, timestamp)) {
      sink = move(duckdb::Value::TIMESTAMP(timestamp));
      return true;
    }
  }

  return false;
}

bool nif::term_to_blob(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  //duckdb internally pass the data ptr into string constructor
  // make_shared_ptr<StringValueInfo>(string(const_char_ptr_cast(data), len));

  ErlNifBinary bin;

  if (enif_inspect_iolist_as_binary(env, term, &bin)) {
    sink = move(duckdb::Value::BLOB(static_cast<duckdb::const_data_ptr_t>(bin.data), bin.size));
    return true;
  }

  if (enif_inspect_binary(env, term, &bin)) {
    sink = move(duckdb::Value::BLOB(static_cast<duckdb::const_data_ptr_t>(bin.data), bin.size));
    return true;
  }

  return false;
}

bool nif::term_to_interval(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  if (enif_is_tuple(env, term)) {
    int arity = 0;
    const ERL_NIF_TERM* interval_parts;
    if (!enif_get_tuple(env, term, &arity, &interval_parts) || arity != 3)
      return false;

    int32_t months, days;
    ErlNifSInt64 micros;
    if (!enif_get_int(env, interval_parts[0], &months) ||
        !enif_get_int(env, interval_parts[1], &days) ||
        !enif_get_int64(env, interval_parts[2], &micros))
      return false;

    sink = move(duckdb::Value::INTERVAL(months, days, micros));
    return true;
  }

  if (enif_is_number(env, term)) {
    ErlNifSInt64 micros;
    if (!enif_get_int64(env, term, &micros))
      return false;

    sink = move(duckdb::Value::INTERVAL(duckdb::Interval::FromMicro(micros)));
    return true;
  }

  if (enif_is_binary(env, term)) {
    ErlNifBinary bin;
    if (!enif_inspect_binary(env, term, &bin))
      return false;

    duckdb::interval_t result;
    duckdb::string error_message;
    bool strict = false;
    if (!duckdb::Interval::FromCString((const char*)bin.data, bin.size, result, &error_message, strict))
      return false;

    sink = move(duckdb::Value::INTERVAL(result));
    return true;
  }

  return false;
}

bool nif::term_to_list(ErlNifEnv* env, ERL_NIF_TERM term, const duckdb::LogicalType& list_type, duckdb::Value& sink) {
  if (!enif_is_list(env, term))
    return false;

  duckdb::LogicalType child_type = duckdb::ListType::GetChildType(list_type);

  unsigned list_length = 0;
  if (!enif_get_list_length(env, term, &list_length))
    return false;

  if (list_length == 0) {
    sink = move(duckdb::Value::LIST(child_type, std::vector<duckdb::Value>()));
    return true;
  }

  duckdb::vector<duckdb::Value> values(list_length);

  ERL_NIF_TERM list = term;
  for (size_t i = 0; i < list_length; i++) {
    ERL_NIF_TERM head, tail;
    duckdb::Value child;
    if (!enif_get_list_cell(env, list, &head, &tail) ||
        !nif::term_to_value(env, head, child_type, child))
      return false;

    values[i] = (move(child));
    list = tail;
  }

  sink = move(duckdb::Value::LIST(child_type, values));

  return true;
}

bool nif::term_to_array(ErlNifEnv* env, ERL_NIF_TERM term, const duckdb::LogicalType& list_type, duckdb::Value& sink) {
  if (!enif_is_list(env, term))
    return false;

  duckdb::LogicalType child_type = duckdb::ListType::GetChildType(list_type);

  unsigned list_length = 0;
  if (!enif_get_list_length(env, term, &list_length))
    return false;

  if (list_length == 0) {
    sink = move(duckdb::Value::ARRAY(child_type, duckdb::vector<duckdb::Value>(0)));
    return true;
  }

  duckdb::vector<duckdb::Value> values(list_length);

  ERL_NIF_TERM list = term;
  for (size_t i = 0; i < list_length; i++) {
    ERL_NIF_TERM head, tail;
    duckdb::Value child;
    if (!enif_get_list_cell(env, list, &head, &tail) ||
        !nif::term_to_value(env, head, child_type, child))
      return false;

    values[i] = (move(child));
    list = tail;
  }

  sink = move(duckdb::Value::ARRAY(child_type, values));

  return true;
}

bool nif::term_to_map(ErlNifEnv* env, ERL_NIF_TERM term, const duckdb::LogicalType& map_type, duckdb::Value& sink) {
  if (!enif_is_list(env, term))
    return false;

  unsigned list_length = 0;
  if (!enif_get_list_length(env, term, &list_length))
    return false;

  auto &key_type = duckdb::MapType::KeyType(map_type);
  auto &value_type = duckdb::MapType::ValueType(map_type);

  if (list_length == 0) {
    sink = move(duckdb::Value::MAP(key_type, value_type, std::vector<duckdb::Value>(), std::vector<duckdb::Value>()));
    return true;
  }

  duckdb::vector<duckdb::Value> keys(list_length);
  duckdb::vector<duckdb::Value> values(list_length);

  ERL_NIF_TERM list = term;
  for (size_t i = 0; i < list_length; i++) {
    ERL_NIF_TERM head, tail;
    int arity = 0;
    const ERL_NIF_TERM* pair;
    duckdb::Value key, value;

    if (!enif_get_list_cell(env, list, &head, &tail) ||
        !enif_get_tuple(env, head, &arity, &pair) || arity != 2 ||
        !term_to_value(env, pair[0], key_type, key) ||
        !term_to_value(env, pair[1], value_type, value))
      return false;

    keys[i] = move(key);
    values[i] = move(value);

    list = tail;
  }

  sink = move(duckdb::Value::MAP(key_type, value_type, keys, values));

  return true;
}

bool nif::term_to_struct(ErlNifEnv* env, ERL_NIF_TERM term, const duckdb::LogicalType& map_type, duckdb::Value& sink) {
  size_t map_size = 0;
  if (!enif_get_map_size(env, term, &map_size))
    return false;

  duckdb::child_list_t<duckdb::Value> children;

  for (duckdb::idx_t child_idx = 0; child_idx < map_size; child_idx++) {
    auto field_name = duckdb::StructType::GetChildName(map_type, child_idx);

    ERL_NIF_TERM value_term;
    if (!enif_get_map_value(env, term, nif::make_binary_term(env, field_name), &value_term))
      return false;

    duckdb::Value value_sink;
    if (!term_to_value(env, value_term, duckdb::StructType::GetChildType(map_type, child_idx), value_sink))
      return false;

    children.push_back(make_pair(field_name, move(value_sink)));
  }

  sink = duckdb::Value::STRUCT(move(children));

  return true;
}

bool nif::term_to_union(ErlNifEnv* env, ERL_NIF_TERM term, const duckdb::LogicalType& union_type, duckdb::Value& sink) {
  int arity = 0;
  const ERL_NIF_TERM* tuple;
  ErlNifBinary bin;
  if (!enif_is_tuple(env, term) ||
      !enif_get_tuple(env, term, &arity, &tuple) || arity != 2 ||
      !enif_is_binary(env, tuple[0]) ||
      !enif_inspect_binary(env, tuple[0], &bin))
    return false;

  auto union_tag_name = std::string((const char*)bin.data, bin.size);

  auto member_types = duckdb::UnionType::CopyMemberTypes(union_type);

  auto it = std::find_if(member_types.begin(), member_types.end(),
                          [=](const duckdb::child_list_t<duckdb::LogicalType>::value_type &it) -> bool {
                            return it.first == union_tag_name;
                          });

  if (it == member_types.end())
    return false;

  std::size_t union_tag_index = std::distance(member_types.begin(), it);

  duckdb::Value union_value;
  if (!term_to_value(env, tuple[1], it->second, union_value))
    return false;

  sink = duckdb::Value::UNION(member_types, union_tag_index, std::move(union_value));

  return true;
}

bool nif::term_to_value(ErlNifEnv* env, ERL_NIF_TERM term, const duckdb::LogicalType& value_type, duckdb::Value& sink) {
  if(term_to_null(env, term, sink))
    return true;

  // <dbg>
  // std::cout << "term_to_value: value_type: " << value_type.ToString() << std::endl;
  // std::cout << "term_to_value: value enum code: " << unsigned(static_cast<std::underlying_type<duckdb::LogicalTypeId>::type>(value_type.id())) << std::endl;
  // </dbg>

  switch(value_type.id()) {
    case duckdb::LogicalTypeId::SQLNULL:
      return term_to_null(env, term, sink);

    case duckdb::LogicalTypeId::BOOLEAN:
      return term_to_boolean(env, term, sink);

    case duckdb::LogicalTypeId::UUID:
      return term_to_uuid(env, term, sink);

    case duckdb::LogicalTypeId::FLOAT:
      return term_to_float(env, term, sink);

    case duckdb::LogicalTypeId::DOUBLE:
      return term_to_double(env, term, sink);

    case duckdb::LogicalTypeId::DECIMAL:
      return term_to_decimal(env, term, sink);

    case duckdb::LogicalTypeId::BIGINT:
      return term_to_bigint(env, term, sink);

    case duckdb::LogicalTypeId::UBIGINT:
      return term_to_ubigint(env, term, sink);

    case duckdb::LogicalTypeId::INTEGER:
      return term_to_integer(env, term, sink);

    case duckdb::LogicalTypeId::UINTEGER:
      return term_to_uinteger(env, term, sink);

    case duckdb::LogicalTypeId::SMALLINT:
      return term_to_smallint(env, term, sink);

    case duckdb::LogicalTypeId::USMALLINT:
      return term_to_usmallint(env, term, sink);

    case duckdb::LogicalTypeId::TINYINT:
      return term_to_tinyint(env, term, sink);

    case duckdb::LogicalTypeId::UTINYINT:
      return term_to_utinyint(env, term, sink);

    case duckdb::LogicalTypeId::HUGEINT:
      return term_to_hugeint(env, term, sink);

    case duckdb::LogicalTypeId::UHUGEINT:
      return term_to_uhugeint(env, term, sink);

    case duckdb::LogicalTypeId::CHAR:
    case duckdb::LogicalTypeId::VARCHAR:
      return term_to_string(env, term, sink);

    case duckdb::LogicalTypeId::DATE:
      return term_to_date(env, term, sink);

    case duckdb::LogicalTypeId::TIME:
      return term_to_time(env, term, sink);

    case duckdb::LogicalTypeId::TIME_TZ:
      return term_to_time_tz(env, term, sink);

    case duckdb::LogicalTypeId::TIMESTAMP:
      return term_to_timestamp(env, term, sink);

    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      return term_to_timestamp_tz(env, term, sink);

    case duckdb::LogicalTypeId::TIMESTAMP_NS:
      return term_to_timestamp_ns(env, term, sink);

    case duckdb::LogicalTypeId::TIMESTAMP_MS:
      return term_to_timestamp_ms(env, term, sink);

    case duckdb::LogicalTypeId::TIMESTAMP_SEC:
      return term_to_timestamp_sec(env, term, sink);

    case duckdb::LogicalTypeId::BLOB:
      return term_to_blob(env, term, sink);

    case duckdb::LogicalTypeId::INTERVAL:
      return term_to_interval(env, term, sink);

    case duckdb::LogicalTypeId::ENUM:
      return term_to_enum(env, term, sink);

    case duckdb::LogicalTypeId::LIST:
      return term_to_list(env, term, value_type, sink);

    case duckdb::LogicalTypeId::ARRAY:
      return term_to_array(env, term, value_type, sink);

    case duckdb::LogicalTypeId::MAP:
      return term_to_map(env, term, value_type, sink);

    case duckdb::LogicalTypeId::STRUCT:
      return term_to_struct(env, term, value_type, sink);

    case duckdb::LogicalTypeId::UNION:
      return term_to_union(env, term, value_type, sink);

    default:
      return false;
  };
}
