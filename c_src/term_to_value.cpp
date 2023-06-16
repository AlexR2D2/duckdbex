#include "term_to_value.h"
#include "duckdb.hpp"
#include "map_iterator.h"
#include "term.h"
#include <iostream>

namespace {
  template <class T>
  inline bool try_convert_to_hugeint(const T& value, duckdb::Value& sink) {
    duckdb::hugeint_t result;
    if (duckdb::Hugeint::TryConvert(value, result)) {
      sink = move(duckdb::Value::HUGEINT(result));
      return true;
    }
    return false;
  }
}

bool nif::term_to_float(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  double a_double;
  if(enif_get_double(env, term, &a_double)) {
    sink = move(duckdb::Value::FLOAT(a_double));
    return true;
  }
  ErlNifSInt64 an_int64;
  if (enif_get_int64(env, term, &an_int64)) {
    sink = move(duckdb::Value::DOUBLE(an_int64));
    return true;
  }
  return true;
}

bool nif::term_to_double(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  double a_double;
  if(enif_get_double(env, term, &a_double)) {
    sink = move(duckdb::Value::DOUBLE(a_double));
    return true;
  }
  ErlNifSInt64 an_int64;
  if (enif_get_int64(env, term, &an_int64)) {
    sink = move(duckdb::Value::DOUBLE(an_int64));
    return true;
  }
  return true;
}

bool nif::term_to_integer(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  int an_int;
  if(enif_get_int(env, term, &an_int)) {
    sink = move(duckdb::Value::INTEGER(an_int));
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
    sink = move(duckdb::Value::BIGINT(an_int64));
    return true;
  }

  long int a_long_int;
  if(enif_get_long(env, term, &a_long_int)) {
    sink = move(duckdb::Value::BIGINT(a_long_int));
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

bool nif::term_to_usmallint(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  unsigned int an_uint;
  if(enif_get_uint(env, term, &an_uint)) {
    sink = move(duckdb::Value::USMALLINT(an_uint));
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

bool nif::term_to_ubigint(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  ErlNifUInt64 an_uint64;
  if (enif_get_uint64(env, term, &an_uint64)) {
    sink = move(duckdb::Value::UBIGINT(an_uint64));
    return true;
  }

  unsigned long a_ulong_int;
  if(enif_get_ulong(env, term, &a_ulong_int)) {
    sink = move(duckdb::Value::UBIGINT(a_ulong_int));
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

bool nif::term_to_decimal(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  int arity = 0;
  const ERL_NIF_TERM* tuple;

  if (enif_get_tuple(env, term, &arity, &tuple)) {
    if (arity != 2)
      return false;

    if (nif::is_atom(env, tuple[0], "decimal")) {
      const ERL_NIF_TERM* decimal_tuple;
      if (!enif_get_tuple(env, tuple[1], &arity, &decimal_tuple) || arity != 3)
        return false;

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

    if (nif::is_atom(env, tuple[0], "hugeint"))
      return term_to_hugeint(env, term, sink);

    return false;
  } else {
    return term_to_string(env, term, sink)
      || term_to_double(env, term, sink)
      || term_to_integer(env, term, sink)
      || term_to_bigint(env, term, sink)
      || term_to_hugeint(env, term, sink);
  }
}

bool nif::term_to_uuid(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  ErlNifBinary bin;
  if (enif_inspect_binary(env, term, &bin)) {
    duckdb::hugeint_t result;
    if(duckdb::UUID::FromCString((const char*)bin.data, bin.size, result)) {
      sink = move(duckdb::Value::UUID(result));
      return true;
    }
  }
  return false;
}

bool nif::term_to_date(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  int arity = 0;
  const ERL_NIF_TERM* tuple;
  ErlNifBinary bin;

  if (enif_get_tuple(env, term, &arity, &tuple) && arity == 3) {
    int year;
    if(!enif_get_int(env, tuple[0], &year))
      return false;

    int month;
    if(!enif_get_int(env, tuple[1], &month))
      return false;

    int day;
    if(!enif_get_int(env, tuple[2], &day))
      return false;

    duckdb::date_t result;
    if (duckdb::Date::TryFromDate((int32_t)year, (int32_t)month, (int32_t)day, result)) {
      sink = move(duckdb::Value::DATE(result));
      return true;
    }
  } else if(enif_inspect_binary(env, term, &bin)) {
    duckdb::idx_t pos = 0;
    duckdb::date_t result;
    bool special = false;
    if (duckdb::Date::TryConvertDate((const char*)bin.data, bin.size, pos, result, special)) {
      sink = move(duckdb::Value::DATE(result));
      return true;
    }
  }

  return false;
}

bool nif::term_to_time(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  int arity = 0;
  const ERL_NIF_TERM* tuple;
  ErlNifBinary bin;

  if (enif_get_tuple(env, term, &arity, &tuple) && (arity == 3 || arity == 4)) {
    int hour;
    if(!enif_get_int(env, tuple[0], &hour) && hour >= 0 && hour <= 23)
      return false;

    int minute;
    if(!enif_get_int(env, tuple[1], &minute) && minute >= 0 && minute <= 59)
      return false;

    int second;
    if(!enif_get_int(env, tuple[2], &second) && second >= 0 && second <= 59)
      return false;

    int micros = 0;
    if (arity == 4)
      if(!enif_get_int(env, tuple[3], &micros) && micros >= 0 && micros <= 999999)
        return false;

    sink = move(duckdb::Value::TIME((int32_t)hour, (int32_t)minute, (int32_t)second, (int32_t)micros));

    return true;
  } else if(enif_inspect_binary(env, term, &bin)) {
    duckdb::idx_t pos = 0;
    duckdb::dtime_t result;
    bool special = false;
    if (duckdb::Time::TryConvertTime((const char*)bin.data, bin.size, pos, result, special)) {
      sink = move(duckdb::Value::TIME(result));
      return true;
    }
  }

  return false;
}

bool nif::term_to_timestamp(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  int arity = 0;
  const ERL_NIF_TERM* tuple;
  ErlNifBinary bin;

  if (enif_get_tuple(env, term, &arity, &tuple) && arity == 2) {
    // parsing: {{2022, 10, 20}, {23, 59, 59, 999123}}
    duckdb::Value date, time;
    if (nif::term_to_date(env, tuple[0], date) && nif::term_to_time(env, tuple[1], time)) {
      duckdb::date_t a_date = date.GetValueUnsafe<duckdb::date_t>();
      duckdb::dtime_t a_time = time.GetValueUnsafe<duckdb::dtime_t>();
      sink = move(duckdb::Value::TIMESTAMP(a_date, a_time));
      return true;
    }
  } else if(enif_inspect_binary(env, term, &bin)) {
    // parsing: "2022-10-20 23:59:59.999123"
    duckdb::timestamp_t result;
    if (duckdb::TimestampCastResult::SUCCESS == duckdb::Timestamp::TryConvertTimestamp((const char*)bin.data, bin.size, result)) {
      sink = move(duckdb::Value::TIMESTAMP(result));
      return true;
    }
  }

  return false;
}

bool nif::term_to_timestamp_tz(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  int arity = 0;
  const ERL_NIF_TERM* tuple;
  ErlNifBinary bin;

  if (enif_get_tuple(env, term, &arity, &tuple) && (arity == 2 || arity == 3)) {
    // parsing: {{2022, 10, 20}, {23, 59, 59, 999123}, {5, 0}} # {5, 0} is UTC offset

    int offset_hours = 0;
    int offset_minutes = 0;

    int offset_arity = 0;
    const ERL_NIF_TERM* offset_tuple;
    if (arity == 3) {
      if (!enif_get_tuple(env, tuple[2], &offset_arity, &offset_tuple) && offset_arity != 2)
        return false;

      if(!enif_get_int(env, offset_tuple[0], &offset_hours) && offset_hours >= 0 && offset_hours <= 23)
        return false;

      if(!enif_get_int(env, offset_tuple[1], &offset_minutes) && offset_minutes >= 0 && offset_minutes <= 59)
        return false;
    }

    duckdb::Value date, time;
    if (nif::term_to_date(env, tuple[0], date) && nif::term_to_time(env, tuple[1], time)) {
      duckdb::date_t a_date = date.GetValueUnsafe<duckdb::date_t>();
      duckdb::dtime_t a_time = time.GetValueUnsafe<duckdb::dtime_t>();
      duckdb::timestamp_t result;
      if (duckdb::Timestamp::TryFromDatetime(a_date, a_time, result)) {
        result = result +
          (offset_hours * duckdb::Interval::MICROS_PER_HOUR +
          offset_minutes * duckdb::Interval::MICROS_PER_MINUTE) * -1;

        sink = move(duckdb::Value::TIMESTAMPTZ(result));
        return true;
      }
    }
  } else if(enif_inspect_binary(env, term, &bin)) {
    // parsing: "2022-10-20 23:59:59.999123"
    duckdb::timestamp_t result;
    bool has_offset = false;
    duckdb::string_t tz;
    if (duckdb::Timestamp::TryConvertTimestampTZ((const char*)bin.data, bin.size, result, has_offset, tz)) {
      sink = move(duckdb::Value::TIMESTAMPTZ(result));
      return true;
    }
  }

  return false;
}

bool nif::term_to_blob(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  ErlNifBinary bin;
  if (enif_inspect_binary(env, term, &bin)) {
    sink = move(duckdb::Value(std::string((const char*)bin.data, bin.size)));
    return true;
  }

  if (enif_inspect_iolist_as_binary(env, term, &bin)) {
    sink = move(duckdb::Value(std::string((const char*)bin.data, bin.size)));
    return true;
  }

  return false;
}

bool nif::term_to_interval(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  ErlNifBinary bin;
  if (enif_inspect_binary(env, term, &bin)) {
    duckdb::interval_t result;
    duckdb::string error_message;
    bool strict = false;
    if (duckdb::Interval::FromCString((const char*)bin.data, bin.size, result, &error_message, strict)) {
      sink = move(duckdb::Value::INTERVAL(result));
      return true;
    }
    return false;
  }

  ErlNifUInt64 an_uint64;
  if (enif_get_uint64(env, term, &an_uint64)) {
    sink = move(duckdb::Value::INTERVAL(duckdb::Interval::FromMicro(an_uint64)));
    return true;
  }

  unsigned long a_ulong_int;
  if(enif_get_ulong(env, term, &a_ulong_int)) {
    sink = move(duckdb::Value::INTERVAL(duckdb::Interval::FromMicro(a_ulong_int)));
    return true;
  }

  return false;
}

bool nif::term_to_hugeint(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink) {
  int arity = 0;
  const ERL_NIF_TERM* tuple;

  if (enif_get_tuple(env, term, &arity, &tuple)) {
    if  (arity != 2)
      return false;

    if (!nif::is_atom(env, tuple[0], "hugeint"))
      return false;

    const ERL_NIF_TERM* higeint_tuple;
    if (!enif_get_tuple(env, tuple[1], &arity, &higeint_tuple) || arity != 2)
      return false;

    ErlNifSInt64 upper;
    if (!enif_get_int64(env, higeint_tuple[0], &upper))
      return false;

    ErlNifUInt64 lower;
    if (!enif_get_uint64(env, higeint_tuple[1], &lower))
      return false;

    duckdb::hugeint_t hugeint = 0;
    hugeint.lower = lower;
    hugeint.upper = upper;

    duckdb::hugeint_t result;
    if (duckdb::Hugeint::TryConvert(hugeint, result)) {
      sink = move(duckdb::Value::HUGEINT(result));
      return true;
    }
    return false;

  } else {
    duckdb::hugeint_t result;

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
}

bool nif::term_to_list(ErlNifEnv* env, ERL_NIF_TERM term, const duckdb::LogicalType& list_type, duckdb::Value& sink) {
  duckdb::LogicalType child_type = duckdb::ListType::GetChildType(list_type);

  unsigned list_length = 0;
  if (!enif_get_list_length(env, term, &list_length))
    return false;

  if (list_length == 0) {
    sink = move(duckdb::Value::EMPTYLIST(child_type));
    return true;
  }

  duckdb::vector<duckdb::Value> values;

  ERL_NIF_TERM list = term;
  for (size_t i = 0; i < list_length; i++) {
    ERL_NIF_TERM head, tail;
    if (!enif_get_list_cell(env, list, &head, &tail))
      return false;

    duckdb::Value child;
    if (!nif::term_to_value(env, head, child_type, child))
      return false;

    values.push_back(move(child));

    list = tail;
  }

  sink = move(duckdb::Value::LIST(child_type, values));

  return true;
}

bool nif::term_to_map(ErlNifEnv* env, ERL_NIF_TERM term, const duckdb::LogicalType& map_type, duckdb::Value& sink) {
  size_t map_size = 0;
  if (!enif_get_map_size(env, term, &map_size))
    return false;

  // auto &children = duckdb::StructType::GetChildTypes(map_type);
  // duckdb::LogicalType key_type = duckdb::ListType::GetChildType(children[0].second);
  // duckdb::LogicalType value_type = duckdb::ListType::GetChildType(children[1].second);

  auto &key_type = duckdb::MapType::KeyType(map_type);
  auto &value_type = duckdb::MapType::ValueType(map_type);

  duckdb::vector<duckdb::Value> map_values(map_size);

  duckdb::idx_t idx = 0;
  nif::ErlangMapIterator map_iterator(env, term);
  while (map_iterator.valid && map_iterator.next()) {
    duckdb::Value key_sink, value_sink;

    if (!term_to_value(env, map_iterator.key, key_type, key_sink) ||
        !term_to_value(env, map_iterator.value, value_type, value_sink))
      return false;

    duckdb::child_list_t<duckdb::Value> map_struct(2);
    map_struct[0] = move(make_pair("key", move(key_sink)));
    map_struct[1] = move(make_pair("value", move(value_sink)));

    map_values[idx++] = move(duckdb::Value::STRUCT(move(map_struct)));
  }

  sink = move(duckdb::Value::MAP(duckdb::ListType::GetChildType(map_type), map_values));

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

bool nif::term_to_value(ErlNifEnv* env, ERL_NIF_TERM term, const duckdb::LogicalType& value_type, duckdb::Value& sink) {
  switch(value_type.id()) {
    case duckdb::LogicalTypeId::BIGINT:
      return term_to_bigint(env, term, sink);

    case duckdb::LogicalTypeId::BOOLEAN:
      return term_to_boolean(env, term, sink);

    case duckdb::LogicalTypeId::DOUBLE:
      return term_to_double(env, term, sink);

    case duckdb::LogicalTypeId::DECIMAL:
      return term_to_decimal(env, term, sink);

    case duckdb::LogicalTypeId::INTEGER:
      return term_to_integer(env, term, sink);

    case duckdb::LogicalTypeId::FLOAT:
      return term_to_float(env, term, sink);

    case duckdb::LogicalTypeId::SMALLINT:
      return term_to_smallint(env, term, sink);

    case duckdb::LogicalTypeId::TINYINT:
      return term_to_tinyint(env, term, sink);

    case duckdb::LogicalTypeId::UBIGINT:
      return term_to_ubigint(env, term, sink);

    case duckdb::LogicalTypeId::UINTEGER:
      return term_to_uinteger(env, term, sink);

    case duckdb::LogicalTypeId::USMALLINT:
      return term_to_usmallint(env, term, sink);

    case duckdb::LogicalTypeId::UTINYINT:
      return term_to_utinyint(env, term, sink);

    case duckdb::LogicalTypeId::VARCHAR:
      return term_to_string(env, term, sink);

    case duckdb::LogicalTypeId::UUID:
      return term_to_uuid(env, term, sink);

    case duckdb::LogicalTypeId::DATE:
      return term_to_date(env, term, sink);

    case duckdb::LogicalTypeId::TIME:
      return term_to_time(env, term, sink);

    case duckdb::LogicalTypeId::TIMESTAMP:
      return term_to_timestamp(env, term, sink);

    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      return term_to_timestamp_tz(env, term, sink);

    case duckdb::LogicalTypeId::BLOB:
      return term_to_blob(env, term, sink);

    case duckdb::LogicalTypeId::INTERVAL:
      return term_to_interval(env, term, sink);

    case duckdb::LogicalTypeId::HUGEINT:
      return term_to_hugeint(env, term, sink);

    case duckdb::LogicalTypeId::ENUM:
      return term_to_string(env, term, sink);

    case duckdb::LogicalTypeId::LIST:
      return term_to_list(env, term, value_type, sink);

    case duckdb::LogicalTypeId::MAP:
      return term_to_map(env, term, value_type, sink);

    case duckdb::LogicalTypeId::STRUCT:
      return term_to_struct(env, term, value_type, sink);

    default:
      return false;
  };
}
