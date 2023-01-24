#pragma once
#include <erl_nif.h>

namespace duckdb {
  class Value;
  class LogicalType;
}

namespace nif {
  bool term_to_float(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink);
  bool term_to_double(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink);
  bool term_to_integer(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink);
  bool term_to_smallint(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink);
  bool term_to_tinyint(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink);
  bool term_to_bigint(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink);
  bool term_to_uinteger(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink);
  bool term_to_usmallint(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink);
  bool term_to_utinyint(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink);
  bool term_to_ubigint(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink);
  bool term_to_boolean(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink);
  bool term_to_string(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink);
  bool term_to_decimal(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink);
  bool term_to_uuid(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink);
  bool term_to_date(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink);
  bool term_to_time(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink);
  bool term_to_timestamp(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink);
  bool term_to_timestamp_tz(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink);
  bool term_to_blob(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink);
  bool term_to_interval(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink);
  bool term_to_hugeint(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::Value& sink);
  bool term_to_list(ErlNifEnv* env, ERL_NIF_TERM term, const duckdb::LogicalType& child_type, duckdb::Value& sink);
  bool term_to_map(ErlNifEnv* env, ERL_NIF_TERM term, const duckdb::LogicalType& map_type, duckdb::Value& sink);
  bool term_to_struct(ErlNifEnv* env, ERL_NIF_TERM term, const duckdb::LogicalType& struct_type, duckdb::Value& sink);

  bool term_to_value(ErlNifEnv* env, ERL_NIF_TERM term, const duckdb::LogicalType& value_type, duckdb::Value& sink);
}
