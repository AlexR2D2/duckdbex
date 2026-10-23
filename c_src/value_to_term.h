#pragma once
#include <erl_nif.h>

namespace duckdb {
  class LogicalType;
  class Value;
}

namespace nif {
  ERL_NIF_TERM logical_type_to_term(ErlNifEnv* env, const duckdb::LogicalType& type);
  bool value_to_term(ErlNifEnv* env, const duckdb::Value& value, ERL_NIF_TERM& sink);
  bool value_to_term(ErlNifEnv* env, const duckdb::Value& value, ERL_NIF_TERM& sink, bool raw);
}
