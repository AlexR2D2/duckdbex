#pragma once
#include <erl_nif.h>

namespace duckdb {
  struct DBConfig;
}

namespace nif {
  bool get_config(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink);
}
