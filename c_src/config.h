#pragma once
#include "duckdb.hpp"
#include <erl_nif.h>
#include <string>

namespace nif {
  bool term_to_option_value(ErlNifEnv* env, ERL_NIF_TERM term, const duckdb::ConfigurationOption& option, duckdb::Value& sink);
  bool term_to_option_value(ErlNifEnv* env, ERL_NIF_TERM term, const duckdb::ExtensionOption& option, duckdb::Value& sink);
  ERL_NIF_TERM config_option_to_term(ErlNifEnv* env, const duckdb::ConfigurationOption& option);
}
