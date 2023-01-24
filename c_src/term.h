#pragma once
#include <erl_nif.h>
#include <string>

namespace duckdb {
  class Value;
}

namespace nif {
  ERL_NIF_TERM
  make_atom(ErlNifEnv* env, const char* atom_name);

  ERL_NIF_TERM
  make_atom(ErlNifEnv* env, const std::string&);

  ERL_NIF_TERM
  make_ok_tuple(ErlNifEnv* env, ERL_NIF_TERM value);

  ERL_NIF_TERM
  make_error_tuple(ErlNifEnv* env, ERL_NIF_TERM value);

  ERL_NIF_TERM
  make_error_tuple(ErlNifEnv* env, const char* cstr, size_t len);

  ERL_NIF_TERM
  make_error_tuple(ErlNifEnv* env, const std::string&);

  ERL_NIF_TERM
  make_binary_term(ErlNifEnv* env, const char* cstr, size_t len);

  ERL_NIF_TERM
  make_binary_term(ErlNifEnv* env, const std::string&);

  bool
  is_atom(ErlNifEnv* env, ERL_NIF_TERM term, const char* expected_atom);
}
