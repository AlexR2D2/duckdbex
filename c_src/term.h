#pragma once
#include <erl_nif.h>
#include <initializer_list>
#include <string>
#include <utility>

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

  bool
  get_string(ErlNifEnv* env, ERL_NIF_TERM term, std::string&);

  ERL_NIF_TERM
  make_struct(ErlNifEnv* env, const char* module,
              const std::initializer_list<std::pair<const char*, ERL_NIF_TERM>>& fields);

  bool
  struct_is(ErlNifEnv* env, ERL_NIF_TERM term, const char* module);

  bool
  get_struct_field(ErlNifEnv* env, ERL_NIF_TERM term, const char* field, ERL_NIF_TERM& sink);

  bool
  atom_to_string(ErlNifEnv* env, ERL_NIF_TERM term, std::string&);
}
