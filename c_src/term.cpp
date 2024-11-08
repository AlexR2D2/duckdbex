#include "term.h"
#include "duckdb.hpp"
#include <erl_nif.h>

const int64_t POWERS_OF_TEN[] {1,
                              10,
                              100,
                              1000,
                              10000,
                              100000,
                              1000000,
                              10000000,
                              100000000,
                              1000000000,
                              10000000000,
                              100000000000,
                              1000000000000,
                              10000000000000,
                              100000000000000,
                              1000000000000000,
                              10000000000000000,
                              100000000000000000,
                              1000000000000000000};

template <class SIGNED, class UNSIGNED>
void get_decimal_major_minor(SIGNED value, uint8_t scale, UNSIGNED& major, UNSIGNED& minor) {
  major = value / (UNSIGNED)POWERS_OF_TEN[scale];
  minor = value % (UNSIGNED)POWERS_OF_TEN[scale];
}

ERL_NIF_TERM
nif::make_atom(ErlNifEnv* env, const char* atom_name) {
  assert(env);
  assert(atom_name);

  ERL_NIF_TERM atom;

  if (enif_make_existing_atom(env, atom_name, &atom, ERL_NIF_LATIN1)) {
    return atom;
  }

  return enif_make_atom(env, atom_name);
}

ERL_NIF_TERM
nif::make_atom(ErlNifEnv* env, const std::string& atom_name) {
  assert(env);
  assert(atom_name);

  ERL_NIF_TERM atom;

  if (enif_make_existing_atom(env, atom_name.c_str(), &atom, ERL_NIF_LATIN1)) {
    return atom;
  }

  return enif_make_atom(env, atom_name.c_str());
}

ERL_NIF_TERM
nif::make_ok_tuple(ErlNifEnv* env, ERL_NIF_TERM value) {
  assert(env);
  assert(value);

  return enif_make_tuple2(env, nif::make_atom(env, "ok"), value);
}

ERL_NIF_TERM
nif::make_error_tuple(ErlNifEnv* env, ERL_NIF_TERM value) {
  assert(env);
  assert(value);

  return enif_make_tuple2(env, nif::make_atom(env, "error"), value);
}

ERL_NIF_TERM
nif::make_error_tuple(ErlNifEnv* env, const char* cstr, size_t len) {
  return nif::make_error_tuple(env, make_binary_term(env, cstr, len));
}

ERL_NIF_TERM
nif::make_error_tuple(ErlNifEnv* env, const std::string& ctr) {
  return nif::make_error_tuple(env, make_binary_term(env, ctr));
}

ERL_NIF_TERM
nif::make_binary_term(ErlNifEnv* env, const char* cstr, size_t len) {
  assert(env);
  assert(cstr);
  assert(len);

  if(cstr) {
    ERL_NIF_TERM result;
    memcpy(enif_make_new_binary(env, len, &result), cstr, len);
    return result;
  } else {
    return enif_make_atom(env, "nil");
  };
}

ERL_NIF_TERM
nif::make_binary_term(ErlNifEnv* env, const std::string& str) {
  return nif::make_binary_term(env, str.c_str(), str.length());
}

bool nif::is_atom(ErlNifEnv* env, ERL_NIF_TERM term, const char* expected_atom) {
  if (!enif_is_atom(env, term))
    return false;

  unsigned atom_len = 0;
  if (!enif_get_atom_length(env, term, &atom_len, ERL_NIF_LATIN1))
    return false;

  std::vector<char> atom(atom_len + 1);
  if(!enif_get_atom(env, term, &atom[0], atom.size(), ERL_NIF_LATIN1))
    return false;

  return std::strcmp(&atom[0], expected_atom) == 0;
}

bool nif::atom_to_string(ErlNifEnv* env, ERL_NIF_TERM term, std::string& sink) {
  if (!enif_is_atom(env, term))
    return false;

  unsigned atom_len = 0;
  if (!enif_get_atom_length(env, term, &atom_len, ERL_NIF_LATIN1))
    return false;

  std::vector<char> atom(atom_len + 1);
  if(!enif_get_atom(env, term, &atom[0], atom.size(), ERL_NIF_LATIN1))
    return false;

  sink = std::string(&atom[0], atom.size());

  return true;
}
