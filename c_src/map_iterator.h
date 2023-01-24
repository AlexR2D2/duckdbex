#pragma once
#include <erl_nif.h>

/*
A map iterator is only useful during the lifetime of environment env that the map belongs to
*/
namespace nif {
  struct ErlangMapIterator {
    ErlangMapIterator(ErlNifEnv* map_env, ERL_NIF_TERM map):
      valid(false),
      env(map_env) {
      valid = enif_map_iterator_create(env, map, &iterator, ERL_NIF_MAP_ITERATOR_FIRST);
    }

    virtual ~ErlangMapIterator() {
      enif_map_iterator_destroy(env, &iterator);
      env = 0;
      valid = false;
    }

    bool next() {
      bool pair_valid = enif_map_iterator_get_pair(env, &iterator, &key, &value);
      enif_map_iterator_next(env, &iterator);
      return pair_valid;
    }

    ErlNifEnv* env;
    ErlNifMapIterator iterator;
    bool valid;
    ERL_NIF_TERM key;
    ERL_NIF_TERM value;
  };
}
