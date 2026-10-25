#include "config.h"
#include "term.h"
#include "term_to_value.h"
#include "value_to_term.h"
#include <cstring>

namespace {
  ERL_NIF_TERM scope_to_term(ErlNifEnv* env, duckdb::SettingScopeTarget scope) {
    switch (scope) {
      case duckdb::SettingScopeTarget::GLOBAL_ONLY:
        return nif::make_atom(env, "global_only");
      case duckdb::SettingScopeTarget::LOCAL_ONLY:
        return nif::make_atom(env, "local_only");
      case duckdb::SettingScopeTarget::GLOBAL_DEFAULT:
        return nif::make_atom(env, "global_default");
      case duckdb::SettingScopeTarget::LOCAL_DEFAULT:
        return nif::make_atom(env, "local_default");
      case duckdb::SettingScopeTarget::INVALID:
      default:
        return nif::make_atom(env, "invalid");
    }
  }

  ERL_NIF_TERM default_value_to_term(ErlNifEnv* env, const duckdb::ConfigurationOption& option) {
    if (!option.default_value)
      return nif::make_atom(env, "nil");

    try {
      auto value_type = duckdb::DBConfig::ParseLogicalType(option.parameter_type);
      auto value = duckdb::Value(option.default_value).DefaultCastAs(value_type, false);

      ERL_NIF_TERM term;
      if (nif::value_to_term(env, value, term))
        return term;
    } catch (...) {
    }

    return nif::make_binary_term(env, option.default_value, std::strlen(option.default_value));
  }
}

bool nif::term_to_option_value(ErlNifEnv* env, ERL_NIF_TERM term, const duckdb::ConfigurationOption& option, duckdb::Value& sink) {
  auto value_type = duckdb::DBConfig::ParseLogicalType(option.parameter_type);
  return nif::term_to_value(env, term, value_type, sink);
}

bool nif::term_to_option_value(ErlNifEnv* env, ERL_NIF_TERM term, const duckdb::ExtensionOption& option, duckdb::Value& sink) {
  return nif::term_to_value(env, term, option.type, sink);
}

ERL_NIF_TERM nif::config_option_to_term(ErlNifEnv* env, const duckdb::ConfigurationOption& option) {
  auto parameter_type = duckdb::DBConfig::ParseLogicalType(option.parameter_type);

  ERL_NIF_TERM map = enif_make_new_map(env);

  enif_make_map_put(env, map, nif::make_atom(env, "name"), nif::make_binary_term(env, option.name, std::strlen(option.name)), &map);
  enif_make_map_put(env, map, nif::make_atom(env, "type"), nif::logical_type_to_term(env, parameter_type), &map);
  enif_make_map_put(env, map, nif::make_atom(env, "parameter_type"), nif::make_binary_term(env, option.parameter_type, std::strlen(option.parameter_type)), &map);
  enif_make_map_put(env, map, nif::make_atom(env, "description"), nif::make_binary_term(env, option.description, std::strlen(option.description)), &map);
  enif_make_map_put(env, map, nif::make_atom(env, "scope"), scope_to_term(env, option.scope), &map);
  enif_make_map_put(env, map, nif::make_atom(env, "default"), default_value_to_term(env, option), &map);

  return map;
}
