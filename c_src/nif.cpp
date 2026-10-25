#include "config.h"
#include "map_iterator.h"
#include "resource.h"
#include "term.h"
#include "term_to_value.h"
#include "value_to_term.h"
#include "duckdb.hpp"
#include <erl_nif.h>
#include <exception>
#include <string>

/*
 * DuckDB API
 */

static ERL_NIF_TERM
number_of_threads(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  if (argc != 1)
    return enif_make_badarg(env);

  auto dbres = get_resource<duckdb::DuckDB>(env, argv[0]);
  if (!dbres)
    return enif_make_badarg(env);

  return enif_make_uint64(env, dbres->data->NumberOfThreads());
}

static ERL_NIF_TERM
extension_is_loaded(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  if (argc != 2)
    return enif_make_badarg(env);

  auto dbres = get_resource<duckdb::DuckDB>(env, argv[0]);
  if (!dbres)
    return enif_make_badarg(env);

  ErlNifBinary extension;
  if (!enif_inspect_binary(env, argv[1], &extension))
    return enif_make_badarg(env);

  return dbres->data->ExtensionIsLoaded(std::string((const char*)extension.data, extension.size))
    ? nif::make_atom(env, "true")
    : nif::make_atom(env, "false");
}

static ERL_NIF_TERM
source_id(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  static const char* source_id = duckdb::DuckDB::SourceID();
  return nif::make_binary_term(env, source_id, std::strlen(source_id));
}

static ERL_NIF_TERM
library_version(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  static const char* lib_vsn = duckdb::DuckDB::LibraryVersion();
  return nif::make_binary_term(env, lib_vsn, std::strlen(lib_vsn));
}

static ERL_NIF_TERM
library_version_of_storage(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  if (argc != 1)
    return enif_make_badarg(env);

  ErlNifUInt64 storage_format_version;
  if (!enif_get_uint64(env, argv[0], &storage_format_version))
    return enif_make_badarg(env);

  return nif::make_binary_term(env, duckdb::GetDuckDBVersions(static_cast<duckdb::StorageVersion>(storage_format_version)));
}

static ERL_NIF_TERM
storage_format_version(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  return enif_make_uint64(env, duckdb::VERSION_NUMBER);
}

static ERL_NIF_TERM
platform(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  return nif::make_binary_term(env, duckdb::DuckDB::Platform());
}

static ERL_NIF_TERM
create_config(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  if (argc != 0)
    return enif_make_badarg(env);

  try {
    ErlangResourceBuilder<duckdb::DBConfig> resource_builder(config_nif_type);
    return nif::make_ok_tuple(env, resource_builder.make_and_release_resource(env));
  } catch (std::exception& ex) {
    return nif::make_error_tuple(env, ex.what());
  }
}

static ERL_NIF_TERM
set_config_option(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  if (argc != 3)
    return enif_make_badarg(env);

  auto configres = get_resource<duckdb::DBConfig>(env, argv[0]);
  if (!configres)
    return enif_make_badarg(env);

  std::string option_name;
  if (!nif::get_string(env, argv[1], option_name))
    return enif_make_badarg(env);

  duckdb::Value value;

  if (auto option = duckdb::DBConfig::GetOptionByName(duckdb::Identifier(option_name))) {
    if (!nif::term_to_option_value(env, argv[2], *option, value))
      return nif::make_error_tuple(env, "invalid value for option \"" + option_name + "\"");

    try {
      configres->data->SetOption(*option, value);
      return nif::make_atom(env, "ok");
    } catch (std::exception& ex) {
      return nif::make_error_tuple(env, ex.what());
    }
  } else {
    duckdb::ExtensionOption extension_option;
    if (!configres->data->TryGetExtensionOption(duckdb::Identifier(option_name), extension_option))
      return nif::make_error_tuple(env, "unknown config option \"" + option_name + "\"");

    if (!nif::term_to_option_value(env, argv[2], extension_option, value))
      return nif::make_error_tuple(env, "invalid value for option \"" + option_name + "\"");

    try {
      configres->data->SetOption(extension_option.setting_index.GetIndex(), std::move(value));
      return nif::make_atom(env, "ok");
    } catch (std::exception& ex) {
      return nif::make_error_tuple(env, ex.what());
    }
  }
}

static ERL_NIF_TERM
get_config_options(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  if (argc != 0)
    return enif_make_badarg(env);

  auto options = duckdb::DBConfig::GetOptions();
  std::vector<ERL_NIF_TERM> terms(options.size());

  for (duckdb::idx_t idx = 0; idx < options.size(); idx++) {
    terms[idx] = nif::config_option_to_term(env, options[idx]);
  }

  if (terms.empty())
    return enif_make_list(env, 0);

  return enif_make_list_from_array(env, &terms[0], terms.size());
}

static ERL_NIF_TERM
open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  std::string path;
  duckdb::DBConfig* config = nullptr;

  if (argc != 2)
    return enif_make_badarg(env);

  ErlNifBinary arg;
  if (!enif_inspect_binary(env, argv[0], &arg))
    return enif_make_badarg(env);

  path = std::string((const char*)arg.data, arg.size);

  if (!nif::is_atom(env, argv[1], "nil")) {
    auto configres = get_resource<duckdb::DBConfig>(env, argv[1]);
    if (!configres)
      return enif_make_badarg(env);
    config = configres->data.get();
  }

  try {
    ErlangResourceBuilder<duckdb::DuckDB> resource_builder(database_nif_type, path, config);
    return nif::make_ok_tuple(env, resource_builder.make_and_release_resource(env));

  } catch (std::exception& ex) {
    return nif::make_error_tuple(env, ex.what());
  }
}

static ERL_NIF_TERM
connection(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  auto dbres = get_resource<duckdb::DuckDB>(env, argv[0]);
  if (!dbres)
    return enif_make_badarg(env);

  ErlangResourceBuilder<duckdb::Connection> resource_builder(connection_nif_type, *dbres->data);

  return nif::make_ok_tuple(env, resource_builder.make_and_release_resource(env));
}

static ERL_NIF_TERM
query_without_parameters(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  if (argc != 2)
    return enif_make_badarg(env);

  auto connres = get_resource<duckdb::Connection>(env, argv[0]);
  if (!connres)
    return enif_make_badarg(env);

  ErlNifBinary sql_stmt;
  if (!enif_inspect_binary(env, argv[1], &sql_stmt))
    return enif_make_badarg(env);

  duckdb::unique_ptr<duckdb::QueryResult> result = connres->data->Query(std::string((const char*)sql_stmt.data, sql_stmt.size));

  if (result->HasError())
    return nif::make_error_tuple(env, result->GetErrorObject().Message());

  ErlangResourceBuilder<duckdb::QueryResult> resource_builder(
    query_result_nif_type,
    std::move(result));

  return nif::make_ok_tuple(env, resource_builder.make_and_release_resource(env));
}

enum class NamedBindResult { OK, INVALID_NAME, UNKNOWN_NAME, INVALID_VALUE };

static NamedBindResult
bind_named_values(ErlNifEnv* env, ERL_NIF_TERM term,
                  const duckdb::case_insensitive_map_t<duckdb::LogicalType>& param_types,
                  duckdb::identifier_map_t<duckdb::BoundParameterData>& sink,
                  std::string& failing_name) {
  nif::ErlangMapIterator iter(env, term);
  if (!iter.valid)
    return NamedBindResult::INVALID_NAME;

  while (iter.next()) {
    failing_name.clear();

    if (!nif::get_string(env, iter.key, failing_name))
      return NamedBindResult::INVALID_NAME;

    auto type_it = param_types.find(failing_name);
    if (type_it == param_types.end())
      return NamedBindResult::UNKNOWN_NAME;

    duckdb::Value bound;
    if (!nif::term_to_value(env, iter.value, type_it->second, bound))
      return NamedBindResult::INVALID_VALUE;

    sink.emplace(duckdb::Identifier(failing_name), duckdb::BoundParameterData(std::move(bound)));
  }

  return NamedBindResult::OK;
}

static ERL_NIF_TERM
named_bind_error(ErlNifEnv* env, NamedBindResult result, const std::string& name) {
  switch (result) {
    case NamedBindResult::UNKNOWN_NAME:
      return nif::make_error_tuple(env, "unknown named parameter \"" + name + "\"");
    case NamedBindResult::INVALID_VALUE:
      return nif::make_error_tuple(env, "invalid value for named parameter \"" + name + "\"");
    default:
      return nif::make_error_tuple(env, "invalid named parameter name");
  }
}

static bool
bind_positional_values(ErlNifEnv* env, ERL_NIF_TERM term,
                       duckdb::case_insensitive_map_t<duckdb::LogicalType>& param_types,
                       duckdb::vector<duckdb::Value>& sink,
                       std::string& error_message) {
  ERL_NIF_TERM item, items = term;
  int arg_idx = 0;

  while (enif_get_list_cell(env, items, &item, &items)) {
    duckdb::Value value;
    auto arg_idx_str = std::to_string(arg_idx + 1);
    if (!nif::term_to_value(env, item, param_types[arg_idx_str], value)) {
      error_message = "invalid type of parameter #" + arg_idx_str;
      return false;
    }
    sink.push_back(std::move(value));
    arg_idx++;
  }

  return true;
}

//
// This is trying to execute multiple statements sql with parameters
// looks like this is not possible yet.
// returns 'Cannot prepare multiple statements at once'
//
static ERL_NIF_TERM
query_with_parameters(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  auto connres = get_resource<duckdb::Connection>(env, argv[0]);
  if (!connres)
    return enif_make_badarg(env);

  ErlNifBinary sql_stmt;
  if (!enif_inspect_binary(env, argv[1], &sql_stmt))
    return enif_make_badarg(env);

  auto statement = connres->data->Prepare(std::string((const char*)sql_stmt.data, sql_stmt.size));
  if (statement->HasError())
    return nif::make_error_tuple(env, statement->GetErrorObject().Message());

  duckdb::vector<duckdb::Value> query_params;
  duckdb::identifier_map_t<duckdb::BoundParameterData> named_values;
  duckdb::case_insensitive_map_t<duckdb::LogicalType> params_types = statement->GetExpectedParameterTypes();

  duckdb::unique_ptr<duckdb::QueryResult> result;

  if (argc == 3 && enif_is_map(env, argv[2])) {
    std::string failing_name;
    auto named_result = bind_named_values(env, argv[2], params_types, named_values, failing_name);
    if (named_result != NamedBindResult::OK)
      return named_bind_error(env, named_result, failing_name);

    result = statement->Execute(named_values);
  } else {
    if (params_types.size()) {
      if (argc != 3)
        return enif_make_badarg(env);

      if (!enif_is_list(env, argv[2]))
        return enif_make_badarg(env);

      std::string error_message;
      if (!bind_positional_values(env, argv[2], params_types, query_params, error_message))
        return nif::make_error_tuple(env, error_message);
    }

    result = statement->Execute(query_params);
  }

  if (result->HasError())
    return nif::make_error_tuple(env, result->GetErrorObject().Message());

  ErlangResourceBuilder<duckdb::QueryResult> resource_builder(
    query_result_nif_type,
    std::move(result));

  return nif::make_ok_tuple(env, resource_builder.make_and_release_resource(env));
}

static ERL_NIF_TERM
prepare_statement(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  auto connres = get_resource<duckdb::Connection>(env, argv[0]);
  if (!connres)
    return enif_make_badarg(env);

  ErlNifBinary sql_stmt;
  if (!enif_inspect_binary(env, argv[1], &sql_stmt))
    return enif_make_badarg(env);

  auto statement = connres->data->Prepare(std::string((const char*)sql_stmt.data, sql_stmt.size));
  if (statement->HasError())
    return nif::make_error_tuple(env, statement->GetErrorObject().Message());

  ErlangResourceBuilder<duckdb::PreparedStatement> resource_builder(
    prepared_statement_nif_type,
    std::move(statement));

  return nif::make_ok_tuple(env, resource_builder.make_and_release_resource(env));
}

static ERL_NIF_TERM
execute_statement(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  auto stmtres = get_resource<duckdb::PreparedStatement>(env, argv[0]);
  if (!stmtres)
    return enif_make_badarg(env);

  duckdb::vector<duckdb::Value> query_params;
  duckdb::identifier_map_t<duckdb::BoundParameterData> named_values;
  duckdb::case_insensitive_map_t<duckdb::LogicalType> params_types = stmtres->data->GetExpectedParameterTypes();

  duckdb::unique_ptr<duckdb::QueryResult> result;

  if (argc == 2 && enif_is_map(env, argv[1])) {
    std::string failing_name;
    auto named_result = bind_named_values(env, argv[1], params_types, named_values, failing_name);
    if (named_result != NamedBindResult::OK)
      return named_bind_error(env, named_result, failing_name);

    result = stmtres->data->Execute(named_values);
  } else {
    if (params_types.size()) {
      if (argc != 2)
        return enif_make_badarg(env);

      if (!enif_is_list(env, argv[1]))
        return enif_make_badarg(env);

      std::string error_message;
      if (!bind_positional_values(env, argv[1], params_types, query_params, error_message))
        return nif::make_error_tuple(env, error_message);
    }

    result = stmtres->data->Execute(query_params);
  }

  if (result->HasError())
    return nif::make_error_tuple(env, result->GetErrorObject().Message());

  ErlangResourceBuilder<duckdb::QueryResult> resource_builder(
    query_result_nif_type,
    std::move(result));

  return nif::make_ok_tuple(env, resource_builder.make_and_release_resource(env));
}

static ERL_NIF_TERM
get_named_parameters(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  if (argc != 1)
    return enif_make_badarg(env);

  auto stmtres = get_resource<duckdb::PreparedStatement>(env, argv[0]);
  if (!stmtres)
    return enif_make_badarg(env);

  auto& named = stmtres->data->GetNamedParameterMap();
  auto types = stmtres->data->GetExpectedParameterTypes();

  std::vector<ERL_NIF_TERM> params;
  params.reserve(named.size());

  for (auto& entry : named) {
    auto& name = entry.first.GetIdentifierName();

    // positional parameters show up in the named map as "1", "2", ...
    if (!name.empty() && name.find_first_not_of("0123456789") == std::string::npos)
      continue;

    auto type_it = types.find(name);

    ERL_NIF_TERM type_term = type_it == types.end()
      ? nif::make_atom(env, "unknown")
      : nif::logical_type_to_term(env, type_it->second);

    params.push_back(enif_make_tuple2(env,
      nif::make_binary_term(env, name),
      type_term));
  }

  if (params.empty())
    return enif_make_list(env, 0);

  return enif_make_list_from_array(env, params.data(), params.size());
}

static ERL_NIF_TERM
begin_transaction(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  if (argc != 1)
    return enif_make_badarg(env);

  auto connres = get_resource<duckdb::Connection>(env, argv[0]);
  if (!connres)
    return enif_make_badarg(env);

  duckdb::unique_ptr<duckdb::QueryResult> result = connres->data->Query("BEGIN TRANSACTION");
  if (result->HasError())
    return nif::make_error_tuple(env, result->GetErrorObject().Message());

  return nif::make_atom(env, "ok");
}

static ERL_NIF_TERM
commit(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  if (argc != 1)
    return enif_make_badarg(env);

  auto connres = get_resource<duckdb::Connection>(env, argv[0]);
  if (!connres)
    return enif_make_badarg(env);

  duckdb::unique_ptr<duckdb::QueryResult> result = connres->data->Query("COMMIT");
  if (result->HasError())
    return nif::make_error_tuple(env, result->GetErrorObject().Message());

  return nif::make_atom(env, "ok");
}

static ERL_NIF_TERM
rollback(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  if (argc != 1)
    return enif_make_badarg(env);

  auto connres = get_resource<duckdb::Connection>(env, argv[0]);
  if (!connres)
    return enif_make_badarg(env);

  duckdb::unique_ptr<duckdb::QueryResult> result = connres->data->Query("ROLLBACK");
  if (result->HasError())
    return nif::make_error_tuple(env, result->GetErrorObject().Message());

  return nif::make_atom(env, "ok");
}

static ERL_NIF_TERM
set_auto_commit(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  if (argc != 2)
    return enif_make_badarg(env);

  auto connres = get_resource<duckdb::Connection>(env, argv[0]);
  if (!connres)
    return enif_make_badarg(env);

  duckdb::Value boolean;
  if (!nif::term_to_boolean(env, argv[1], boolean))
    return enif_make_badarg(env);

  connres->data->SetAutoCommit(duckdb::BooleanValue::Get(boolean));

  return nif::make_atom(env, "ok");
}

static ERL_NIF_TERM
is_auto_commit(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  if (argc != 1)
    return enif_make_badarg(env);

  auto connres = get_resource<duckdb::Connection>(env, argv[0]);
  if (!connres)
    return enif_make_badarg(env);

  return nif::make_ok_tuple(env, nif::make_atom(env, connres->data->IsAutoCommit() ? "true" : "false"));
}

static ERL_NIF_TERM
has_active_transaction(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  if (argc != 1)
    return enif_make_badarg(env);

  auto connres = get_resource<duckdb::Connection>(env, argv[0]);
  if (!connres)
    return enif_make_badarg(env);

  return nif::make_ok_tuple(env, nif::make_atom(env, connres->data->HasActiveTransaction() ? "true" : "false"));
}

static ERL_NIF_TERM
columns(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  if (argc != 1)
    return enif_make_badarg(env);

  auto result = get_resource<duckdb::QueryResult>(env, argv[0]);
  if (!result)
    return enif_make_badarg(env);

  if (result->data->HasError()) {
    auto error = result->data->GetError();
    return nif::make_error_tuple(env, error);
  }

  if (duckdb::idx_t columns_count = result->data->ColumnCount()) {
    std::vector<ERL_NIF_TERM> columns(columns_count);
    for (duckdb::idx_t col = 0; col < columns_count; col++) {
      const duckdb::string &column_name = result->data->ColumnName(col).GetIdentifierName();
      columns[col] = nif::make_binary_term(env, column_name);
    }
    return enif_make_list_from_array(env, &columns[0], columns.size());
  } else {
    return enif_make_list(env, 0);
  }
}

static ERL_NIF_TERM
fetch_chunk(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  if (argc != 1)
    return enif_make_badarg(env);

  auto result = get_resource<duckdb::QueryResult>(env, argv[0]);
  if (!result)
    return enif_make_badarg(env);

  if (result->data->HasError()) {
    auto error = result->data->GetError();
    return nif::make_error_tuple(env, error);
  }

  std::vector<ERL_NIF_TERM> rows;

  duckdb::unique_ptr<duckdb::DataChunk> chunk;
  duckdb::ErrorData error;
  if (result->data->TryFetchOrError(chunk, error) && chunk) {
    duckdb::idx_t rows_count = chunk->size();
    duckdb::idx_t columns_count = chunk->ColumnCount();

    for (duckdb::idx_t row = 0; row < rows_count; row++) {
      std::vector<ERL_NIF_TERM> columns(columns_count);
      for (duckdb::idx_t col = 0; col < columns_count; col++) {
        auto value = chunk->GetValue(col, row);
        ERL_NIF_TERM sink;
        if (!nif::value_to_term(env, value, sink))
          return nif::make_error_tuple(env, "Can't convert DuckDB value of type '" + value.type().ToString() + "' to the Erlang term.");
        columns[col] = sink;
      }

      rows.push_back(enif_make_list_from_array(env, &columns[0], columns_count));
    }

    return enif_make_list_from_array(env, &rows[0], rows.size());
  } else {
    return enif_make_list(env, 0);
  }
}

static ERL_NIF_TERM
fetch_all(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  if (argc != 1)
    return enif_make_badarg(env);

  auto result = get_resource<duckdb::QueryResult>(env, argv[0]);
  if (!result)
    return enif_make_badarg(env);

  if (result->data->HasError()) {
    auto error = result->data->GetError();
    return nif::make_error_tuple(env, error);
  }

  std::vector<ERL_NIF_TERM> rows;

  duckdb::unique_ptr<duckdb::DataChunk> chunk;
  duckdb::ErrorData error;
  while (result->data->TryFetchOrError(chunk, error) && chunk) {
    duckdb::idx_t rows_count = chunk->size();
    duckdb::idx_t columns_count = chunk->ColumnCount();
    for (duckdb::idx_t row = 0; row < rows_count; row++) {
      std::vector<ERL_NIF_TERM> columns(columns_count);
      for (duckdb::idx_t col = 0; col < columns_count; col++) {
        auto value = chunk->GetValue(col, row);
        ERL_NIF_TERM sink;
        if (!nif::value_to_term(env, value, sink))
          return nif::make_error_tuple(env, "Can't convert DuckDB value of type '" + value.type().ToString() + "' to the Erlang term.");
        columns[col] = sink;
      }

      rows.push_back(enif_make_list_from_array(env, &columns[0], columns_count));
    }
  }

  if (rows.size())
    return enif_make_list_from_array(env, &rows[0], rows.size());
  else
    return enif_make_list(env, 0);
}

static ERL_NIF_TERM
appender(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  if (argc < 2 || argc > 3) {
    return enif_make_badarg(env);
  }

  auto connres = get_resource<duckdb::Connection>(env, argv[0]);
  if (!connres)
    return enif_make_badarg(env);

  ErlNifBinary binary_table_name;
  if (argc == 2) {
    // Only table name is provided
    if (!enif_inspect_binary(env, argv[1], &binary_table_name)) {
      return enif_make_badarg(env);
    }

    std::string table_name((const char*)binary_table_name.data, binary_table_name.size);

    if (!connres->data->TableInfo(duckdb::Identifier(table_name)))
      return nif::make_error_tuple(env, "Table '" + table_name + "' could not be found");

    // Construct first so a throwing Appender constructor cannot leak an Erlang resource.
    ErlangResourceBuilder<duckdb::Appender> resource_builder(
      appender_nif_type, duckdb::make_uniq<duckdb::Appender>(*connres->data, duckdb::Identifier(table_name)));
    return nif::make_ok_tuple(env, resource_builder.make_and_release_resource(env));
  } else {
    ErlNifBinary binary_schema_name;

    // Schema name and table name are provided
    if (!enif_inspect_binary(env, argv[1], &binary_schema_name)) {
      return enif_make_badarg(env);
    }
    if (!enif_inspect_binary(env, argv[2], &binary_table_name)) {
      return enif_make_badarg(env);
    }

    std::string schema_name((const char*)binary_schema_name.data, binary_schema_name.size);
    std::string table_name((const char*)binary_table_name.data, binary_table_name.size);

    if (!connres->data->TableInfo(duckdb::Identifier(schema_name), duckdb::Identifier(table_name))) {
      return nif::make_error_tuple(env, "Table '" + schema_name + "." + table_name + "' could not be found");
    }

    // Construct first so a throwing Appender constructor cannot leak an Erlang resource.
    ErlangResourceBuilder<duckdb::Appender> resource_builder(
      appender_nif_type, duckdb::make_uniq<duckdb::Appender>(*connres->data, duckdb::Identifier(schema_name), duckdb::Identifier(table_name)));
    return nif::make_ok_tuple(env, resource_builder.make_and_release_resource(env));
  }
}

static ERL_NIF_TERM
appender_add_row(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  if (argc != 2)
    return enif_make_badarg(env);

  auto apres = get_resource<duckdb::Appender>(env, argv[0]);
  if (!apres)
    return enif_make_badarg(env);

  if (!enif_is_list(env, argv[1]))
    return enif_make_badarg(env);

  const duckdb::vector<duckdb::LogicalType>& types = apres->data->GetActiveTypes();

  unsigned row_size = 0;
  if(!enif_get_list_length(env, argv[1], &row_size) || row_size != types.size())
    return enif_make_badarg(env);

  apres->data->BeginRow();

  ERL_NIF_TERM item, items;
  items = argv[1];
  int column_idx = 0;
  while(enif_get_list_cell(env, items, &item, &items)) {
    duckdb::Value value;
    if (!nif::term_to_value(env, item, types[column_idx], value))
      return nif::make_error_tuple(env, "invalid type of column: " + std::to_string(column_idx));

    apres->data->Append(value);
    column_idx++;
  }

  apres->data->EndRow();

  return nif::make_atom(env, "ok");
}

static ERL_NIF_TERM
appender_add_rows(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  if (argc != 2)
    return enif_make_badarg(env);

  auto apres = get_resource<duckdb::Appender>(env, argv[0]);
  if (!apres)
    return enif_make_badarg(env);

  if (!enif_is_list(env, argv[1]))
    return enif_make_badarg(env);

  const duckdb::vector<duckdb::LogicalType>& types = apres->data->GetActiveTypes();

  ERL_NIF_TERM item, row, rows;
  rows = argv[1];
  while(enif_get_list_cell(env, rows, &row, &rows)) {
    if (!enif_is_list(env, row))
      return enif_make_badarg(env);

    unsigned row_size = 0;
    if(!enif_get_list_length(env, row, &row_size) || row_size != types.size())
      return enif_make_badarg(env);

    apres->data->BeginRow();
    int column_idx = 0;
    while(enif_get_list_cell(env, row, &item, &row)) {
      duckdb::Value value;
      if (!nif::term_to_value(env, item, types[column_idx], value))
        return nif::make_error_tuple(env, "invalid type of column: " + std::to_string(column_idx));

      apres->data->Append(value);
      column_idx++;
    }
    apres->data->EndRow();
  }

  return nif::make_atom(env, "ok");
}

static ERL_NIF_TERM
appender_flush(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  if (argc != 1)
    return enif_make_badarg(env);

  auto apres = get_resource<duckdb::Appender>(env, argv[0]);
  if (!apres)
    return enif_make_badarg(env);

  apres->data->Flush();

  return nif::make_atom(env, "ok");
}

static ERL_NIF_TERM
appender_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  if (argc != 1)
    return enif_make_badarg(env);

  auto apres = get_resource<duckdb::Appender>(env, argv[0]);
  if (!apres)
    return enif_make_badarg(env);

  apres->data->Close();
  apres->data = nullptr;

  return nif::make_atom(env, "ok");
}

static ERL_NIF_TERM
release(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  if (argc != 1)
    return enif_make_badarg(env);

  if (auto res = get_resource<duckdb::Appender>(env, argv[0]))
    res->data = nullptr;

  if (auto res = get_resource<duckdb::PreparedStatement>(env, argv[0]))
    res->data = nullptr;

  if (auto res = get_resource<duckdb::QueryResult>(env, argv[0]))
    res->data = nullptr;

  if (auto res = get_resource<duckdb::Connection>(env, argv[0]))
    res->data = nullptr;

  if (auto res = get_resource<duckdb::DuckDB>(env, argv[0]))
    res->data = nullptr;

  if (auto res = get_resource<duckdb::DBConfig>(env, argv[0]))
    res->data = nullptr;

  return nif::make_atom(env, "ok");
}

/*
 * Load the nif. Initialize some stuff
 */
static int
on_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM info) {
  database_nif_type = enif_open_resource_type(
    env,
    "duckdbex",
    "database_nif_type",
    resource_destructor<duckdb::DuckDB>,
    ERL_NIF_RT_CREATE,
    NULL);

  if (!database_nif_type) {
      return -1;
  }

  config_nif_type = enif_open_resource_type(
    env,
    "duckdbex",
    "config_nif_type",
    resource_destructor<duckdb::DBConfig>,
    ERL_NIF_RT_CREATE,
    NULL);

  if (!config_nif_type) {
      return -1;
  }

  connection_nif_type = enif_open_resource_type(
    env,
    "duckdbex",
    "connection_nif_type",
    resource_destructor<duckdb::Connection>,
    ERL_NIF_RT_CREATE,
    NULL);

  if (!connection_nif_type) {
      return -1;
  }

  appender_nif_type = enif_open_resource_type(
    env,
    "duckdbex",
    "appender_nif_type",
    resource_destructor<duckdb::Appender>,
    ERL_NIF_RT_CREATE,
    NULL);

  if (!appender_nif_type) {
      return -1;
  }

  query_result_nif_type = enif_open_resource_type(
    env,
    "duckdbex",
    "query_result_nif_type",
    resource_destructor<duckdb::QueryResult>,
    ERL_NIF_RT_CREATE,
    NULL);

  if (!query_result_nif_type) {
      return -1;
  }

  prepared_statement_nif_type = enif_open_resource_type(
    env,
    "duckdbex",
    "prepared_statement_nif_type",
    resource_destructor<duckdb::PreparedStatement>,
    ERL_NIF_RT_CREATE,
    NULL);

  if (!prepared_statement_nif_type) {
      return -1;
  }

  return 0;
}

static int
on_reload(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info) {
  return 0;
}

static int
on_upgrade(ErlNifEnv* env, void** priv, void** old_priv_data, ERL_NIF_TERM load_info) {
  return 0;
}

// C++ exceptions must not escape an appender NIF call into BEAM.
template <ERL_NIF_TERM (*Function)(ErlNifEnv*, int, const ERL_NIF_TERM[])>
static ERL_NIF_TERM
catch_appender_exceptions(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  try {
    return Function(env, argc, argv);
  } catch (const std::exception& ex) {
    return nif::make_error_tuple(env, ex.what());
  } catch (...) {
    return nif::make_error_tuple(env, "Unknown appender exception");
  }
}

static ErlNifFunc nif_funcs[] = {
  {"source_id", 0, source_id, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"library_version", 0, library_version, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"storage_format_version", 0, storage_format_version, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"library_version", 1, library_version_of_storage, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"platform", 0, platform, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"number_of_threads", 1, number_of_threads, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"extension_is_loaded", 2, extension_is_loaded, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"create_config", 0, create_config, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"set_config_option", 3, set_config_option, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"get_config_options", 0, get_config_options, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"open", 2, open, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"connection", 1, connection, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"query", 2, query_without_parameters, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"query", 3, query_with_parameters, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"prepare_statement", 2, prepare_statement, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"execute_statement", 1, execute_statement, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"execute_statement", 2, execute_statement, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"get_named_parameters", 1, get_named_parameters, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"begin_transaction", 1, begin_transaction, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"commit", 1, commit, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"rollback", 1, rollback, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"set_auto_commit", 2, set_auto_commit, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"is_auto_commit", 1, is_auto_commit, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"has_active_transaction", 1, has_active_transaction, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"columns", 1, columns, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"fetch_chunk", 1, fetch_chunk, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"fetch_all", 1, fetch_all, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"appender", 2, catch_appender_exceptions<appender>, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"appender", 3, catch_appender_exceptions<appender>, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"appender_add_row", 2, catch_appender_exceptions<appender_add_row>, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"appender_add_rows", 2, catch_appender_exceptions<appender_add_rows>, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"appender_flush", 1, catch_appender_exceptions<appender_flush>, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"appender_close", 1, catch_appender_exceptions<appender_close>, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"release", 1, release, ERL_NIF_DIRTY_JOB_IO_BOUND}
};

ERL_NIF_INIT(Elixir.Duckdbex.NIF, nif_funcs, on_load, on_reload, on_upgrade, NULL)
