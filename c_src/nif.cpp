#include "config.h"
#include "resource.h"
#include "term.h"
#include "term_to_value.h"
#include "value_to_term.h"
#include "duckdb.hpp"
#include <erl_nif.h>
#include <string>
#include <iostream>

/*
 * Resources
 */

static ErlNifResourceType* database_nif_type = nullptr;
static ErlNifResourceType* connection_nif_type = nullptr;
static ErlNifResourceType* query_result_nif_type = nullptr;
static ErlNifResourceType* prepared_statement_nif_type = nullptr;
static ErlNifResourceType* appender_nif_type = nullptr;

/*
 * DuckDB API
 */

static ERL_NIF_TERM
number_of_threads(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  if (argc != 1)
    return enif_make_badarg(env);

  erlang_resource<duckdb::DuckDB>* dbres = nullptr;
  if(!enif_get_resource(env, argv[0], database_nif_type, (void**)&dbres))
    return enif_make_badarg(env);

  return enif_make_uint64(env, dbres->data->NumberOfThreads());
}

static ERL_NIF_TERM
extension_is_loaded(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  if (argc != 2)
    return enif_make_badarg(env);

  erlang_resource<duckdb::DuckDB>* dbres = nullptr;
  if(!enif_get_resource(env, argv[0], database_nif_type, (void**)&dbres))
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

  if (const char* lib_vsn = duckdb::GetDuckDBVersion(storage_format_version))
    return nif::make_binary_term(env, lib_vsn, std::strlen(lib_vsn));
  else
    return nif::make_atom(env, "nil");
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
open(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  std::string path;
  duckdb::DBConfig config;

  if (argc != 2)
    return enif_make_badarg(env);

  ErlNifBinary arg;
  if (!enif_inspect_binary(env, argv[0], &arg))
    return enif_make_badarg(env);

  path = std::string((const char*)arg.data, arg.size);

  if (!nif::is_atom(env, argv[1], "nil"))
    if (!nif::get_config(env, argv[1], config))
      return enif_make_badarg(env);

  try {
    ErlangResourceBuilder<duckdb::DuckDB> resource_builder(database_nif_type, path, &config);
    return nif::make_ok_tuple(env, resource_builder.make_and_release_resource(env));

  } catch (std::exception& ex) {
    return nif::make_error_tuple(env, ex.what());
  }
}

static ERL_NIF_TERM
connection(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  erlang_resource<duckdb::DuckDB>* dbres = nullptr;
  if(!enif_get_resource(env, argv[0], database_nif_type, (void**)&dbres))
    return enif_make_badarg(env);

  ErlangResourceBuilder<duckdb::Connection> resource_builder(connection_nif_type, *dbres->data);

  return nif::make_ok_tuple(env, resource_builder.make_and_release_resource(env));
}

static ERL_NIF_TERM
query(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  erlang_resource<duckdb::Connection>* connres = nullptr;
  if(!enif_get_resource(env, argv[0], connection_nif_type, (void**)&connres))
    return enif_make_badarg(env);

  ErlNifBinary sql_stmt;
  if (!enif_inspect_binary(env, argv[1], &sql_stmt))
    return enif_make_badarg(env);

  auto statement = connres->data->Prepare(std::string((const char*)sql_stmt.data, sql_stmt.size));
  if (!statement->success)
    return nif::make_error_tuple(env, statement->error.Message());

  duckdb::vector<duckdb::Value> query_params;

  duckdb::case_insensitive_map_t<duckdb::LogicalType> params_types = statement->GetExpectedParameterTypes();

  if (params_types.size()) {
    if (argc != 3)
      return enif_make_badarg(env);

    if (!enif_is_list(env, argv[2]))
      return enif_make_badarg(env);

    ERL_NIF_TERM item, items;
    items = argv[2];
    int arg_idx = 0;
    while(enif_get_list_cell(env, items, &item, &items)) {
      duckdb::Value value;
      auto arg_idx_str = std::to_string(arg_idx + 1);
      if (!nif::term_to_value(env, item, params_types[arg_idx_str], value))
        return nif::make_error_tuple(env, "invalid type of parameter #" + arg_idx_str);
      query_params.push_back(move(value));
      arg_idx++;
    }
  }

  duckdb::unique_ptr<duckdb::QueryResult> result = statement->Execute(query_params, false);

  if (result->HasError())
    return nif::make_error_tuple(env, result->GetErrorObject().Message());

  ErlangResourceBuilder<duckdb::QueryResult> resource_builder(
    query_result_nif_type,
    std::move(result));

  return nif::make_ok_tuple(env, resource_builder.make_and_release_resource(env));
}

static ERL_NIF_TERM
prepare_statement(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  erlang_resource<duckdb::Connection>* connres = nullptr;
  if(!enif_get_resource(env, argv[0], connection_nif_type, (void**)&connres))
    return enif_make_badarg(env);

  ErlNifBinary sql_stmt;
  if (!enif_inspect_binary(env, argv[1], &sql_stmt))
    return enif_make_badarg(env);

  auto statement = connres->data->Prepare(std::string((const char*)sql_stmt.data, sql_stmt.size));
  if (!statement->success)
    return nif::make_error_tuple(env, statement->error.Message());

  ErlangResourceBuilder<duckdb::PreparedStatement> resource_builder(
    prepared_statement_nif_type,
    std::move(statement));

  return nif::make_ok_tuple(env, resource_builder.make_and_release_resource(env));
}

static ERL_NIF_TERM
execute_statement(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  erlang_resource<duckdb::PreparedStatement>* stmtres = nullptr;
  if(!enif_get_resource(env, argv[0], prepared_statement_nif_type, (void**)&stmtres))
    return enif_make_badarg(env);

  duckdb::vector<duckdb::Value> query_params;
  duckdb::case_insensitive_map_t<duckdb::LogicalType> params_types = stmtres->data->GetExpectedParameterTypes();

  if (params_types.size()) {
    if (argc != 2)
      return enif_make_badarg(env);

    if (!enif_is_list(env, argv[1]))
      return enif_make_badarg(env);

    ERL_NIF_TERM item, items;
    items = argv[1];
    int arg_idx = 0;
    while(enif_get_list_cell(env, items, &item, &items)) {
      duckdb::Value value;
      auto arg_idx_str = std::to_string(arg_idx + 1);
      if (!nif::term_to_value(env, item, params_types[arg_idx_str], value))
        return nif::make_error_tuple(env, "invalid type of parameter #" + arg_idx_str);
      query_params.push_back(move(value));
      arg_idx++;
    }
  }

  duckdb::unique_ptr<duckdb::QueryResult> result = stmtres->data->Execute(query_params);

  if (result->HasError())
    return nif::make_error_tuple(env, result->GetErrorObject().Message());

  ErlangResourceBuilder<duckdb::QueryResult> resource_builder(
    query_result_nif_type,
    std::move(result));

  return nif::make_ok_tuple(env, resource_builder.make_and_release_resource(env));
}

static ERL_NIF_TERM
columns(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  if (argc != 1)
    return enif_make_badarg(env);

  erlang_resource<duckdb::QueryResult>* result = nullptr;
  if(!enif_get_resource(env, argv[0], query_result_nif_type, (void**)&result))
    return enif_make_badarg(env);

  if (result->data->HasError()) {
    auto error = result->data->GetError();
    return nif::make_error_tuple(env, error);
  }

  if (duckdb::idx_t columns_count = result->data->ColumnCount()) {
    std::vector<ERL_NIF_TERM> columns(columns_count);
    for (duckdb::idx_t col = 0; col < columns_count; col++) {
      duckdb::string column_name = result->data->ColumnName(col);
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

  erlang_resource<duckdb::QueryResult>* result = nullptr;
  if(!enif_get_resource(env, argv[0], query_result_nif_type, (void**)&result))
    return enif_make_badarg(env);

  if (result->data->HasError()) {
    auto error = result->data->GetError();
    return nif::make_error_tuple(env, error);
  }

  std::vector<ERL_NIF_TERM> rows;

  duckdb::unique_ptr<duckdb::DataChunk> chunk;
  duckdb::PreservedError error;
  if (result->data->TryFetch(chunk, error) && chunk) {
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

  erlang_resource<duckdb::QueryResult>* result = nullptr;
  if(!enif_get_resource(env, argv[0], query_result_nif_type, (void**)&result))
    return enif_make_badarg(env);

  if (result->data->HasError()) {
    auto error = result->data->GetError();
    return nif::make_error_tuple(env, error);
  }

  std::vector<ERL_NIF_TERM> rows;

  duckdb::unique_ptr<duckdb::DataChunk> chunk;
  duckdb::PreservedError error;
  while (result->data->TryFetch(chunk, error) && chunk) {
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
  erlang_resource<duckdb::Connection>* connres = nullptr;
  if(!enif_get_resource(env, argv[0], connection_nif_type, (void**)&connres))
    return enif_make_badarg(env);

  ErlNifBinary binary_table_name;
  if (!enif_inspect_binary(env, argv[1], &binary_table_name))
    return enif_make_badarg(env);

  std::string table_name((const char*)binary_table_name.data, binary_table_name.size);

  if (!connres->data->TableInfo(table_name))
    return nif::make_error_tuple(env, "Table '" + table_name + "' could not be found");

  ErlangResourceBuilder<duckdb::Appender> resource_builder(appender_nif_type, *connres->data, table_name);
  return nif::make_ok_tuple(env, resource_builder.make_and_release_resource(env));
}

static ERL_NIF_TERM
appender_add_row(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  if (argc != 2)
    return enif_make_badarg(env);

  erlang_resource<duckdb::Appender>* apres = nullptr;
  if(!enif_get_resource(env, argv[0], appender_nif_type, (void**)&apres))
    return enif_make_badarg(env);

  if (!enif_is_list(env, argv[1]))
    return enif_make_badarg(env);

  duckdb::vector<duckdb::LogicalType> types = apres->data->GetTypes();

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

  erlang_resource<duckdb::Appender>* apres = nullptr;
  if(!enif_get_resource(env, argv[0], appender_nif_type, (void**)&apres))
    return enif_make_badarg(env);

  if (!enif_is_list(env, argv[1]))
    return enif_make_badarg(env);

  duckdb::vector<duckdb::LogicalType> types = apres->data->GetTypes();

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

  erlang_resource<duckdb::Appender>* apres = nullptr;
  if(!enif_get_resource(env, argv[0], appender_nif_type, (void**)&apres))
    return enif_make_badarg(env);

  apres->data->Flush();

  return nif::make_atom(env, "ok");
}

static ERL_NIF_TERM
appender_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  if (argc != 1)
    return enif_make_badarg(env);

  erlang_resource<duckdb::Appender>* apres = nullptr;
  if(!enif_get_resource(env, argv[0], appender_nif_type, (void**)&apres))
    return enif_make_badarg(env);

  apres->data->Close();

  return nif::make_atom(env, "ok");
}

/*
 * Resources destructors
 */

static void
database_type_destructor(ErlNifEnv* env, void* arg) {
  erlang_resource<duckdb::DuckDB>* resource = static_cast<erlang_resource<duckdb::DuckDB>*>(arg);
  resource->data = nullptr;
}

static void
connection_type_destructor(ErlNifEnv* env, void* arg) {
  erlang_resource<duckdb::Connection>* resource = static_cast<erlang_resource<duckdb::Connection>*>(arg);
  resource->data = nullptr;
}

static void
appender_type_destructor(ErlNifEnv* env, void* arg) {
  erlang_resource<duckdb::Appender>* resource = static_cast<erlang_resource<duckdb::Appender>*>(arg);
  resource->data = nullptr;
}

static void
query_result_type_destructor(ErlNifEnv* env, void* arg) {
  erlang_resource<duckdb::QueryResult>* resource = static_cast<erlang_resource<duckdb::QueryResult>*>(arg);
  resource->data = nullptr;
}

static void
prepared_statement_type_destructor(ErlNifEnv* env, void* arg) {
  erlang_resource<duckdb::PreparedStatement>* resource = static_cast<erlang_resource<duckdb::PreparedStatement>*>(arg);
  resource->data = nullptr;
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
    database_type_destructor,
    ERL_NIF_RT_CREATE,
    NULL);

  if (!database_nif_type) {
      return -1;
  }

  connection_nif_type = enif_open_resource_type(
    env,
    "duckdbex",
    "connection_nif_type",
    connection_type_destructor,
    ERL_NIF_RT_CREATE,
    NULL);

  if (!connection_nif_type) {
      return -1;
  }

  appender_nif_type = enif_open_resource_type(
    env,
    "duckdbex",
    "appender_nif_type",
    appender_type_destructor,
    ERL_NIF_RT_CREATE,
    NULL);

  if (!appender_nif_type) {
      return -1;
  }

  query_result_nif_type = enif_open_resource_type(
    env,
    "duckdbex",
    "query_result_nif_type",
    query_result_type_destructor,
    ERL_NIF_RT_CREATE,
    NULL);

  if (!query_result_nif_type) {
      return -1;
  }

  prepared_statement_nif_type = enif_open_resource_type(
    env,
    "duckdbex",
    "prepared_statement_nif_type",
    prepared_statement_type_destructor,
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

static ErlNifFunc nif_funcs[] = {
  {"source_id", 0, source_id, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"library_version", 0, library_version, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"storage_format_version", 0, storage_format_version, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"library_version", 1, library_version_of_storage, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"platform", 0, platform, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"number_of_threads", 1, number_of_threads, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"extension_is_loaded", 2, extension_is_loaded, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"open", 2, open, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"connection", 1, connection, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"query", 2, query, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"query", 3, query, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"prepare_statement", 2, prepare_statement, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"execute_statement", 1, execute_statement, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"execute_statement", 2, execute_statement, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"columns", 1, columns, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"fetch_chunk", 1, fetch_chunk, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"fetch_all", 1, fetch_all, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"appender", 2, appender, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"appender_add_row", 2, appender_add_row, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"appender_add_rows", 2, appender_add_rows, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"appender_flush", 1, appender_flush, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"appender_close", 1, appender_close, ERL_NIF_DIRTY_JOB_IO_BOUND}
};

ERL_NIF_INIT(Elixir.Duckdbex.NIF, nif_funcs, on_load, on_reload, on_upgrade, NULL)
