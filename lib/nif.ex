defmodule Duckdbex.NIF do
  @moduledoc false

  @on_load :init

  @type db() :: reference()
  @type connection() :: reference()
  @type query_result() :: reference()
  @type statement() :: reference()
  @type appender :: reference()
  @type reason() :: :atom | binary()

  def init(),
    do: :ok = :erlang.load_nif(Path.join(:code.priv_dir(:duckdbex), "duckdb_nif"), 0)

  @spec release(db() | connection() | statement() | query_result() | appender()) :: :ok
  def release(_resource), do: :erlang.nif_error(:not_loaded)

  @spec open(binary(), Duckdbex.Config.t()) :: {:ok, db()} | {:error, reason()}
  def open(_path, _config), do: :erlang.nif_error(:not_loaded)

  @spec connection(db()) :: {:ok, connection()} | {:error, reason()}
  def connection(_database), do: :erlang.nif_error(:not_loaded)

  @spec query(connection(), binary()) :: {:ok, query_result()} | {:error, reason()}
  def query(_connection, _string_sql), do: :erlang.nif_error(:not_loaded)

  @spec query(connection(), binary(), list()) :: {:ok, query_result()} | {:error, reason()}
  def query(_connection, _string_sql, _args), do: :erlang.nif_error(:not_loaded)

  @spec prepare_statement(connection(), binary()) :: {:ok, statement()} | {:error, reason()}
  def prepare_statement(_connection, _string_sql), do: :erlang.nif_error(:not_loaded)

  @spec execute_statement(statement()) :: {:ok, query_result()} | {:error, reason()}
  def execute_statement(_statement), do: :erlang.nif_error(:not_loaded)

  @spec execute_statement(statement(), list()) :: {:ok, query_result()} | {:error, reason()}
  def execute_statement(_statement, _args), do: :erlang.nif_error(:not_loaded)

  @spec begin_transaction(connection()) :: :ok | {:error, reason()}
  def begin_transaction(_conn), do: :erlang.nif_error(:not_loaded)

  @spec commit(connection()) :: :ok | {:error, reason()}
  def commit(_conn), do: :erlang.nif_error(:not_loaded)

  @spec rollback(connection()) :: :ok | {:error, reason()}
  def rollback(_conn), do: :erlang.nif_error(:not_loaded)

  @spec set_auto_commit(connection(), boolean()) :: :ok | {:error, reason()}
  def set_auto_commit(_conn, _auto_commit?), do: :erlang.nif_error(:not_loaded)

  @spec is_auto_commit(connection()) :: {:ok, boolean()} | {:error, reason()}
  def is_auto_commit(_conn), do: :erlang.nif_error(:not_loaded)

  @spec has_active_transaction(connection()) :: {:ok, boolean()} | {:error, reason()}
  def has_active_transaction(_conn), do: :erlang.nif_error(:not_loaded)

  @spec columns(query_result()) :: list(binary()) | {:error, reason()}
  def columns(_query_result), do: :erlang.nif_error(:not_loaded)

  @spec fetch_chunk(query_result()) :: list() | {:error, reason()}
  def fetch_chunk(_query_result), do: :erlang.nif_error(:not_loaded)

  @spec fetch_all(query_result()) :: list() | {:error, reason()}
  def fetch_all(_query_result), do: :erlang.nif_error(:not_loaded)

  @spec appender(connection(), binary()) :: {:ok, appender()} | {:error, reason()}
  def appender(_connection, _table_name), do: :erlang.nif_error(:not_loaded)

  @spec appender(connection(), binary(), binary()) :: {:ok, appender()} | {:error, reason()}
  def appender(_connection, _schema_name, _table_name), do: :erlang.nif_error(:not_loaded)

  @spec appender_add_row(appender(), list()) :: :ok | {:error, reason()}
  def appender_add_row(_appender, _row), do: :erlang.nif_error(:not_loaded)

  @spec appender_add_rows(appender(), list(list())) :: :ok | {:error, reason()}
  def appender_add_rows(_appender, _rows), do: :erlang.nif_error(:not_loaded)

  @spec appender_flush(appender()) :: :ok | {:error, reason()}
  def appender_flush(_appender), do: :erlang.nif_error(:not_loaded)

  @spec appender_flush(appender()) :: :ok | {:error, reason()}
  def appender_close(_appender), do: :erlang.nif_error(:not_loaded)

  @spec library_version() :: binary()
  def library_version(), do: :erlang.nif_error(:not_loaded)

  @spec storage_format_version() :: integer()
  def storage_format_version(), do: :erlang.nif_error(:not_loaded)

  @spec library_version(integer()) :: binary()
  def library_version(_storage_format_version), do: :erlang.nif_error(:not_loaded)

  @spec source_id() :: binary()
  def source_id(), do: :erlang.nif_error(:not_loaded)

  @spec platform() :: binary()
  def platform(), do: :erlang.nif_error(:not_loaded)

  @spec number_of_threads(db()) :: integer()
  def number_of_threads(_database), do: :erlang.nif_error(:not_loaded)

  @spec extension_is_loaded(db(), binary()) :: boolean()
  def extension_is_loaded(_database, _extension_name), do: :erlang.nif_error(:not_loaded)
end
