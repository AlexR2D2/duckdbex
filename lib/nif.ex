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

  @spec open(binary(), Duckdbex.Config.t) :: {:ok, db()} | {:error, reason()}
  def open(_path, _config), do: :erlang.nif_error(:not_loaded)

  @spec connection(db()) :: {:ok, connection()} | {:error, reason()}
  def connection(_database), do: :erlang.nif_error(:not_loaded)

  @spec number_of_threads(db()) :: {:ok, integer()} | {:error, reason()}
  def number_of_threads(_database), do: :erlang.nif_error(:not_loaded)

  @spec source_id(db()) :: {:ok, binary()} | {:error, reason()}
  def source_id(_database), do: :erlang.nif_error(:not_loaded)

  @spec library_version(db()) :: {:ok, binary()} | {:error, reason()}
  def library_version(_database), do: :erlang.nif_error(:not_loaded)

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

  @spec fetch_chunk(query_result()) :: :ok | {:error, reason()}
  def fetch_chunk(_query_result), do: :erlang.nif_error(:not_loaded)

  @spec fetch_all(query_result()) :: :ok | {:error, reason()}
  def fetch_all(_query_result), do: :erlang.nif_error(:not_loaded)

  @spec appender(connection(), binary()) :: {:ok, appender()} | {:error, reason()}
  def appender(_connection, _table_name), do: :erlang.nif_error(:not_loaded)

  @spec appender_add_row(appender(), list()) :: :ok | {:error, reason()}
  def appender_add_row(_appender, _row), do: :erlang.nif_error(:not_loaded)

  @spec appender_add_rows(appender(), list(list())) :: :ok | {:error, reason()}
  def appender_add_rows(_appender, _rows), do: :erlang.nif_error(:not_loaded)

  @spec appender_flush(appender()) :: :ok | {:error, reason()}
  def appender_flush(_appender), do: :erlang.nif_error(:not_loaded)

  @spec appender_flush(appender()) :: :ok | {:error, reason()}
  def appender_close(_appender), do: :erlang.nif_error(:not_loaded)
end
