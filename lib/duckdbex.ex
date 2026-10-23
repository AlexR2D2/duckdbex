defmodule Duckdbex do
  @moduledoc """
  DuckDB API module
  """

  @type config() :: reference()
  @type db() :: reference()
  @type reason() :: :atom | binary()
  @type connection() :: reference()
  @type statement() :: reference()
  @type query_result() :: reference()
  @type appender :: reference()

  @doc """
  Creates a DuckDB config object.

  The config can be reused across `open/2` calls and configured through `set_config_option/3`.

  ## Examples

    iex> {:ok, _config} = Duckdbex.create_config()
  """
  @spec create_config() :: {:ok, config()} | {:error, reason()}
  def create_config(),
    do: Duckdbex.NIF.create_config()

  @doc """
  Sets a DuckDB config option by name.

  Use `get_config_options/0` to discover the options reported by DuckDB.

  For string-like options pass binaries. For boolean options pass `true`/`false`.

  ## Examples

    iex> {:ok, config} = Duckdbex.create_config()
    iex> :ok = Duckdbex.set_config_option(config, "threads", 1)
  """
  @spec set_config_option(config(), binary() | atom(), term()) :: :ok | {:error, reason()}
  def set_config_option(config, name, value)
      when is_reference(config) and (is_binary(name) or is_atom(name)),
      do: Duckdbex.NIF.set_config_option(config, name, value)

  @doc """
  Returns the DuckDB config options reported by `DBConfig::GetOptions()`.

  Each option is represented as a map with metadata such as name, type, scope and default value.
  """
  @spec get_config_options() :: list(map())
  def get_config_options(),
    do: Duckdbex.NIF.get_config_options()

  @doc """
  Release resource (config, db, connection, stmt, query_result)

  For appenders, first call `appender_close/1` while the creating connection is
  still open and reachable, and check for `:ok`. Releasing an appender directly
  or relying on garbage collection does not guarantee that buffered rows are written.

  Will cause destruction and automatic closing the releasing resource in the calling process on dirty schedulers. The released resource cannot be used after this point.

  If you do not release some resource you have the resource will be automatically released by the Erlang garbage collector (GC) when you lose last ref to the resource, but the GC may run resource releasing in the main schedulers which may affect the performance of your main system perfomance.

  ## Examples

    iex> {:ok, db} = Duckdbex.open("my_database.duckdb")
    iex> {:ok, conn} = Duckdbex.connection(db)
    iex> {:ok, res} = Duckdbex.query(conn, "SELECT 1 WHERE $1 = 1;", [1])
    iex> :ok = Duckdbex.release(res)
    iex> :ok = Duckdbex.release(conn)
    iex> :ok = Duckdbex.release(db)
  """
  @spec release(config() | db() | connection() | statement() | query_result() | appender()) :: :ok
  def release(resource) when is_reference(resource),
    do: Duckdbex.NIF.release(resource)

  @doc """
  Opens database in the specified file.

  If specified file does not exist, a new database file with the given name will be created automatically.

  ## Examples

    iex> {:ok, _config} = Duckdbex.create_config()
    iex> {:ok, _db} = Duckdbex.open("my_database.duckdb", nil)
  """
  @spec open(binary(), config() | nil) :: {:ok, db()} | {:error, reason()}
  def open(path, config) when is_binary(path),
    do: Duckdbex.NIF.open(path, config)

  @doc """
  If the path to the file is specified, then opens the database in the file.

  If specified file does not exist, a new database file with the given name will be created automatically.
  If config is specified, then opens the database in memory with that config.

  ## Examples

    iex> {:ok, _db} = Duckdbex.open("my_database.duckdb")

    iex> {:ok, config} = Duckdbex.create_config()
    iex> :ok = Duckdbex.set_config_option(config, "threads", 1)
    iex> {:ok, _db} = Duckdbex.open(config)
  """
  @spec open(binary() | config()) :: {:ok, db()} | {:error, reason()}
  def open(path) when is_binary(path),
    do: Duckdbex.NIF.open(path, nil)

  def open(config) when is_reference(config),
    do: Duckdbex.NIF.open(":memory:", config)

  @doc """
  Opens database in the memory.

  ## Examples

    iex> {:ok, _db} = Duckdbex.open()
  """
  @spec open() :: {:ok, db()} | {:error, reason()}
  def open(),
    do: Duckdbex.NIF.open(":memory:", nil)

  @doc """
  Creates connection object to work with database.

  To work with database the connection object is requiered. Connection object hold a shared reference to database, so it is possible to forget the database reference and hold the connection reference only.

  ## Examples

    iex> {:ok, db} = Duckdbex.open()
    iex> {:ok, _conn} = Duckdbex.connection(db)
  """
  @spec connection(db()) :: {:ok, connection()} | {:error, reason()}
  def connection(db) when is_reference(db),
    do: Duckdbex.NIF.connection(db)

  @doc """
  Issues a query to the database and returns a result reference.

  ## Examples

    iex> {:ok, db} = Duckdbex.open()
    iex> {:ok, conn} = Duckdbex.connection(db)
    iex> {:ok, _res} = Duckdbex.query(conn, "SELECT 1;")
  """
  @spec query(connection(), binary()) :: {:ok, query_result()} | {:error, reason()}
  def query(connection, sql_string) when is_reference(connection) and is_binary(sql_string),
    do: Duckdbex.NIF.query(connection, sql_string)

  @doc """
  Issues a query to the database with parameters and returns a result reference.

  Parameters can be positional (a list) or named (a map using the `$name`
  syntax). Named parameters are matched case-insensitively.

  ## Examples

    iex> {:ok, db} = Duckdbex.open()
    iex> {:ok, conn} = Duckdbex.connection(db)
    iex> {:ok, _res} = Duckdbex.query(conn, "SELECT 1 WHERE $1 = 1;", [1])

    iex> {:ok, db} = Duckdbex.open()
    iex> {:ok, conn} = Duckdbex.connection(db)
    iex> {:ok, res} = Duckdbex.query(conn, "SELECT $name::VARCHAR;", %{"name" => "world"})
    iex> [["world"]] = Duckdbex.fetch_all(res)
  """
  @spec query(connection(), binary(), list() | map()) ::
          {:ok, query_result()} | {:error, reason()}
  def query(connection, sql_string, args)
      when is_reference(connection) and is_binary(sql_string) and is_list(args),
      do: Duckdbex.NIF.query(connection, sql_string, args)

  @spec query(connection(), binary(), map()) :: {:ok, query_result()} | {:error, reason()}
  def query(connection, sql_string, args)
      when is_reference(connection) and is_binary(sql_string) and is_map(args),
      do: Duckdbex.NIF.query(connection, sql_string, args)

  @doc """
  Prepare the specified query, returning a reference to the prepared statement object

  ## Examples

    iex> {:ok, db} = Duckdbex.open()
    iex> {:ok, conn} = Duckdbex.connection(db)
    iex> {:ok, _stmt} = Duckdbex.prepare_statement(conn, "SELECT 1 WHERE $1 = 1;")
  """
  @spec prepare_statement(connection(), binary()) :: {:ok, statement()} | {:error, reason()}
  def prepare_statement(connection, sql_string)
      when is_reference(connection) and is_binary(sql_string),
      do: Duckdbex.NIF.prepare_statement(connection, sql_string)

  @doc """
  Execute the prepared statement

  ## Examples

    iex> {:ok, db} = Duckdbex.open()
    iex> {:ok, conn} = Duckdbex.connection(db)
    iex> {:ok, stmt} = Duckdbex.prepare_statement(conn, "SELECT 1;")
    iex> {:ok, res} = Duckdbex.execute_statement(stmt)
    iex> [[1]] = Duckdbex.fetch_all(res)
  """
  @spec execute_statement(statement()) :: {:ok, query_result()} | {:error, reason()}
  def execute_statement(statement) when is_reference(statement),
    do: Duckdbex.NIF.execute_statement(statement)

  @doc """
  Execute the prepared statement with the given list or map of parameters

  ## Examples

    iex> {:ok, db} = Duckdbex.open()
    iex> {:ok, conn} = Duckdbex.connection(db)
    iex> {:ok, stmt} = Duckdbex.prepare_statement(conn, "SELECT 1 WHERE $1 = 1;")
    iex> {:ok, res} = Duckdbex.execute_statement(stmt, [1])
    iex> [[1]] = Duckdbex.fetch_all(res)

    iex> {:ok, db} = Duckdbex.open()
    iex> {:ok, conn} = Duckdbex.connection(db)
    iex> {:ok, stmt} = Duckdbex.prepare_statement(conn, "SELECT $value::INTEGER;")
    iex> {:ok, res} = Duckdbex.execute_statement(stmt, %{"value" => 1})
    iex> [[1]] = Duckdbex.fetch_all(res)
  """
  @spec execute_statement(statement(), list()) :: {:ok, query_result()} | {:error, reason()}
  def execute_statement(statement, args) when is_reference(statement) and is_list(args),
    do: Duckdbex.NIF.execute_statement(statement, args)

  @spec execute_statement(statement(), map()) :: {:ok, query_result()} | {:error, reason()}
  def execute_statement(statement, args) when is_reference(statement) and is_map(args),
    do: Duckdbex.NIF.execute_statement(statement, args)

  @doc """
  Returns the named parameters of a prepared statement as `{name, type}` tuples.

  Positional parameters (`$1`, `$2`, ...) are not included.

  ## Examples

    iex> {:ok, db} = Duckdbex.open()
    iex> {:ok, conn} = Duckdbex.connection(db)
    iex> {:ok, stmt} = Duckdbex.prepare_statement(conn, "SELECT $value::INTEGER;")
    iex> [{"value", :integer}] = Duckdbex.get_named_parameters(stmt)
  """
  @spec get_named_parameters(statement()) :: [{binary(), atom()}]
  def get_named_parameters(statement) when is_reference(statement),
    do: Duckdbex.NIF.get_named_parameters(statement)

  @doc """
  Begin a transaction

  ## Examples

    iex> {:ok, db} = Duckdbex.open()
    iex> {:ok, conn} = Duckdbex.connection(db)
    iex> :ok = Duckdbex.begin_transaction(conn)
  """
  @spec begin_transaction(connection()) :: :ok | {:error, reason()}
  def begin_transaction(connection) when is_reference(connection),
    do: Duckdbex.NIF.begin_transaction(connection)

  @doc """
  Commit the transaction

  ## Examples

    iex> {:ok, db} = Duckdbex.open()
    iex> {:ok, conn} = Duckdbex.connection(db)
    iex> :ok = Duckdbex.begin_transaction(conn)
    iex> :ok = Duckdbex.commit(conn)
  """
  @spec commit(connection()) :: :ok | {:error, reason()}
  def commit(connection) when is_reference(connection),
    do: Duckdbex.NIF.commit(connection)

  @doc """
  Rollback the transaction

  ## Examples

    iex> {:ok, db} = Duckdbex.open()
    iex> {:ok, conn} = Duckdbex.connection(db)
    iex> :ok = Duckdbex.begin_transaction(conn)
    iex> :ok = Duckdbex.rollback(conn)
  """
  @spec rollback(connection()) :: :ok | {:error, reason()}
  def rollback(connection) when is_reference(connection),
    do: Duckdbex.NIF.rollback(connection)

  @doc """
  Set transaction autocommit for connection

  ## Examples

    iex> {:ok, db} = Duckdbex.open()
    iex> {:ok, conn} = Duckdbex.connection(db)
    iex> :ok = Duckdbex.set_auto_commit(conn, true)
  """
  @spec set_auto_commit(connection(), boolean()) :: :ok | {:error, reason()}
  def set_auto_commit(connection, auto_commit?)
      when is_reference(connection) and is_boolean(auto_commit?),
      do: Duckdbex.NIF.set_auto_commit(connection, auto_commit?)

  @doc """
  Check if transaction autocommit is set for connection

  ## Examples

    iex> {:ok, db} = Duckdbex.open()
    iex> {:ok, conn} = Duckdbex.connection(db)
    iex> {:ok, true} = Duckdbex.is_auto_commit(conn)
  """
  @spec is_auto_commit(connection()) :: {:ok, boolean()} | {:error, reason()}
  def is_auto_commit(connection) when is_reference(connection),
    do: Duckdbex.NIF.is_auto_commit(connection)

  @doc """
  Checks if there is an active transaction for connection

  ## Examples

    iex> {:ok, db} = Duckdbex.open()
    iex> {:ok, conn} = Duckdbex.connection(db)
    iex> {:ok, false} = Duckdbex.has_active_transaction(conn)
  """
  @spec has_active_transaction(connection()) :: {:ok, boolean()} | {:error, reason()}
  def has_active_transaction(connection) when is_reference(connection),
    do: Duckdbex.NIF.has_active_transaction(connection)

  @doc """
  Returns columns names from the query result.

  ## Examples

    iex> {:ok, db} = Duckdbex.open()
    iex> {:ok, conn} = Duckdbex.connection(db)
    iex> {:ok, res} = Duckdbex.query(conn, "SELECT 1 as 'my_name';")
    iex> ["my_name"] = Duckdbex.columns(res)
  """
  @spec columns(query_result()) :: list() | {:error, reason()}
  def columns(query_result) when is_reference(query_result),
    do: Duckdbex.NIF.columns(query_result)

  @doc """
  Fetches a data chunk from the query result.

  Returns empty list if there are no more results to fetch.

  ## Examples

    iex> {:ok, db} = Duckdbex.open()
    iex> {:ok, conn} = Duckdbex.connection(db)
    iex> {:ok, res} = Duckdbex.query(conn, "SELECT 1;")
    iex> [[1]] = Duckdbex.fetch_chunk(res)
  """
  @spec fetch_chunk(query_result()) :: list() | {:error, reason()}
  def fetch_chunk(query_result) when is_reference(query_result),
    do: Duckdbex.NIF.fetch_chunk(query_result)

  @doc """
  Fetches all data from the query result.

  Returns empty list if there are no result to fetch.

  ## Examples

    iex> {:ok, db} = Duckdbex.open()
    iex> {:ok, conn} = Duckdbex.connection(db)
    iex> {:ok, res} = Duckdbex.query(conn, "SELECT 1;")
    iex> [[1]] = Duckdbex.fetch_all(res)
  """
  @spec fetch_all(query_result()) :: list() | {:error, reason()}
  def fetch_all(query_result) when is_reference(query_result),
    do: Duckdbex.NIF.fetch_all(query_result)

  @doc """
  Creates the Appender to load bulk data into a DuckDB database.

  This is the recommended way to load bulk data.

  The appender does not keep its creating connection alive. Keep that connection
  open and reachable until `appender_close/1` succeeds. A variable being in scope
  is not enough: after its last use, it may no longer keep the resource alive.
  Calling `release(conn)` after closing the appender, as below, keeps the connection
  reachable throughout the appender operations.

  Flushing buffered rows after the connection is released or garbage-collected
  returns `{:error, reason}`. Do not rely on garbage collection to save buffered
  rows; explicitly close the appender and check for `:ok`.

  ## Examples

    iex> {:ok, db} = Duckdbex.open()
    iex> {:ok, conn} = Duckdbex.connection(db)
    iex> {:ok, _res} = Duckdbex.query(conn, "CREATE TABLE table_1 (data INTEGER);")
    iex> {:ok, appender} = Duckdbex.appender(conn, "table_1")
    iex> :ok = Duckdbex.appender_close(appender)
    iex> :ok = Duckdbex.release(conn)
  """
  @spec appender(connection(), binary()) :: {:ok, appender()} | {:error, reason()}
  def appender(connection, table_name) when is_reference(connection) and is_binary(table_name),
    do: Duckdbex.NIF.appender(connection, table_name)

  @doc """
  Creates the Appender to load bulk data into a DuckDB database with schema name.

  Keep the creating connection open and reachable until the appender is closed.
  See `appender/2` for the connection lifetime requirements.

  ## Examples

    iex> {:ok, db} = Duckdbex.open()
    iex> {:ok, conn} = Duckdbex.connection(db)
    iex> {:ok, _res} = Duckdbex.query(conn, "CREATE SCHEMA schema_1")
    iex> {:ok, _res} = Duckdbex.query(conn, "CREATE TABLE schema_1.table_1 (data INTEGER);")
    iex> {:ok, appender} = Duckdbex.appender(conn, "schema_1", "table_1")
    iex> :ok = Duckdbex.appender_close(appender)
    iex> :ok = Duckdbex.release(conn)
  """
  @spec appender(connection(), binary(), binary()) :: {:ok, appender()} | {:error, reason()}
  def appender(connection, schema_name, table_name)
      when is_reference(connection) and is_binary(schema_name) and is_binary(table_name),
      do: Duckdbex.NIF.appender(connection, schema_name, table_name)

  @doc """
  Append row into a DuckDB database table.

  Rows are buffered and may not be visible immediately. Call `appender_flush/1`
  or `appender_close/1` and check for `:ok` to confirm that buffered rows were written.
  Keep the creating connection open and reachable until the appender is closed;
  see `appender/2`. Garbage collection does not guarantee a successful flush.

  ## Examples

    iex> {:ok, db} = Duckdbex.open()
    iex> {:ok, conn} = Duckdbex.connection(db)
    iex> {:ok, _res} = Duckdbex.query(conn, "CREATE TABLE table_1 (data INTEGER);")
    iex> {:ok, appender} = Duckdbex.appender(conn, "table_1")
    iex> :ok = Duckdbex.appender_add_row(appender, [1])
    iex> :ok = Duckdbex.appender_close(appender)
    iex> :ok = Duckdbex.release(conn)
  """
  @spec appender_add_row(appender(), list()) :: :ok | {:error, reason()}
  def appender_add_row(appender, row) when is_reference(appender) and is_list(row),
    do: Duckdbex.NIF.appender_add_row(appender, row)

  @doc """
  Append multiple rows into a DuckDB database table at once.

  Rows are buffered and may not be visible immediately. Call `appender_flush/1`
  or `appender_close/1` and check for `:ok` to confirm that buffered rows were written.
  Keep the creating connection open and reachable until the appender is closed;
  see `appender/2`. Garbage collection does not guarantee a successful flush.

  ## Examples

    iex> {:ok, db} = Duckdbex.open()
    iex> {:ok, conn} = Duckdbex.connection(db)
    iex> {:ok, _res} = Duckdbex.query(conn, "CREATE TABLE table_1 (the_n1 INTEGER, the_str1 STRING);")
    iex> {:ok, appender} = Duckdbex.appender(conn, "table_1")
    iex> :ok = Duckdbex.appender_add_rows(appender, [[1, "one"], [2, "two"]])
    iex> :ok = Duckdbex.appender_close(appender)
    iex> :ok = Duckdbex.release(conn)
  """
  @spec appender_add_rows(appender(), list(list())) :: :ok | {:error, reason()}
  def appender_add_rows(appender, rows) when is_reference(appender) and is_list(rows),
    do: Duckdbex.NIF.appender_add_rows(appender, rows)

  @doc """
  Flush buffered rows without closing the appender.

  Keep the creating connection open and reachable; see `appender/2`.
  Returns `:ok` on success or `{:error, reason}` if flushing fails.

  ## Examples

    iex> {:ok, db} = Duckdbex.open()
    iex> {:ok, conn} = Duckdbex.connection(db)
    iex> {:ok, _res} = Duckdbex.query(conn, "CREATE TABLE table_1 (the_n1 INTEGER, the_str1 STRING);")
    iex> {:ok, appender} = Duckdbex.appender(conn, "table_1")
    iex> :ok = Duckdbex.appender_add_rows(appender, [[1, "one"], [2, "two"]])
    iex> {:ok, res} = Duckdbex.query(conn, "SELECT * FROM table_1;")
    iex> [] = Duckdbex.fetch_all(res)
    iex> :ok = Duckdbex.appender_flush(appender)
    iex> {:ok, res} = Duckdbex.query(conn, "SELECT * FROM table_1;")
    iex> [[1, "one"], [2, "two"]] = Duckdbex.fetch_all(res)
    iex> :ok = Duckdbex.appender_close(appender)
    iex> :ok = Duckdbex.release(conn)
  """
  @spec appender_flush(appender()) :: :ok | {:error, reason()}
  def appender_flush(appender) when is_reference(appender),
    do: Duckdbex.NIF.appender_flush(appender)

  @doc """
  Flush buffered rows and close the appender, releasing its resource on success.

  Keep the creating connection open and reachable until this call completes;
  see `appender/2`. Returns `:ok` on success or `{:error, reason}` if closing fails.
  After a successful close, the appender cannot be used again.

  Check the result before releasing the connection. `release/1` can clean up an
  appender after an error, but does not guarantee that buffered rows were written.

  ## Examples

    iex> {:ok, db} = Duckdbex.open()
    iex> {:ok, conn} = Duckdbex.connection(db)
    iex> {:ok, _res} = Duckdbex.query(conn, "CREATE TABLE table_1 (the_n1 INTEGER, the_str1 STRING);")
    iex> {:ok, appender} = Duckdbex.appender(conn, "table_1")
    iex> :ok = Duckdbex.appender_add_rows(appender, [[1, "one"], [2, "two"]])
    iex> {:ok, res} = Duckdbex.query(conn, "SELECT * FROM table_1;")
    iex> [] = Duckdbex.fetch_all(res)
    iex> :ok = Duckdbex.appender_close(appender)
    iex> {:ok, res} = Duckdbex.query(conn, "SELECT * FROM table_1;")
    iex> [[1, "one"], [2, "two"]] = Duckdbex.fetch_all(res)
    iex> :ok = Duckdbex.release(conn)
  """
  @spec appender_close(appender()) :: :ok | {:error, reason()}
  def appender_close(appender) when is_reference(appender),
    do: Duckdbex.NIF.appender_close(appender)

  @doc """
  Returns the version of the linked DuckDB, with a version postfix for dev versions

  Usually used for developing C extensions that must return this for a compatibility check.

  ## Examples

    iex> Duckdbex.library_version()
    iex> "v0.7.0"
  """
  @spec library_version() :: binary()
  def library_version(),
    do: Duckdbex.NIF.library_version()

  @doc """
  Returns the version number of the database storage format

  ## Examples

    iex> Duckdbex.storage_format_version()
    iex> 43
  """
  @spec storage_format_version() :: integer()
  def storage_format_version(),
    do: Duckdbex.NIF.storage_format_version()

  @doc """
  Returns the version of the DuckDB library, for storage format version

  ## Examples

    iex> Duckdbex.library_version(39)
    iex> "v0.6.0 or v0.6.1"
  """
  @spec library_version(integer()) :: binary()
  def library_version(storage_format_version) when is_integer(storage_format_version),
    do: Duckdbex.NIF.library_version(storage_format_version)

  @doc """
  Returns the commit hash of the linked DuckDB library

  ## Examples

    iex> Duckdbex.source_id()
    iex> "b00b93f0b1"
  """
  @spec source_id() :: binary()
  def source_id(),
    do: Duckdbex.NIF.source_id()

  @doc """
  Returns the Platform of the linked DuckDB library

  ## Examples

    iex> Duckdbex.platform()
    iex> "osx_amd64"
  """
  @spec platform() :: binary()
  def platform(),
    do: Duckdbex.NIF.platform()

  @doc """
  Returns whether extension is loaded

  ## Examples

    iex> {:ok, db} = Duckdbex.open()
    iex> Duckdbex.extension_is_loaded(db, "parquet")
    iex> false
  """
  @spec extension_is_loaded(db(), binary()) :: boolean()
  def extension_is_loaded(db, extension_name),
    do: Duckdbex.NIF.extension_is_loaded(db, extension_name)

  @doc """
  Returns the count of DuckDB threads

  This is DuckDB own native threads (not a dirty scheduler Erlang threads)
  ## Examples

    iex> {:ok, db} = Duckdbex.open()
    iex> Duckdbex.number_of_threads(db)
    iex> 8
  """
  @spec number_of_threads(db()) :: integer()
  def number_of_threads(db),
    do: Duckdbex.NIF.number_of_threads(db)

  @doc """
  Convert an erlang/elixir integer to a DuckDB hugeint.

  For more information on DuckDB numeric types, see [DuckDB Numeric Data Types](https://duckdb.org/docs/sql/data_types/numeric) For more information on DuckDB numeric types.

  ## Examples

    iex> {:ok, db} = Duckdbex.open()
    iex> {:ok, conn} = Duckdbex.connection(db)
    iex> {:ok, _res} = Duckdbex.query(conn, "CREATE TABLE hugeints(value HUGEINT);")
    iex> {:ok, _res} = Duckdbex.query(conn, "INSERT INTO hugeints VALUES (98233720368547758080000::hugeint);")
    iex> hugeint = Duckdbex.integer_to_hugeint(98233720368547758080000)
    iex> %Duckdbex.Hugeint{upper: 5325, lower: 4808176044395724800} = hugeint
    iex> {:ok, res} = Duckdbex.query(conn, "SELECT * FROM hugeints WHERE value = $1", [hugeint])
    iex> [[%Duckdbex.Hugeint{upper: 5325, lower: 4808176044395724800}]] = Duckdbex.fetch_all(res)
  """
  def integer_to_hugeint(integer) when is_integer(integer),
    do: Duckdbex.Hugeint.from_integer(integer)

  @doc """
  Convert a duckdb hugeint record to erlang/elixir integer.

  ## Examples

    iex> {:ok, db} = Duckdbex.open()
    iex> {:ok, conn} = Duckdbex.connection(db)
    iex> {:ok, _res} = Duckdbex.query(conn, "CREATE TABLE hugeints(value HUGEINT);")
    iex> {:ok, _res} = Duckdbex.query(conn, "INSERT INTO hugeints VALUES (98233720368547758080000::hugeint);")
    iex> {:ok, res} = Duckdbex.query(conn, "SELECT * FROM hugeints;")
    iex> [[hugeint = %Duckdbex.Hugeint{upper: 5325, lower: 4808176044395724800}]] = Duckdbex.fetch_all(res)
    iex> 98233720368547758080000 = Duckdbex.hugeint_to_integer(hugeint)
  """
  def hugeint_to_integer(hugeint), do: Duckdbex.Hugeint.to_integer(hugeint)
end
