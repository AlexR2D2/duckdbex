defmodule Duckdbex do
  @moduledoc """
  DuckDB API module
  """

  @type db() :: reference()
  @type reason() :: :atom | binary()
  @type connection() :: reference()
  @type statement() :: reference()
  @type query_result() :: reference()
  @type appender :: reference()

  @doc """
  Opens database in the specified file.

  If specified file does not exist, a new database file with the given name will be created automatically.

  ## Examples
  ```
  iex> {:ok, _db} = Duckdbex.open("my_database.duckdb", %Duckdbex.Config{})
  ```
  """
  @spec open(binary(), Duckdbex.Config.t | nil) :: {:ok, db()} | {:error, reason()}
  def open(path, config) when is_binary(path),
    do: Duckdbex.NIF.open(path, config)

  @doc """
  If the path to the file is specified, then opens the database in the file.

  If specified file does not exist, a new database file with the given name will be created automatically.
  If database config is specified, then opens the database in the memory with the custom database config.

  ## Examples
  ```
  iex> {:ok, _db} = Duckdbex.open("my_database.duckdb")

  iex> {:ok, _db} = Duckdbex.open(%Duckdbex.Config{access_mode: :automatic})
  ```
  """
  @spec open(binary() | Duckdbex.Config.t) :: {:ok, db()} | {:error, reason()}
  def open(path) when is_binary(path),
    do: Duckdbex.NIF.open(path, nil)

  def open(%Duckdbex.Config{} = config),
    do: Duckdbex.NIF.open(":memory:", config)

  @doc """
  Opens database in the memory.

  ## Examples
  ```
  iex> {:ok, _db} = Duckdbex.open()
  ```
  """
  @spec open() :: {:ok, db()} | {:error, reason()}
  def open(),
    do: Duckdbex.NIF.open(":memory:", nil)

  @doc """
  Creates connection object to work with database.

  To work with database the connection object is requiered. Connection object hold a shared reference to database, so it is possible to forget the database reference and hold the connection reference only.

  ## Examples
  ```
  iex> {:ok, db} = Duckdbex.open()
  iex> {:ok, _conn} = Duckdbex.connection(db)
  ```
  """
  @spec connection(db()) :: {:ok, connection()} | {:error, reason()}
  def connection(db) when is_reference(db),
    do: Duckdbex.NIF.connection(db)

  @doc """
  Issues a query to the database and returns a result reference.

  ## Examples
  ```
  iex> {:ok, db} = Duckdbex.open()
  iex> {:ok, conn} = Duckdbex.connection(db)
  iex> {:ok, _res} = Duckdbex.query(conn, "SELECT 1;")
  ```
  """
  @spec query(connection(), binary()) :: {:ok, query_result()} | {:error, reason()}
  def query(connection, sql_string) when is_reference(connection) and is_binary(sql_string),
    do: Duckdbex.NIF.query(connection, sql_string)

  @doc """
  Issues a query to the database with parameters and returns a result reference.

  ## Examples
  ```
  iex> {:ok, db} = Duckdbex.open()
  iex> {:ok, conn} = Duckdbex.connection(db)
  iex> {:ok, _res} = Duckdbex.query(conn, "SELECT 1 WHERE $1 = 1;", [1])
  ```
  """
  @spec query(connection(), binary(), list()) :: {:ok, query_result()} | {:error, reason()}
  def query(connection, sql_string, args) when is_reference(connection) and is_binary(sql_string) and is_list(args),
    do: Duckdbex.NIF.query(connection, sql_string, args)

  @doc """
  Prepare the specified query, returning a reference to the prepared statement object

  ## Examples
  ```
  iex> {:ok, db} = Duckdbex.open()
  iex> {:ok, conn} = Duckdbex.connection(db)
  iex> {:ok, _stmt} = Duckdbex.prepare_statement(conn, "SELECT 1 WHERE $1 = 1;")
  ```
  """
  @spec prepare_statement(connection(), binary()) :: {:ok, statement()} | {:error, reason()}
  def prepare_statement(connection, sql_string) when is_reference(connection) and is_binary(sql_string),
    do: Duckdbex.NIF.prepare_statement(connection, sql_string)

  @doc """
  Execute the prepared statement

  ## Examples
  ```
  iex> {:ok, db} = Duckdbex.open()
  iex> {:ok, conn} = Duckdbex.connection(db)
  iex> {:ok, stmt} = Duckdbex.prepare_statement(conn, "SELECT 1;")
  iex> {:ok, res} = Duckdbex.execute_statement(stmt)
  iex> [[1]] = Duckdbex.fetch_all(res)
  ```
  """
  @spec execute_statement(statement()) :: {:ok, query_result()} | {:error, reason()}
  def execute_statement(statement) when is_reference(statement),
    do: Duckdbex.NIF.execute_statement(statement)

  @doc """
  Execute the prepared statement with the given list of parameters

  ## Examples
  ```
  iex> {:ok, db} = Duckdbex.open()
  iex> {:ok, conn} = Duckdbex.connection(db)
  iex> {:ok, stmt} = Duckdbex.prepare_statement(conn, "SELECT 1 WHERE $1 = 1;")
  iex> {:ok, res} = Duckdbex.execute_statement(stmt, [1])
  iex> [[1]] = Duckdbex.fetch_all(res)
  ```
  """
  @spec execute_statement(statement(), list()) :: {:ok, query_result()} | {:error, reason()}
  def execute_statement(statement, args) when is_reference(statement) and is_list(args),
    do: Duckdbex.NIF.execute_statement(statement, args)

  @doc """
  Fetches a data chunk from the query result.

  Returns empty list if there are no more results to fetch.

  ## Examples
  ```
  iex> {:ok, db} = Duckdbex.open()
  iex> {:ok, conn} = Duckdbex.connection(db)
  iex> {:ok, res} = Duckdbex.query(conn, "SELECT 1;")
  iex> [[1]] = Duckdbex.fetch_chunk(res)
  ```
  """
  @spec fetch_chunk(query_result()) :: :ok | {:error, reason()}
  def fetch_chunk(query_result) when is_reference(query_result),
    do: Duckdbex.NIF.fetch_chunk(query_result)


  @doc """
  Fetches all data from the query result.

  Returns empty list if there are no result to fetch.

  ## Examples
  ```
  iex> {:ok, db} = Duckdbex.open()
  iex> {:ok, conn} = Duckdbex.connection(db)
  iex> {:ok, res} = Duckdbex.query(conn, "SELECT 1;")
  iex> [[1]] = Duckdbex.fetch_all(res)
  ```
  """
  @spec fetch_all(query_result()) :: :ok | {:error, reason()}
  def fetch_all(query_result) when is_reference(query_result),
    do: Duckdbex.NIF.fetch_all(query_result)

  @doc """
  Creates the Appender to load bulk data into a DuckDB database.

  This is the recommended way to load bulk data.

  ## Examples
  ```
  iex> {:ok, db} = Duckdbex.open()
  iex> {:ok, conn} = Duckdbex.connection(db)
  iex> {:ok, _res} = Duckdbex.query(conn, "CREATE TABLE table_1 (data INTEGER);")
  iex> {:ok, _appender} = Duckdbex.appender(conn, "table_1")
  ```
  """
  @spec appender(connection(), binary()) :: {:ok, appender()} | {:error, reason()}
  def appender(connection, table_name) when is_reference(connection) and is_binary(table_name),
    do: Duckdbex.NIF.appender(connection, table_name)

  @doc """
  Append row into a DuckDB database table.

  Any values added to the appender are cached prior to being inserted into the database system for performance reasons. That means that, while appending, the rows might not be immediately visible in the system. The cache is automatically flushed when the appender goes out of scope or when Duckdbex.appender_close(appender) is called. The cache can also be manually flushed using the Duckdbex.appender_flush(appender) method. After either flush or close is called, all the data has been written to the database system.

  ## Examples
  ```
  iex> {:ok, db} = Duckdbex.open()
  iex> {:ok, conn} = Duckdbex.connection(db)
  iex> {:ok, _res} = Duckdbex.query(conn, "CREATE TABLE table_1 (data INTEGER);")
  iex> {:ok, appender} = Duckdbex.appender(conn, "table_1")
  iex> :ok = Duckdbex.appender_add_row(appender, [1])
  ```
  """
  @spec appender_add_row(appender(), list()) :: :ok | {:error, reason()}
  def appender_add_row(appender, row) when is_reference(appender) and is_list(row),
    do: Duckdbex.NIF.appender_add_row(appender, row)

  @doc """
  Append multiple rows into a DuckDB database table at once.

  Any values added to the appender are cached prior to being inserted into the database system for performance reasons. That means that, while appending, the rows might not be immediately visible in the system. The cache is automatically flushed when the appender goes out of scope or when `Duckdbex.appender_close/1` is called. The cache can also be manually flushed using the `Duckdbex.appender_flush/1` method. After either flush or close is called, all the data has been written to the database system.

  ## Examples
  ```
  iex> {:ok, db} = Duckdbex.open()
  iex> {:ok, conn} = Duckdbex.connection(db)
  iex> {:ok, _res} = Duckdbex.query(conn, "CREATE TABLE table_1 (the_n1 INTEGER, the_str1 STRING);")
  iex> {:ok, appender} = Duckdbex.appender(conn, "table_1")
  iex> :ok = Duckdbex.appender_add_rows(appender, [[1, "one"], [2, "two"]])
  ```
  """
  @spec appender_add_rows(appender(), list(list())) :: :ok | {:error, reason()}
  def appender_add_rows(appender, rows) when is_reference(appender) and is_list(rows),
    do: Duckdbex.NIF.appender_add_rows(appender, rows)

  @doc """
  Commit the changes made by the appender.

  ## Examples
  ```
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
  ```
  """
  @spec appender_flush(appender()) :: :ok | {:error, reason()}
  def appender_flush(appender) when is_reference(appender),
    do: Duckdbex.NIF.appender_flush(appender)

  @doc """
  Flush the changes made by the appender and close it.

  The appender cannot be used after this point

  ## Examples
  ```
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
  ```
  """
  @spec appender_flush(appender()) :: :ok | {:error, reason()}
  def appender_close(appender) when is_reference(appender),
    do: Duckdbex.NIF.appender_close(appender)

  @doc """
  Convert an erlang/elixir integer to a DuckDB hugeint.

  For more information on DuckDB numeric types, see [DuckDB Numeric Data Types](https://duckdb.org/docs/sql/data_types/numeric) For more information on DuckDB numeric types.

  ## Examples
  ```
  iex> {:ok, db} = Duckdbex.open()
  iex> {:ok, conn} = Duckdbex.connection(db)
  iex> {:ok, _res} = Duckdbex.query(conn, "CREATE TABLE hugeints(value HUGEINT);")
  iex> {:ok, _res} = Duckdbex.query(conn, "INSERT INTO hugeints VALUES (98233720368547758080000::hugeint);")
  iex> hugeint = Duckdbex.integer_to_hugeint(98233720368547758080000)
  iex> {:ok, res} = Duckdbex.query(conn, "SELECT * FROM hugeints WHERE value = $1", [{:hugeint, hugeint}])
  iex> [[{5325, 4808176044395724800}]] = Duckdbex.fetch_all(res)
  ```
  """
  def integer_to_hugeint(integer) when is_integer(integer) do
    {:erlang.bsr(integer, 64), :erlang.band(integer, 0xFFFFFFFFFFFFFFFF)}
  end

  @doc """
  Convert a duckdb hugeint record to erlang/elixir integer.

  ## Examples
  ```
  iex> {:ok, db} = Duckdbex.open()
  iex> {:ok, conn} = Duckdbex.connection(db)
  iex> {:ok, _res} = Duckdbex.query(conn, "CREATE TABLE hugeints(value HUGEINT);")
  iex> {:ok, _res} = Duckdbex.query(conn, "INSERT INTO hugeints VALUES (98233720368547758080000::hugeint);")
  iex> {:ok, res} = Duckdbex.query(conn, "SELECT * FROM hugeints;")
  iex> [[hugeint = {5325, 4808176044395724800}]] = Duckdbex.fetch_all(res)
  iex> 98233720368547758080000 = Duckdbex.hugeint_to_integer(hugeint)
  ```
  """
  def hugeint_to_integer({upper, lower}) when is_integer(upper) and is_integer(lower) and lower >= 0 do
    upper |> :erlang.bsl(64) |> :erlang.bor(lower)
  end
end
