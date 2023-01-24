# Duckdbex

The Library embeds C++ [DuckDB](https://duckdb.org) database into you application.

DuckDB is an in-process SQL database management system designed to support analytical query workloads, also known as Online analytical processing (OLAP). It has no external dependencies, neither for compilation nor during run-time and completely embedded (like Sqlite) within a host (BEAM in our case) process.

To find out where and why you could use DuckDB, please, see [why DuckDB](https://duckdb.org/why_duckdb) section of DuckDB docs.

Also, you may find useful the DuckDB [documentation](https://duckdb.org/docs/sql/introduction).

The library uses `amalgamation` of the DuckDB sources, which combine all sources into two files duckdb.hpp and duckdb.cpp. This is the standard source distribution of libduckdb. `Amalgamation` allows to work with [CSV](https://duckdb.org/docs/data/csv) and [Parquet](https://duckdb.org/docs/data/parquet) but it does not yet include DuckDB extensions like [JSON](https://duckdb.org/docs/extensions/json), [Full Text Search](https://duckdb.org/docs/extensions/full_text_search), [HTTPFS](https://duckdb.org/docs/extensions/httpfs), [SQLite Scanner](https://duckdb.org/docs/extensions/sqlite_scanner), [Postgres Scanner](https://duckdb.org/docs/extensions/postgres_scanner) and [Substrait](https://duckdb.org/docs/extensions/substrait).

All NIF functions implemented as [**Dirty NIF**](https://www.erlang.org/doc/man/erl_nif.html)

**DuckDB is in the active development phase, and new version of library may not open the database created by the old version.**

[HexDocs](https://hexdocs.pm/duckdbex/)

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `duckdbex` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:duckdbex, "~> 0.1.0"}
  ]
end
```

# Usage

Using the `duckdbex` is quite simple. You need to open database file, create connection, run a query and fetch the results. [livebook](https://github.com/AlexR2D2/duckdbex/blob/main/duckdbex_sandbox.livemd)

## Open database

To open the DuckDB database, you must specify the path to the DuckDB file

```elixir
# Open an existing database file
{:ok, db} = Duckdbex.open("exis_movies.duckdb")
```

If specified database file does not exist, a new database file with the given name will be created automatically.

```elixir
# If the specified file does not exist the new database will be created
{:ok, db} = Duckdbex.open("not_yet_exist_my_new_duckdb_database")
```

If you do not specify any database file the database will be created in the memory. Note that for an in-memory database no data is persisted to disk (i.e. all data is lost when you exit the app or database|connection object out of scope).

```elixir
# Just create database in the memory
{:ok, db} = Duckdbex.open()
```

The DuckDB has a number of different options and could be configured by passing the `Duckdbex.Config` struct into the `Duckdbex.open/1` or `Duckdbex.open/2`.

```elixir
{:ok, _db} = Duckdbex.open(%Duckdbex.Config{checkpoint_wal_size: 8388608})
```

## Create Connection

With the DuckDB instance, you can create one or many Connection instances using the `Duckdbex.connection/1` function. In fact, the DuckDB connection is the native OS thread (not a lightweight Elixir process). While individual connections are thread-safe, they will be locked during querying. So, it is recommended to use the different DuckDB connections in the different Elixir processees to allow for the best parallel performance.

```elixir
{:ok, db} = Duckdbex.open()

# Create a connection to the opened database
{:ok, conn} = Duckdbex.connection(db)
```

## Make a Query

To make a query you need call `Duckdbex.query/2` passing connection reference and query string. Query parameters can be passed as list into `Duckdbex.query/3`. The query call will return the reference to the result.

```elixir
{:ok, db} = Duckdbex.open()
{:ok, conn} = Duckdbex.connection(db)

# Run a query and get the reference to the result
{:ok, result_ref} = Duckdbex.query(conn, "SELECT 1;")

# Run a query with parameters
{:ok, result_ref} = Duckdbex.query(conn, "SELECT 1 WHERE $1 = 1;", [1])
```

## Fetch Result

To get data from result you need pass result reference from the `Duckdbex.query/2` or `Duckdbex.query/3` call into the `Duckdbex.fetch_all/1` or `Duckdbex.fetch_chunk/1`. The `Duckdbex.fetch_all/1` will return all the data at once. To get data chunk by chunk you should call `Duckdbex.fetch_chunk/1`.

```elixir
# Run a query and get the reference to the result
{:ok, result_ref} = Duckdbex.query(conn, """
  SELECT userId, movieId, rating FROM ratings WHERE userId = $1;
""", [1])

# Get all the data from the result reference at once
Duckdbex.fetch_all(result_ref)
# => [[userId: 1, movieId: 1, rating: 6], [userId: 1, movieId: 2, rating: 12]]
```

or by chunks

```elixir
# Run a query and get the reference to the result
{:ok, result_ref} = Duckdbex.query(conn, "SELECT * FROM ratings;")

# fetch chunk
Duckdbex.fetch_chunk(result_ref)
# => [[userId: 1, movieId: 1, rating: 6], [userId: 1, movieId: 2, rating: 12]...]

# fetch next chunk
Duckdbex.fetch_chunk(result_ref)
# => [<rows>]

...

# the data is over and fetch_chunk returns the empty list
Duckdbex.fetch_chunk(result_ref)
# => []
```

## Prepared statement

Prepared statement speeding up queries that will be executed many times with different parameters. Also it allows to avoid string concatenation/SQL injection attacks.

```elixir
# Prepare statement
{:ok, stmt_ref} = Duckdbex.prepare_statement(conn, "SELECT 1;")

# Execute statement
{:ok, result_ref} = Duckdbex.execute_statement(stmt_ref)

# Fetch result
Duckdbex.fetch_all(result_ref)
# => [[1]]
```

or with parameters

```elixir
# Prepare statement
{:ok, stmt_ref} = Duckdbex.prepare_statement(conn, "SELECT * FROM ratings WHERE userId = $1;")

# Execute statement
{:ok, result_ref} = Duckdbex.execute_statement(stmt_ref, [1])
# fetch result ...

# Execute statement
{:ok, result_ref} = Duckdbex.execute_statement(stmt_ref, [42])
# fetch result ...
```

## Importing Data

DuckDB provides several methods that allows you to easily and efficiently insert data to the database.

* [Insert Statements](#insert-statements)
* [CSV Files](#csv-files)
* [Parquet Files](#parquet-files)
* [Appender](#appender)

### Insert Statements

This is standart way of inserting data into relational database, but DuckDB **is not recommended to use this method if you are inserting more than a few records**. See [details](https://duckdb.org/docs/data/insert). To insert bulk data into database, please, use [Appender](#appender).

```elixir
{:ok, db} = Duckdbex.open()
{:ok, conn} = Duckdbex.connection(db)
{:ok, _res} = Duckdbex.query(conn, "CREATE TABLE people(id INTEGER, name VARCHAR);")

{:ok, _res} = Duckdbex.query(conn, "INSERT INTO people VALUES (1, 'Mark'), (2, 'Hannes');")
```

A more detailed description together with syntax diagram can be found [here](https://duckdb.org/docs/sql/statements/insert).

### CSV Files

DuckDB has an embedded CSV reader that allows to load CSV files directly into database (escaping data transfering from Elixir to NIF, so ERTS doesn't involved in this). Also, DuckDB supports **compressed** CSV files, e.g. a gzipped file like `my_csv_file.csv.gz` and etc.

For example we have a `test.csv` CSV file:
```csv
FlightDate|UniqueCarrier|OriginCityName|DestCityName
1988-01-01|AA|New York, NY|Los Angeles, CA
1988-01-02|AA|New York, NY|Los Angeles, CA
1988-01-03|AA|New York, NY|Los Angeles, CA
```

Let's load this file into database:

```elixir
{:ok, db} = Duckdbex.open()
{:ok, conn} = Duckdbex.connection(db)

# read a CSV file from disk, auto-infer options
{:ok, res} = Duckdbex.query conn, "SELECT * FROM 'test.csv';"

Duckdbex.fetch_all(res)
# will result in
[
  [{1988, 1, 1}, "AA", "New York, NY", "Los Angeles, CA"],
  [{1988, 1, 2}, "AA", "New York, NY", "Los Angeles, CA"],
  [{1988, 1, 3}, "AA", "New York, NY", "Los Angeles, CA"]
]
```

or we could export data directly into CSV (escaping Elixir)

```elixir
Duckdbex.query conn, "CREATE TABLE ontime AS SELECT * FROM 'test.csv';"

# write the result of a query to a CSV file
Duckdbex.query conn, "COPY (SELECT * FROM ontime) TO 'test.csv' WITH (HEADER 1, DELIMITER '|');"
```

see more examples [here](https://duckdb.org/docs/data/csv)

### Parquet Files

[Parquet](https://parquet.apache.org) is an open source, column-oriented data file format designed for efficient data storage and retrieval. It provides efficient data compression and encoding schemes with enhanced performance to handle complex data in bulk. DuckDB has a built-in Parquet reader. Like CSV reader it works directly with the files escaping ERTS.

```elixir
# read a single parquet file
Duckdbex.query conn, "SELECT * FROM 'test.parquet';"

# figure out which columns/types are in a parquet file
Duckdbex.query conn, "DESCRIBE SELECT * FROM 'test.parquet';"

# read all files that match the glob pattern
Duckdbex.query conn, "SELECT * FROM 'test/*.parquet';"

# export the table contents of the entire database as parquet
Duckdbex.query conn, "EXPORT DATABASE 'target_directory' (FORMAT PARQUET);"
```
you can find more examples [here](https://duckdb.org/docs/data/parquet)

### Appender

Appender can be used to load bulk data into a DuckDB database.

```elixir
{:ok, db} = Duckdbex.open()
{:ok, conn} = Duckdbex.connection(db)

Duckdbex.query(conn, "CREATE TABLE people(id INTEGER, name VARCHAR);")

# Create Appender for table 'people'
{:ok, appender} = Duckdbex.appender conn, "people"

# Append two rows at once.
Duckdbex.appender_add_rows appender, [[2, "Sarah"], [3, "Alex"]]

# Persist cached rows
Duckdbex.appender_flush()
```

Any values added to the appender are cached prior to being inserted into the database system for performance reasons. That means that, while appending, the rows might not be immediately visible in the system. The cache is automatically flushed when the appender goes out of scope or when `Duckdbex.appender_close/1` is called. The cache can also be manually flushed using the `Duckdbex.appender_flush/1` method. After either flush or close is called, all the data has been written to the database system.

## Data Types

Along with [General-Purpose Data types](https://duckdb.org/docs/sql/data_types/overview) DuckDB supports three nested data types: [LIST](https://duckdb.org/docs/sql/data_types/list), [STRUCT](https://duckdb.org/docs/sql/data_types/struct) and [MAP](https://duckdb.org/docs/sql/data_types/map). There is also [UNION](https://duckdb.org/docs/sql/data_types/union) data type what implemented on top of STRUCT types, and simply keep the “tag” as the first entry. Each supports different use cases and has a different structure.

**You can find more examples about how to work with different types [here](https://github.com/AlexR2D2/duckdbex/test/nif/types_test.exs).**

### LIST

An ordered sequence of data values of the same type.
Each row must have the same data type within each LIST, but can have any number of elements.

```elixir
# Just select a list
{:ok, r} = Duckdbex.query(conn, "SELECT [1, 2, 3];")
[[[1, 2, 3]]] = Duckdbex.fetch_all(r)

# Table with columns of LIST type
Duckdbex.query(conn, "CREATE TABLE list_table (int_list INT[], varchar_list VARCHAR[]);")
Duckdbex.query(conn, "INSERT INTO list_table VALUES ([1, 2], ['one', 'two']), ([4, 5], ['three', NULL]), ([6, 7], NULL);")
{:ok, r} = Duckdbex.query(conn, "SELECT * FROM list_table")
[[[1, 2], ["one", "two"]], [[4, 5], ["three", nil]], [[6, 7], nil]] = Duckdbex.fetch_all(r)
```

### STRUCT

Conceptually, a STRUCT column contains an ordered list of other columns called “entries”. The entries are referenced by name using strings. This document refers to those entry names as keys. Each row in the STRUCT column must have the same keys. Each key must have the same type of value for each row. See more [here](https://duckdb.org/docs/sql/data_types/struct).

```elixir
{:ok, r} = Duckdbex.query(conn, "SELECT {'x': 1, 'y': 2, 'z': 3};")
[[%{"x" => 1, "y" => 2, "z" => 3}]] = Duckdbex.fetch_all(r)
```

### MAP

MAPs are similar to STRUCTs in that they are an ordered list of “entries” where a key maps to a value. However, MAPs do not need to have the same keys present on each row, and thus open additional use cases. MAPs are useful when the schema is unknown beforehand, and when adding or removing keys in subsequent rows. Their flexibility is a key differentiator.
See more [here](https://duckdb.org/docs/sql/data_types/map).

```elixir
Duckdbex.query(conn, "CREATE TABLE map_table (map_col MAP(INT, DOUBLE));")
Duckdbex.query(conn, "INSERT INTO map_table VALUES (map([1, 2], [2.98, 3.14])), (map([3, 4], [9.8, 1.6]));")

{:ok, r} = Duckdbex.query(conn, "SELECT * FROM map_table")
[[%{1 => 2.98, 2 => 3.14}], [%{3 => 9.8, 4 => 1.6}]] = Duckdbex.fetch_all(r)
```

### UNION

A UNION type (not to be confused with the SQL UNION operator) is a nested type capable of holding one of multiple “alternative” values, much like the union in C. The main difference being that these UNION types are tagged unions and thus always carry a discriminator “tag” which signals which alternative it is currently holding, even if the inner value itself is null. See more [here](https://duckdb.org/docs/sql/data_types/union).


```elixir
{:ok, _} = Duckdbex.query(conn, "CREATE TABLE tbl1(u UNION(num INT, str VARCHAR));")
{:ok, _} = Duckdbex.query(conn, "INSERT INTO tbl1 values (1) , ('two') , (union_value(str := 'three'));")
{:ok, r} = Duckdbex.query(conn, "SELECT u from tbl1;")
[[%{"num" => 1}], [%{"str" => "two"}], [%{"str" => "three"}]] = Duckdbex.fetch_all(r)
```

### ENUM

The ENUM type represents a dictionary data structure with all possible unique values of a column. For example, a column storing the days of the week can be an Enum holding all possible days. Enums are particularly interesting for string columns with high cardinality. This is because the column only stores a numerical reference to the string in the Enum dictionary, resulting in immense savings in disk storage and faster query performance. See more [here](https://duckdb.org/docs/sql/data_types/enum).

```elixir
{:ok, _} = Duckdbex.query(conn, "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');")

{:ok, _} = Duckdbex.query(conn, "CREATE TABLE person (name text, current_mood mood);")
{:ok, _} = Duckdbex.query(conn, "INSERT INTO person VALUES ('Pedro','happy'), ('Mark', NULL), ('Pagliacci', 'sad'), ackey', 'ok');")

{:ok, r} = Duckdbex.query(conn, "SELECT * FROM person WHERE current_mood = 'sad';")
[["Pagliacci", "sad"]] = Duckdbex.fetch_all(r)

{:ok, r} = Duckdbex.query(conn, "SELECT * FROM person WHERE current_mood = $1;", ["sad"])
[["Pagliacci", "sad"]] = Duckdbex.fetch_all(r)
```

Documentation generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
