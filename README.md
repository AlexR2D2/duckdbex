# Duckdbex

This is an **Elixir** Library that embeds C++ [DuckDB](https://duckdb.org) database into your **Elixir** application.

DuckDB is an in-process SQL database management system designed to support analytical query workloads, also known as Online analytical processing (OLAP). It has no external dependencies, neither for compilation nor during run-time and completely embedded (like Sqlite) within a host (BEAM in our case) process.

To find out where and why you could use DuckDB, please see [why DuckDB](https://duckdb.org/why_duckdb) section of DuckDB docs.

Also, you may find useful the DuckDB [documentation](https://duckdb.org/docs/sql/introduction).

The library vendors the DuckDB C++ sources, generated from the upstream [duckdb/duckdb](https://github.com/duckdb/duckdb) repository by DuckDB's own `scripts/package_build.py` — see [`bin/regen_duckdb.sh`](bin/regen_duckdb.sh) and [`c_src/duckdb/PROVENANCE.md`](c_src/duckdb/PROVENANCE.md) for the exact version, source id and regeneration workflow. The generated tree is not committed; `mix compile` regenerates it when `c_src/duckdb.tar.gz` is missing (requires `gh`, `python3` and `make`). The build statically links `core_functions`, `parquet`, `json` and `icu` (the upstream build baseline), so functions like `now()` / `version()`, Parquet import/export, JSON functions and timezone/collation support work out of the box. Other extensions like [Full Text Search](https://duckdb.org/docs/extensions/full_text_search), [HTTPFS](https://duckdb.org/docs/extensions/httpfs), [SQLite Scanner](https://duckdb.org/docs/extensions/sqlite_scanner), [Postgres Scanner](https://duckdb.org/docs/extensions/postgres_scanner) and [Substrait](https://duckdb.org/docs/extensions/substrait) are not bundled.

All NIF functions implemented as [**Dirty NIF**](https://www.erlang.org/doc/man/erl_nif.html)

Online [HexDocs](https://hexdocs.pm/duckdbex/)

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `duckdbex` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:duckdbex, "~> 0.6.0"}
  ]
end
```

The Hex package ships precompiled NIFs. Building from a checkout compiles DuckDB from source; the first build regenerates the vendored sources and takes several minutes.

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

DuckDB has a number of configuration options. In Duckdbex you can create a config reference, set options on it, and then pass that config into `Duckdbex.open/1` or `Duckdbex.open/2`.

```elixir
{:ok, config} = Duckdbex.create_config()
:ok = Duckdbex.set_config_option(config, "checkpoint_threshold", "8MB")
{:ok, _db} = Duckdbex.open(config)
```

You can inspect the list of options reported by DuckDB:

```elixir
Duckdbex.get_config_options()
# => [
#      %{
#        name: "threads",
#        type: :bigint,
#        parameter_type: "BIGINT",
#        description: "...",
#        scope: :global_default,
#        default: 1
#      },
#      ...
#    ]
```

Duckdbex does not maintain a separate allowlist or denylist of config options. `Duckdbex.get_config_options/0` returns the options reported by DuckDB itself, and if some option cannot be used through `Duckdbex.create_config/0`, DuckDB will return the error when you call `Duckdbex.set_config_option/3` or open the database with that config.

## Create Connection

With the DuckDB instance, you can create one or many Connection instances using the `Duckdbex.connection/1` function. In fact, the DuckDB connection is the native OS thread (not a lightweight Elixir process). While individual connections are thread-safe, they will be locked during querying. So, it is recommended to use the different DuckDB connections in the different Elixir processes to allow for the best parallel performance.

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

## Closing connection, database and releasing resources

All opened database/connecions/results refs will be closed/released automatically as soon as the ref for an object (db, conn, result_ref) will be thrown away. For example:

Lets open the database:

```elixir
# Open an existing database file
{:ok, db} = Duckdbex.open("exis_movies.duckdb")
#Reference<0.1076596279.3008626690.232411>
```

Now the `db` holds the reference `#Reference<0.1076596279.3008626690.232411>` to the underlying database object. If you throw away the 'db' ref to the underlying database object the database will be closed automatically. Lets simulate this via 'assignment' to `db` some stub value


```elixir
# throw away the ref to the database
db = "forcing closing the database"
```

Now the `db` holds the `"forcing closing the database"` binary and there is no any 'variable' in out code what holds the ref `#Reference<0.1076596279.3008626690.232411>` to the database. So, technically speaking, ref count to underlying database object is 0. Erlang automatically calls the destructor for database object and it will be closed correctly. So, if, for example, you holds the db ref in GenServer state the db will be closed automatically if GenServer will be terminated/crashed. You don't need to call some function to close database.

But what if you need to close the database/connection/result_ref explicitly, for example, you want to close the database (flush all underlying db buffers to disk) and when archive the db file. To prevent using the strange code like `db = "forcing closing the database"` there is `Duckdbex.release(resource)` function to explicitly closing any underlying DuckDB resource:

```elixir
iex> {:ok, db} = Duckdbex.open("my_database.duckdb")
iex> {:ok, conn} = Duckdbex.connection(db)
iex> {:ok, res} = Duckdbex.query(conn, "SELECT 1 WHERE $1 = 1;", [1])
iex> :ok = Duckdbex.release(res)
iex> :ok = Duckdbex.release(conn)
iex> :ok = Duckdbex.release(db)
```

Now, all are explicitly closed.

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

### Named parameters

DuckDB 2.0 supports named parameters using the `$name` syntax. Pass them as a map, keyed by
parameter name. Names are matched case-insensitively.

```elixir
{:ok, r} = Duckdbex.query(conn, "SELECT $name::VARCHAR;", %{"name" => "world"})
[["world"]] = Duckdbex.fetch_all(r)

# prepared statements work the same way
{:ok, stmt_ref} = Duckdbex.prepare_statement(conn, "SELECT $value::INTEGER;")
[{"value", :integer}] = Duckdbex.get_named_parameters(stmt_ref)
{:ok, r} = Duckdbex.execute_statement(stmt_ref, %{"value" => 1})
```

## Importing Data

DuckDB provides several methods that allows you to easily and efficiently insert data to the database.

* [Insert Statements](#insert-statements)
* [CSV Files](#csv-files)
* [Parquet Files](#parquet-files)
* [Appender](#appender)

### Insert Statements

This is standard way of inserting data into relational database, but DuckDB **is not recommended to use this method if you are inserting more than a few records**. See [details](https://duckdb.org/docs/data/insert). To insert bulk data into database, please, use [Appender](#appender).

```elixir
{:ok, db} = Duckdbex.open()
{:ok, conn} = Duckdbex.connection(db)
{:ok, _res} = Duckdbex.query(conn, "CREATE TABLE people(id INTEGER, name VARCHAR);")

{:ok, _res} = Duckdbex.query(conn, "INSERT INTO people VALUES (1, 'Mark'), (2, 'Hannes');")
```

A more detailed description together with syntax diagram can be found [here](https://duckdb.org/docs/sql/statements/insert).

### CSV Files

DuckDB has an embedded CSV reader that allows to load CSV files directly into database (escaping data transferring from Elixir to NIF, so ERTS doesn't involved in this). Also, DuckDB supports **compressed** CSV files, e.g. a gzipped file like `my_csv_file.csv.gz`, etc.

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
# download parquet extension (the .so/.dll extension will be downloaded from the remote source)
Duckdbex.query conn, "INSTALL 'parquet';"

# load parquet extension into the app
Duckdbex.query conn, "LOAD 'parquet';"

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

Appender can be used to load bulk data into a DuckDB database. It uses the connection
that created it and does not keep that connection alive. Keep the connection open
and reachable until `Duckdbex.appender_close/1` succeeds.

An Elixir variable being in scope is not enough: after its last use, it may no
longer keep the resource alive. In this example, `Duckdbex.release(conn)` after
closing the appender keeps `conn` reachable throughout the appender operations:

```elixir
{:ok, db} = Duckdbex.open()
{:ok, conn} = Duckdbex.connection(db)
{:ok, _} = Duckdbex.query(conn, "CREATE TABLE people(id INTEGER, name VARCHAR);")

{:ok, appender} = Duckdbex.appender(conn, "people")
:ok = Duckdbex.appender_add_rows(appender, [[2, "Sarah"], [3, "Alex"]])

# Flush buffered rows and close the appender before releasing its connection.
:ok = Duckdbex.appender_close(appender)
:ok = Duckdbex.release(conn)
```

Rows are buffered and may not be visible immediately. Use `Duckdbex.appender_flush/1`
to flush them while keeping the appender open, or `Duckdbex.appender_close/1` when
finished. Both return `:ok` on success or `{:error, reason}` on failure; check the
result before treating buffered rows as written.

If the connection is released or garbage-collected, flushing buffered rows fails
with `{:error, reason}`. Releasing the appender or relying on garbage collection
does not guarantee a successful flush. If your application reuses the connection,
keep it reachable for subsequent operations and close the appender before releasing it.

## Data Types

Along with [General-Purpose Data types](https://duckdb.org/docs/sql/data_types/overview) DuckDB supports several nested data types: [LIST](https://duckdb.org/docs/sql/data_types/list), [STRUCT](https://duckdb.org/docs/sql/data_types/struct), TUPLE, [MAP](https://duckdb.org/docs/sql/data_types/map) and [VARIANT](https://duckdb.org/docs/2.0/sql/data_types/variant.html). There is also [UNION](https://duckdb.org/docs/sql/data_types/union) data type what implemented on top of STRUCT types, and simply keep the “tag” as the first entry. [GEOMETRY](https://duckdb.org/docs/2.0/sql/data_types/geometry.html) values are exposed as WKB binaries. Each supports different use cases and has a different structure.

**You can find more examples about how to work with different types [here](https://github.com/AlexR2D2/duckdbex/test/types_test.exs).**

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
[[[{1, 2.98}, {2, 3.14}]], [[{3, 9.8}, {4, 1.6}]]] = Duckdbex.fetch_all(r)
```

MAPs are returned as a list of `{key, value}` pairs rather than an Elixir map:
DuckDB keeps the entry order and compares maps by it
(`MAP([1,2],[10,20]) = MAP([2,1],[20,10])` is false), while Elixir maps are
unordered. Use `Map.new/1` when you only need lookups.

### TUPLE

A TUPLE is an unnamed STRUCT: its fields are positional and have a fixed arity, so it maps to an Elixir tuple. Parameters accept either a tuple or a list.

```elixir
{:ok, r} = Duckdbex.query(conn, "SELECT (1, 'a')::TUPLE(INTEGER, VARCHAR);")
[[{1, "a"}]] = Duckdbex.fetch_all(r)

{:ok, r} = Duckdbex.query(conn, "SELECT $1::TUPLE(INTEGER, VARCHAR);", [{1, "a"}])
[[{1, "a"}]] = Duckdbex.fetch_all(r)
```

### UNION

A UNION type (not to be confused with the SQL UNION operator) is a nested type capable of holding one of multiple “alternative” values, much like the union in C. The main difference being that these UNION types are tagged unions and thus always carry a discriminator “tag” which signals which alternative it is currently holding, even if the inner value itself is null. See more [here](https://duckdb.org/docs/sql/data_types/union).


```elixir
{:ok, _} = Duckdbex.query(conn, "CREATE TABLE tbl1(u UNION(num INT, str VARCHAR));")
{:ok, _} = Duckdbex.query(conn, "INSERT INTO tbl1 values (1) , ('two') , (union_value(str := 'three'));")
{:ok, r} = Duckdbex.query(conn, "SELECT u from tbl1;")
[[{"num", 1}], [{"str", "two"}], [{"str", "three"}]] = Duckdbex.fetch_all(r)
```

### VARIANT

VARIANT holds a value of any type and maps to the corresponding Elixir value. Scalars (`nil`, booleans, numbers and strings), lists and maps can also be passed as parameters.
See more [here](https://duckdb.org/docs/2.0/sql/data_types/variant.html).

```elixir
{:ok, r} = Duckdbex.query(conn, "SELECT CAST([1, 2] AS VARIANT);")
[[[1, 2]]] = Duckdbex.fetch_all(r)

{:ok, r} = Duckdbex.query(conn, "SELECT $1::VARIANT;", [%{"a" => [1, 2]}])
[[%{"a" => [1, 2]}]] = Duckdbex.fetch_all(r)
```

### GEOMETRY

GEOMETRY values are passed through as WKB binaries, as query results and as parameters alike.
See more [here](https://duckdb.org/docs/2.0/sql/data_types/geometry.html).

```elixir
# WKB for POINT(1 2)
wkb = <<1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64>>

{:ok, r} = Duckdbex.query(conn, "SELECT $1::GEOMETRY;", [wkb])
[[^wkb]] = Duckdbex.fetch_all(r)
```

### ENUM

The ENUM type represents a dictionary data structure with all possible unique values of a column. For example, a column storing the days of the week can be an Enum holding all possible days. Enums are particularly interesting for string columns with high cardinality. This is because the column only stores a numerical reference to the string in the Enum dictionary, resulting in immense savings in disk storage and faster query performance. See more [here](https://duckdb.org/docs/sql/data_types/enum).

```elixir
{:ok, _} = Duckdbex.query(conn, "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');")

{:ok, _} = Duckdbex.query(conn, "CREATE TABLE person (name text, current_mood mood);")
{:ok, _} = Duckdbex.query(conn, "INSERT INTO person VALUES ('Pedro','happy'), ('Mark', NULL), ('Pagliacci', 'sad'), ('ackey', 'ok');")

{:ok, r} = Duckdbex.query(conn, "SELECT * FROM person WHERE current_mood = 'sad';")
[["Pagliacci", "sad"]] = Duckdbex.fetch_all(r)

{:ok, r} = Duckdbex.query(conn, "SELECT * FROM person WHERE current_mood = $1;", ["sad"])
[["Pagliacci", "sad"]] = Duckdbex.fetch_all(r)
```

## Extensions

DuckDB allows to load and use extensions. By default extensions are downloaded from the remote source. These extensions are verified and signed. Also you can use you own unsigned extensions. By default, extensions are installed under the user's home directory:
```
~/.duckdb/extensions/duckdb_version/platform_name/
```

To change the default location where DuckDB stores its extensions, use the extension_directory configuration option:
```
SET extension_directory = '/path/to/your/extension/directory';
```

To change the extensions directory in Duckdbex, configure DuckDB directly through `Duckdbex.create_config/0` and `Duckdbex.set_config_option/3`, or run `SET extension_directory = ...` after opening a connection.

### Remote installation

Download and install verified signed extension from remote source

```elixir
# download extension binary
{:ok, _} = Duckdbex.query(conn, "INSTALL 'parquet';")

# load extension into the app
{:ok, _} = Duckdbex.query(conn, "LOAD 'parquet';")

# now you can use the extension
{_, r} = Duckdbex.query(conn, "SELECT * FROM 'test.parquet';")
```

### Local installation (unsigned extension)

Almost the same as above, but you must configure DuckDB to allow unsigned extension

```elixir
# configure DuckDB to allow unsigned extensions
{:ok, config} = Duckdbex.create_config()
:ok = Duckdbex.set_config_option(config, "allow_unsigned_extensions", true)
{:ok, db} = Duckdbex.open(config)
{:ok, conn} = Duckdbex.connection(db)

# install unsigned extension from local source
{:ok, _} = Duckdbex.query(conn, "INSTALL '/home/extensions/my_custom.duckdb_extension';")
{:ok, _} = Duckdbex.query(conn, "LOAD '/home/extensions/my_custom.duckdb_extension';")
```

### Custom build DuckDB extensions

If extension what downloaded from the remote source doesn't load because of mismatch of compiler/syslibs and etc., you can build DuckDB extensions by youself and use it as unsigned extensions. You need cmake installed.

```shell
$ git clone https://github.com/duckdb/duckdb.git
duckdb$ cd duckdb
duckdb$ BUILD_JSON=1 make
duckdb$ cp build/release/extension/json/json.duckdb_extension /home/duckdb_unsigned_extensions
```
Now we can use this extension

```elixir
{:ok, config} = Duckdbex.create_config()
:ok = Duckdbex.set_config_option(config, "allow_unsigned_extensions", true)
{:ok, db} = Duckdbex.open(config)
{:ok, conn} = Duckdbex.connection(db)

# load extension into the app
{:ok, _} = Duckdbex.query(conn, "INSTALL '/home/duckdb_unsigned_extensions/json.duckdb_extension';")
{:ok, _} = Duckdbex.query(conn, "LOAD '/home/duckdb_unsigned_extensions/json.duckdb_extension';")

# use the extension
{:ok, _} = Duckdbex.query(conn, "SELECT * FROM read_json_objects('some_data.json');")
...
```

Documentation generated with [ExDoc](https://github.com/elixir-lang/ex_doc)

## Elixir representation of DuckDB types

Scalar DuckDB types map to Elixir's built-in types where they exist, and to
`Duckdbex.*` structs where Elixir has no equivalent.

| DuckDB | Elixir |
| --- | --- |
| `DATE` | `Date` |
| `TIME` | `Time` |
| `TIMESTAMP` | `NaiveDateTime` |
| `TIMESTAMPTZ` | `DateTime` (UTC) |
| `TIMESTAMP_S` / `_MS` | `NaiveDateTime` |
| `TIMESTAMP_NS` | `%Duckdbex.TimestampNs{datetime: NaiveDateTime, nanosecond: 0..999}` |
| `TIMESTAMPTZ_NS` | `%Duckdbex.TimestampTzNs{datetime: DateTime, nanosecond: 0..999}` |
| `TIMETZ` | `%Duckdbex.TimeTz{time: Time, offset: seconds}` |
| `INTERVAL` | `%Duckdbex.Interval{months, days, micros}` |
| `DECIMAL` | `%Duckdbex.Decimal{unscaled, width, scale}` |
| `HUGEINT` / `UHUGEINT` | `%Duckdbex.Hugeint{upper, lower}` |
| `UNION` | `%Duckdbex.Union{tag, value}` |
| `MAP` | `[{key, value}]` |
| `TUPLE` | tuple |
| `LIST` / `ARRAY` | list |

Parameters accept the same representations (strings are still accepted for
date, time, timestamp and interval columns).

DuckDB's `infinity` / `-infinity` (valid for `DATE`, `TIMESTAMP`, `TIMESTAMPTZ` and
their `_NS` variants) are returned as `:infinity` / `:"-infinity"` atoms, because
neither `Date` nor `NaiveDateTime` can represent them. The same atoms are accepted
as parameters.

### Raw values

Decoding costs a struct per value. For throughput-sensitive reads, `fetch_all/2`
and `fetch_chunk/2` accept `raw: true`, which returns the integers DuckDB stores:

```elixir
> {:ok, r} = Duckdbex.query(conn, "SELECT DATE '1970-01-02', TIMESTAMP '1970-01-01 00:00:02';")
> Duckdbex.fetch_all(r, raw: true)
[[1, 2_000_000]]
```

| DuckDB | raw value |
| --- | --- |
| `DATE` | days since 1970-01-01 |
| `TIME` | microseconds since midnight |
| `TIMESTAMP` / `TIMESTAMPTZ` | microseconds since the epoch |
| `TIMESTAMP_S` / `_MS` / `_NS` | seconds / milliseconds / nanoseconds |
| `TIMETZ` | `{microseconds, offset_seconds}` |

Other types are unaffected, and `:infinity` / `:"-infinity"` stay atoms in both
modes.

### Huge numbers (hugeint)

`HUGEINT` is a 128-bit integer. The NIF API cannot build Erlang bignums, so the
value is returned as two 64-bit halves. Convert it when you need an integer:

```elixir
> {:ok, r} = Duckdbex.query(conn, "SELECT SUM(1);")
> Duckdbex.fetch_all(r)
[[%Duckdbex.Hugeint{upper: 0, lower: 1}]]
> Duckdbex.hugeint_to_integer(%Duckdbex.Hugeint{upper: 0, lower: 1})
1
```

Passing a huge value works the same way:

```elixir
> hi = Duckdbex.integer_to_hugeint(123456789123456789123456789)
%Duckdbex.Hugeint{upper: 6692605, lower: 17502027875430457109}
> {:ok, r} = Duckdbex.query(conn, "SELECT SUM(1234567891234567891234567891) > $1;", [hi])
> Duckdbex.fetch_all(r)
[[true]]
```

The conversion is not automatic because it would add a pass over every fetched
row.

### Decimal

`DECIMAL` keeps the unscaled integer, width and scale. `unscaled` is a plain
integer when it fits in 64 bits and a `Duckdbex.Hugeint` otherwise. If the
optional [`decimal`](https://hex.pm/packages/decimal) package is available:

```elixir
> {:ok, r} = Duckdbex.query(conn, "SELECT 123.45::DECIMAL(10,2);")
> [[decimal]] = Duckdbex.fetch_all(r)
> %Duckdbex.Decimal{unscaled: 12345, width: 10, scale: 2} = decimal
> {:ok, decimal} = Duckdbex.Decimal.to_decimal(decimal)
> Decimal.to_string(decimal)
"123.45"
```

## Remote access (Quack)

DuckDB 2.0 ships the pre-release
[Quack](https://github.com/duckdb/duckdb-quack) client-server protocol. It is a regular DuckDB
extension, so duckdbex can serve a database and query it from another connection.

```elixir
# server
{:ok, _} = Duckdbex.query(conn, "INSTALL quack; LOAD quack;")
{:ok, _} = Duckdbex.query(conn, "CREATE TABLE main.hello (s VARCHAR);")
{:ok, _} = Duckdbex.query(conn, "INSERT INTO main.hello VALUES ('world');")

{:ok, r} = Duckdbex.query(conn, "CALL quack_serve('quack:localhost', token = 'secret');")
[["quack:localhost", "http://localhost:9494", "secret"]] = Duckdbex.fetch_all(r)

# client
{:ok, _} = Duckdbex.query(client, "INSTALL quack; LOAD quack;")
{:ok, _} = Duckdbex.query(client, "CREATE SECRET (TYPE quack, TOKEN 'secret');")
{:ok, _} = Duckdbex.query(client, "ATTACH 'quack:localhost' AS remote;")

{:ok, r} = Duckdbex.query(client, "SELECT * FROM remote.hello;")
[["world"]] = Duckdbex.fetch_all(r)
```

`CALL quack_serve(...)` returns as soon as the server is listening; the server runs inside the
DuckDB instance and stops when that instance is released. The end-to-end test is opt-in because it
needs network access to install the extension: `mix test --include quack`.

## Upgrading to DuckDB 2.0

DuckDB 2.0 has a new storage format, so database files written by 2.0 cannot be opened by
DuckDB 1.x. DuckDB 2.0 still reads and writes files created by earlier versions, so upgrading an
existing database is safe as long as you do not need to go back.

duckdbex 0.7 changes how DuckDB types are represented in Elixir: dates and
timestamps are `Date`/`Time`/`NaiveDateTime`/`DateTime` instead of tuples, and
`DECIMAL`, `HUGEINT`, `INTERVAL`, `TIMETZ` and `UNION` are `Duckdbex.*` structs
instead of tuples. See "Elixir representation of DuckDB types" above. Inputs
must use the new representation.

Other changes that affect duckdbex users:

* The PEG parser is now the default. The single-arrow lambda syntax (`x -> x + 1`) is disabled;
  use `lambda x: x + 1` or `SET lambda_syntax = 'enable_single_arrow';`.
* Parser errors include source locations, and several DuckDB error messages were reworded.
* Unnamed structs such as `row(1, 'a')` are now reported as `TUPLE` and arrive as an Elixir tuple.
* `TUPLE`, `TIMESTAMPTZ_NS`, `VARIANT` and `GEOMETRY` conversions were added, and `json` plus
  `icu` are now statically linked, so JSON functions (`'…'::JSON`, `json_extract`, …) and
  timezone/collation functions work without `LOAD`.
