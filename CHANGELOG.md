# Changelog

0.7.0
- Breaking: DuckDB values now use native Elixir types and structs instead of ad-hoc tuples.
  - `DATE` -> `Date`, `TIME` -> `Time`, `TIMESTAMP` -> `NaiveDateTime`, `TIMESTAMPTZ` -> `DateTime` (UTC), `TIMESTAMP_NS`/`TIMESTAMPTZ_NS` -> `%Duckdbex.TimestampNs{}` / `%Duckdbex.TimestampTzNs{}`.
  - `DECIMAL` -> `%Duckdbex.Decimal{}`, `HUGEINT`/`UHUGEINT` -> `%Duckdbex.Hugeint{}`, `INTERVAL` -> `%Duckdbex.Interval{}`, `TIMETZ` -> `%Duckdbex.TimeTz{}`, `UNION` -> `%Duckdbex.Union{}`.
  - Parameters accept the same representations; the old tuples are no longer accepted.
  - `Duckdbex.hugeint_to_integer/1` and `integer_to_hugeint/1` now use `%Duckdbex.Hugeint{}`.
  - `infinity` / `-infinity` dates and timestamps are returned as `:infinity` / `:"-infinity"` atoms instead of an invalid struct (and no longer crash on infinite timestamps).

0.6.0
- [DuckDB v2.0.0](https://github.com/duckdb/duckdb/releases/tag/v2.0.0) ([#60](https://github.com/AlexR2D2/duckdbex/pull/60))
- Building from source now requires a C++17 compiler.
- Some DuckDB error messages changed text (e.g. missing prepared statement parameters); code matching on error strings may need updating.
- Added named parameters: `Duckdbex.query/3` and `execute_statement/2` accept a map keyed by parameter name (`$name`), plus `get_named_parameters/1` for introspection.
- Added support for the new `TUPLE` and `TIMESTAMPTZ_NS` types. `TUPLE` is returned as an Elixir list and accepted as a list or a tuple, `TIMESTAMPTZ_NS` keeps nanosecond precision.
- Added support for `VARIANT` and `GEOMETRY` values. `GEOMETRY` is exposed as WKB binaries, and `VARIANT` accepts nested lists and maps as parameters.
- STRUCT parameters now match map keys case-insensitively, following DuckDB 2.0 `Identifier` semantics.
- Statically linked `json` and `icu`, so JSON functions and timezone/collation functions work without `LOAD`.
- Documented the Quack client-server protocol, with an opt-in end-to-end test (`mix test --include quack`).
- The generated DuckDB sources are no longer committed or shipped in the Hex package; they are regenerated on demand by `bin/regen_duckdb.sh` when `c_src/duckdb.tar.gz` is missing. A source build therefore needs `gh`, `python3` and `make`.

0.5.2
- [DuckDB v1.5.6 Bugfix Release](https://github.com/duckdb/duckdb/releases/tag/v1.5.6)

0.5.1
- Fixed BEAM crashes caused by uncaught C++ exceptions in appender operations, including after the connection is released or garbage-collected. These operations now return `{:error, reason}` on exceptions.
- Prevented NIF resource leaks when appender construction throws an exception.
- Documented that the connection must remain alive until the appender is successfully closed, and added regression tests for appender and result lifetimes.
- Fixed source compilation failing with `uint8_t` undeclared by adding the missing `<cstdint>` include ([#59](https://github.com/AlexR2D2/duckdbex/issues/59)).

0.5.0
- Statically link core_functions and parquet extensions ([PR](https://github.com/AlexR2D2/duckdbex/pull/57))

0.4.1
- Updated to DuckDB 1.5.3.

0.4.0
  - Breaking change release.
  - Updated to DuckDB 1.5.1.
  - Removed the old `%Duckdbex.Config{}` config API.
  - Replaced the old `%Duckdbex.Config{}`-based config layer with NIF-backed `DBConfig` resources.
  - Added `Duckdbex.create_config/0`, `Duckdbex.set_config_option/3` and `Duckdbex.get_config_options/0`.
  - `Duckdbex.get_config_options/0` now exposes the config options reported by DuckDB directly.
  - Updated tests and README to document the new config model.
  - Merged the @arthurbailao PR [fix: correct NIF resource lifecycle and C++ initialization](https://github.com/AlexR2D2/duckdbex/pull/53)

0.3.21
  - merged the @karvla PR: [handle special float cases #52](https://github.com/AlexR2D2/duckdbex/pull/52)

0.3.20
  - [DuckDB 1.4.4 bugfix release](https://github.com/duckdb/duckdb/releases/tag/v1.4.4)
  - Added the `DUCKDBEX_EXTENSION_DIRECTORY` env var to set default value of extensions directory in %DBConfig{}.
  - Added the `DUCKDBEX_TEMPORARY_DIRECTORY` env var to set default value of temporary directory in %DBConfig{}.

0.3.19
  - [DuckDB 1.4.3 bugfix release](https://github.com/duckdb/duckdb/releases/tag/v1.4.3)

0.3.18
  - [DuckDB 1.4.2 bugfix release](https://github.com/duckdb/duckdb/releases/tag/v1.4.2)

0.3.17
  - [DuckDB 1.4.1 bugfix release](https://github.com/duckdb/duckdb/releases/tag/v1.4.1)

0.3.16
  - Fixed issue with invalid checksums in HEX release.
  - Added `allowed_paths` config parameter.
  - Added `allowed_directories` config parameter.

0.3.15
  - [DuckDB 1.4.0 bugfix release](https://github.com/duckdb/duckdb/releases/tag/v1.4.0)
  - Autoload and install extension by default.

0.3.14
- Added dialyzer

0.3.10
  - [DuckDB 1.2.1 bugfix release](https://github.com/duckdb/duckdb/releases/tag/v1.2.1)

0.3.9
- [DuckDB 1.2.0 release](https://github.com/duckdb/duckdb/releases/tag/v1.2.0). Please, read the [Announcing DuckDB 1.2.0](https://duckdb.org/2025/02/05/announcing-duckdb-120)
- Fixed the isinf/isnan build error on Linux
- `DuckDB.query(sql)` without parameters can execute multiple SQL statements at onсe.

0.3.8
- Added transaction managing functions: begin_transaction, commit, rollback, set_auto_commit, is_auto_commit, has_active_transaction

0.3.7
- DuckDB 1.1.3 bugfix release.
- Added release(resource) function.
- Extended DbConfig with new parameters.
- Now INTERVAL type in Elixir is tuple of size 3 : {months, days, micros} (Here in month 30 days...).
- Now MAP type in Elixir is list of tuples [{key, value}]. Because MAP is ordered list and 'key' can be any type.
