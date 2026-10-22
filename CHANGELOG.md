# Changelog

0.6.0
- [DuckDB v2.0.0](https://github.com/duckdb/duckdb/releases/tag/v2.0.0) ([#60](https://github.com/AlexR2D2/duckdbex/pull/60))
- Building from source now requires a C++17 compiler.
- Some DuckDB error messages changed text (e.g. missing prepared statement parameters); code matching on error strings may need updating.

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
