# Changelog

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
