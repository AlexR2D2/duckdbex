# Changelog

0.3.7
- DuckDB 1.1.3 bugfix release.
- Added release(resource) function.
- Extended DbConfig with new parameters.
- Now INTERVAL type in Elixir is tuple of size 3 : {months, days, micros} (Here in month 30 days...).
- Now MAP type in Elixir is list of tuples [{key, value}]. Because MAP is ordered list and 'key' can be any type.
