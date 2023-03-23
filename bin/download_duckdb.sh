#!/usr/bin/env bash

set -e

# VERSION is something like this 'v0.7.1'.
# This is git tag of duckdb source code.
# You can find available versions(tags) in https://github.com/duckdb/duckdb

mkdir -p tmp
pushd tmp

wget https://github.com/duckdb/duckdb/releases/download/$VERSION/libduckdb-src.zip

unzip -o libduckdb-src.zip

cp duckdb.hpp ../c_src/duckdb
cp duckdb.cpp ../c_src/duckdb

popd

rm -rf tmp

echo "UPDATED DUCKDB C++ CODE TO $VERSION!"
