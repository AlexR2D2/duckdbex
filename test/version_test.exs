defmodule Duckdbex.VersionTest do
  use ExUnit.Case, async: true

  describe "DuckDB version" do
    test "matches @duckdb_version in mix.exs" do
      assert {:ok, db} = Duckdbex.open(":memory:")
      assert {:ok, conn} = Duckdbex.connection(db)
      assert {:ok, result} = Duckdbex.query(conn, "SELECT version()")
      assert [["v" <> database_version]] = Duckdbex.fetch_all(result)

      assert database_version == Duckdbex.MixProject.duckdb_version()
    end
  end
end
