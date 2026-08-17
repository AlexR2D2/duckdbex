defmodule Duckdbex.ExtensionsTest do
  use ExUnit.Case, async: true

  setup do
    assert {:ok, db} = Duckdbex.open()
    assert {:ok, conn} = Duckdbex.connection(db)
    %{conn: conn}
  end

  describe "core_functions" do
    test "now() and current_timestamp return timestamps", %{conn: conn} do
      assert {:ok, r} = Duckdbex.query(conn, "SELECT now();")
      assert [[{{_, _, _}, {_, _, _, _}}]] = Duckdbex.fetch_all(r)

      assert {:ok, r} = Duckdbex.query(conn, "SELECT current_timestamp;")
      assert [[{{_, _, _}, {_, _, _, _}}]] = Duckdbex.fetch_all(r)
    end

    # bump this together with scripts/regen_duckdb.sh <ref>
    test "version() reports the bundled duckdb version", %{conn: conn} do
      assert {:ok, r} = Duckdbex.query(conn, "SELECT version();")
      assert [["v1.5.5"]] = Duckdbex.fetch_all(r)
    end

    test "date functions work without LOAD", %{conn: conn} do
      assert {:ok, r} = Duckdbex.query(conn, "SELECT strftime(DATE '2024-01-15', '%Y-%m');")
      assert [["2024-01"]] = Duckdbex.fetch_all(r)

      assert {:ok, r} =
               Duckdbex.query(
                 conn,
                 "SELECT date_diff('day', DATE '2024-01-01', DATE '2024-01-10');"
               )

      assert [[9]] = Duckdbex.fetch_all(r)
    end
  end

  describe "statically linked extensions" do
    test "core_functions and parquet report STATICALLY_LINKED", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(
                 conn,
                 "SELECT extension_name, install_mode FROM duckdb_extensions() WHERE extension_name IN ('core_functions', 'parquet') ORDER BY extension_name;"
               )

      assert [
               ["core_functions", "STATICALLY_LINKED"],
               ["parquet", "STATICALLY_LINKED"]
             ] = Duckdbex.fetch_all(r)
    end
  end

  describe "parquet" do
    test "round-trips data through a parquet file", %{conn: conn} do
      path = parquet_path()
      on_exit(fn -> File.rm(path) end)

      assert {:ok, _} =
               Duckdbex.query(conn, """
               COPY (SELECT 42 AS int_col, 3.14::DOUBLE AS float_col, 'hello' AS str_col, TIMESTAMP '2024-01-15 10:30:00' AS ts_col)
               TO '#{path}' (FORMAT PARQUET);
               """)

      assert {:ok, r} = Duckdbex.query(conn, "SELECT * FROM '#{path}';")
      assert [[42, 3.14, "hello", {{2024, 1, 15}, {10, 30, 0, 0}}]] = Duckdbex.fetch_all(r)
    end

    test "parquet_scan reads a file written by COPY", %{conn: conn} do
      path = parquet_path()
      on_exit(fn -> File.rm(path) end)

      assert {:ok, _} =
               Duckdbex.query(conn, """
               COPY (SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, label))
               TO '#{path}' (FORMAT PARQUET);
               """)

      assert {:ok, r} = Duckdbex.query(conn, "SELECT * FROM parquet_scan('#{path}') ORDER BY id;")
      assert [[1, "a"], [2, "b"]] = Duckdbex.fetch_all(r)
    end

    defp parquet_path do
      Path.join(System.tmp_dir!(), "duckdbex_ext_#{System.unique_integer([:positive])}.parquet")
    end
  end
end
