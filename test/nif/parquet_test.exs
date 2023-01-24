defmodule Duckdbex.Nif.ParquetTest do
  use ExUnit.Case

  alias Duckdbex.NIF

  setup ctx do
    {:ok, db} = NIF.open(":memory:", nil)
    {:ok, conn} = NIF.connection(db)
    on_exit(fn -> File.rm_rf "test/support/recorded.parquet" end)
    Map.put(ctx, :conn, conn)
  end

  test "read parquet", %{conn: conn} do
    assert {:ok, res} = Duckdbex.NIF.query(conn, "SELECT * FROM 'test/support/data.parquet';")
    assert [[1, 2], [3, 4], [5, 6]] = Duckdbex.NIF.fetch_all(res)
  end

  test "write parquet", %{conn: conn} do
    assert {:ok, res} = Duckdbex.NIF.query(conn, """
      COPY (SELECT * FROM 'test/support/data.parquet')
      TO 'test/support/recorded.parquet' (FORMAT 'parquet')
    """)
    assert [[3]] = Duckdbex.NIF.fetch_all(res)

    assert {:ok, res} = Duckdbex.NIF.query(conn, "SELECT * FROM 'test/support/recorded.parquet';")
    assert [[1, 2], [3, 4], [5, 6]] = Duckdbex.NIF.fetch_all(res)
  end
end
