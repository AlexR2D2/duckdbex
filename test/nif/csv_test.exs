defmodule Duckdbex.Nif.CSVTest do
  use ExUnit.Case

  alias Duckdbex.NIF

  setup ctx do
    {:ok, db} = NIF.open(":memory:", nil)
    {:ok, conn} = NIF.connection(db)
    on_exit(fn -> File.rm_rf("test/support/recorded.csv") end)
    Map.put(ctx, :conn, conn)
  end

  test "read csv", %{conn: conn} do
    assert {:ok, res} = Duckdbex.NIF.query(conn, "SELECT * FROM 'test/support/data.csv';")
    assert [["1", "2", "3"], ["a", "b", "c"]] = Duckdbex.NIF.fetch_all(res)
  end

  test "write csv", %{conn: conn} do
    assert {:ok, _} = NIF.query(conn, "CREATE TABLE tbl(the_date DATE, the_hugeint HUGEINT);")

    assert {:ok, _} =
             NIF.query(
               conn,
               "INSERT INTO tbl VALUES ('2000-01-01', 5::hugeint), ('2022-01-31', 10);"
             )

    assert {:ok, res} =
             Duckdbex.NIF.query(conn, """
               COPY tbl TO 'test/support/recorded.csv' (HEADER, DELIMITER ',');
             """)

    assert [[2]] = Duckdbex.NIF.fetch_all(res)

    assert {:ok, res} = Duckdbex.NIF.query(conn, "SELECT * FROM 'test/support/recorded.csv';")
    assert [[{2000, 1, 1}, 5], [{2022, 1, 31}, 10]] = Duckdbex.NIF.fetch_all(res)
  end
end
