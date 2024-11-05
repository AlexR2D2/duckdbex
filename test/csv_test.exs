defmodule Duckdbex.CSVTest do
  use ExUnit.Case

  setup ctx do
    {:ok, db} = Duckdbex.open(":memory:", nil)
    {:ok, conn} = Duckdbex.connection(db)
    on_exit(fn -> File.rm_rf("test/support/recorded.csv") end)
    Map.put(ctx, :conn, conn)
  end

  test "read csv auto infer options", %{conn: conn} do
    assert {:ok, res} = Duckdbex.query(conn, "SELECT * FROM 'test/support/data.csv';")
    assert [["1", "2", "3"], ["a", "b", "c"]] = Duckdbex.fetch_all(res)
  end

  test "read csv using user defined options", %{conn: conn} do
    assert {:ok, res} =
             Duckdbex.query(
               conn,
               "SELECT * FROM read_csv('test/support/data.csv', header = false);"
             )

    assert [["c1", "c2", "c3"], ["1", "2", "3"], ["a", "b", "c"]] = Duckdbex.fetch_all(res)

    assert {:ok, res} =
             Duckdbex.query(
               conn,
               "SELECT * FROM read_csv('test/support/data.csv', header = true);"
             )

    assert [["1", "2", "3"], ["a", "b", "c"]] = Duckdbex.fetch_all(res)
  end

  test "write csv", %{conn: conn} do
    assert {:ok, _} =
             Duckdbex.query(conn, "CREATE TABLE tbl(the_date DATE, the_hugeint HUGEINT);")

    assert {:ok, _} =
             Duckdbex.query(
               conn,
               "INSERT INTO tbl VALUES ('2000-01-01', 5::hugeint), ('2022-01-31', 10);"
             )

    assert {:ok, res} =
             Duckdbex.query(conn, """
               COPY tbl TO 'test/support/recorded.csv' (HEADER, DELIMITER ',');
             """)

    assert [[2]] = Duckdbex.fetch_all(res)

    assert {:ok, res} = Duckdbex.query(conn, "SELECT * FROM 'test/support/recorded.csv';")
    assert [[{2000, 1, 1}, 5], [{2022, 1, 31}, 10]] = Duckdbex.fetch_all(res)
  end
end
