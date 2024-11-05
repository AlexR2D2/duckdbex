defmodule Duckdbex.Nif.ColumnsTest do
  use ExUnit.Case

  setup ctx do
    {:ok, db} = Duckdbex.open(":memory:", nil)
    {:ok, conn} = Duckdbex.connection(db)
    Map.put(ctx, :conn, conn)
  end

  test "when table is empty, returns columns names", %{conn: conn} do
    {:ok, _} =
      Duckdbex.query(conn, """
        CREATE TABLE columns_test(count BIGINT, is_ready BOOLEAN, name VARCHAR);
      """)

    {:ok, result_ref} = Duckdbex.query(conn, "SELECT * FROM columns_test")

    assert ["count", "is_ready", "name"] = Duckdbex.columns(result_ref)
  end

  test "when table has rows, returns columns names", %{conn: conn} do
    {:ok, _} =
      Duckdbex.query(conn, """
        CREATE TABLE columns_test(count BIGINT, is_ready BOOLEAN, name VARCHAR);
      """)

    {:ok, _} =
      Duckdbex.query(conn, """
        INSERT INTO columns_test VALUES (1, true, 'one'), (2, true, 'two');
      """)

    {:ok, result_ref} = Duckdbex.query(conn, "SELECT name, count FROM columns_test")

    assert ["name", "count"] = Duckdbex.NIF.columns(result_ref)
  end

  test "when select at different column name, returns specified column name", %{conn: conn} do
    {:ok, _} =
      Duckdbex.query(conn, """
        CREATE TABLE columns_test(count BIGINT, is_ready BOOLEAN, name VARCHAR);
      """)

    {:ok, _} =
      Duckdbex.query(conn, """
        INSERT INTO columns_test VALUES (1, true, 'one'), (2, true, 'two');
      """)

    {:ok, result_ref} = Duckdbex.query(conn, "SELECT name as my_name FROM columns_test")

    assert ["my_name"] = Duckdbex.columns(result_ref)
  end

  test "when select constants, returns constanst itself as columns names", %{conn: conn} do
    {:ok, result_ref} = Duckdbex.query(conn, "SELECT 1, 'two', 3.14")

    assert ["1", "'two'", "3.14"] = Duckdbex.columns(result_ref)
  end
end
