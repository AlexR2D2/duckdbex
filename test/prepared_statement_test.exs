defmodule Duckdbex.PreparedStatementTest do
  use ExUnit.Case

  setup ctx do
    {:ok, db} = Duckdbex.open(":memory:", nil)
    {:ok, conn} = Duckdbex.connection(db)
    Map.put(ctx, :conn, conn)
  end

  test "prepared statement", %{conn: conn} do
    assert {:ok, stmt} = Duckdbex.prepare_statement(conn, "SELECT 1;")
    assert {:ok, res} = Duckdbex.execute_statement(stmt)
    assert [[1]] = Duckdbex.fetch_all(res)
  end

  test "prepared statement with params", %{conn: conn} do
    assert {:ok, stmt} = Duckdbex.prepare_statement(conn, "SELECT 1 WHERE $1 = 1;")
    assert {:ok, res} = Duckdbex.execute_statement(stmt, [1])
    assert [[1]] = Duckdbex.fetch_all(res)
  end
end
