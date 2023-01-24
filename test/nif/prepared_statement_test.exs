defmodule Duckdbex.Nif.PreparedStatementTest do
  use ExUnit.Case

  alias Duckdbex.NIF

  setup ctx do
    {:ok, db} = NIF.open(":memory:", nil)
    {:ok, conn} = NIF.connection(db)
    Map.put(ctx, :conn, conn)
  end

  test "prepared statement", %{conn: conn} do
    assert {:ok, stmt} = NIF.prepare_statement(conn, "SELECT 1;")
    assert {:ok, res} = NIF.execute_statement(stmt)
    assert [[1]] = NIF.fetch_all(res)
  end

  test "prepared statement with params", %{conn: conn} do
    assert {:ok, stmt} = NIF.prepare_statement(conn, "SELECT 1 WHERE $1 = 1;")
    assert {:ok, res} = NIF.execute_statement(stmt, [1])
    assert [[1]] = NIF.fetch_all(res)
  end
end
