defmodule Duckdbex.FetchTest do
  use ExUnit.Case

  setup ctx do
    {:ok, db} = Duckdbex.open(":memory:", nil)
    {:ok, conn} = Duckdbex.connection(db)
    Map.put(ctx, :conn, conn)
  end

  test "fetch_chunk", %{conn: conn} do
    {:ok, _} =
      Duckdbex.query(conn, """
        CREATE TABLE chunk_test(bigint BIGINT, boolean BOOLEAN, varchar VARCHAR);
      """)

    {:ok, _} =
      Duckdbex.query(conn, """
        INSERT INTO chunk_test VALUES (1, true, 'one'), (2, true, 'two');
      """)

    {:ok, result_ref} = Duckdbex.query(conn, "SELECT * FROM chunk_test")

    assert [[1, true, "one"], [2, true, "two"]] == Duckdbex.fetch_chunk(result_ref)
    assert [] == Duckdbex.fetch_chunk(result_ref)
    assert [] == Duckdbex.fetch_chunk(result_ref)
  end

  test "fetch_all", %{conn: conn} do
    {:ok, _} =
      Duckdbex.query(conn, """
        CREATE TABLE chunk_test(bigint BIGINT, boolean BOOLEAN, varchar VARCHAR);
      """)

    {:ok, _} =
      Duckdbex.query(conn, """
        INSERT INTO chunk_test VALUES (1, true, 'one'), (2, true, 'two');
      """)

    {:ok, result_ref} = Duckdbex.query(conn, "SELECT * FROM chunk_test")

    assert [[1, true, "one"], [2, true, "two"]] == Duckdbex.fetch_all(result_ref)
    assert [] == Duckdbex.fetch_all(result_ref)
    assert [] == Duckdbex.fetch_all(result_ref)
  end
end
