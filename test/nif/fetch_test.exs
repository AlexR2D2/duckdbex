defmodule Duckdbex.Nif.FetchTest do
  use ExUnit.Case

  alias Duckdbex.NIF

  setup ctx do
    {:ok, db} = NIF.open(":memory:", nil)
    {:ok, conn} = NIF.connection(db)
    Map.put(ctx, :conn, conn)
  end

  test "fetch_chunk", %{conn: conn} do
    {:ok, _} = Duckdbex.NIF.query(conn, """
      CREATE TABLE chunk_test(bigint BIGINT, boolean BOOLEAN, varchar VARCHAR);
    """)

    {:ok, _} = Duckdbex.NIF.query(conn, """
      INSERT INTO chunk_test VALUES (1, true, 'one'), (2, true, 'two');
    """)

    {:ok, result_ref} = Duckdbex.NIF.query(conn, "SELECT * FROM chunk_test")

    assert [[1, true, "one"], [2, true, "two"]] == Duckdbex.NIF.fetch_chunk(result_ref)
    assert [] == Duckdbex.NIF.fetch_chunk(result_ref)
    assert [] == Duckdbex.NIF.fetch_chunk(result_ref)
  end

  test "fetch_all", %{conn: conn} do
    {:ok, _} = Duckdbex.NIF.query(conn, """
      CREATE TABLE chunk_test(bigint BIGINT, boolean BOOLEAN, varchar VARCHAR);
    """)

    {:ok, _} = Duckdbex.NIF.query(conn, """
      INSERT INTO chunk_test VALUES (1, true, 'one'), (2, true, 'two');
    """)

    {:ok, result_ref} = Duckdbex.NIF.query(conn, "SELECT * FROM chunk_test")

    assert [[1, true, "one"], [2, true, "two"]] == Duckdbex.NIF.fetch_all(result_ref)
    assert [] == Duckdbex.NIF.fetch_all(result_ref)
    assert [] == Duckdbex.NIF.fetch_all(result_ref)
  end
end
