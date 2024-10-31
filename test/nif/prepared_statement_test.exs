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

  test "insert union without params", %{conn: conn} do
    NIF.query(
      conn,
      """
        CREATE TABLE test_table(
          test_field UNION(single_text text, multiple_text text[])
        );
      """
    )

    assert {:ok, stmt} =
             NIF.prepare_statement(
               conn,
               """
                 INSERT INTO test_table (test_field)
                 VALUES ('test')
               """
             )

    assert {:ok, _res} = NIF.execute_statement(stmt)

    assert {:ok, stmt} = NIF.prepare_statement(conn, "SELECT * FROM test_table;")
    assert {:ok, res} = NIF.execute_statement(stmt)
    assert [[%{"single_text" => "test"}]] = NIF.fetch_all(res)
  end

  test "insert union with params", %{conn: conn} do
    NIF.query(
      conn,
      """
        CREATE TABLE test_table(
          test_field UNION(single_text text, multiple_text text[])
        );
      """
    )

    assert {:ok, stmt} =
             NIF.prepare_statement(
               conn,
               """
                 INSERT INTO test_table (test_field)
                 VALUES (?)
               """
             )

    assert {:ok, _res} = NIF.execute_statement(stmt, ["test"])

    assert {:ok, stmt} = NIF.prepare_statement(conn, "SELECT * FROM test_table;")
    assert {:ok, res} = NIF.execute_statement(stmt)
    assert [[%{"single_text" => "test"}]] = NIF.fetch_all(res)
  end

  test "insert struct without params", %{conn: conn} do
    NIF.query(conn, """
      create type test_struct as struct(
        test_field text
      );
    """)

    NIF.query(conn, """
      CREATE TABLE test_table(
        test_struct test_struct
      );
    """)

    assert {:ok, stmt} =
             NIF.prepare_statement(conn, """
               INSERT INTO test_table (test_struct)
               VALUES (struct_pack(test_field := 'test'))
             """)
    assert {:ok, _res} = NIF.execute_statement(stmt)

    assert {:ok, stmt} = NIF.prepare_statement(conn, "SELECT * FROM test_table;")
    assert {:ok, res} = NIF.execute_statement(stmt)
    assert [[%{"test_field" => "test"}]] = NIF.fetch_all(res)
  end

  test "insert struct with params", %{conn: conn} do
    NIF.query(conn, """
      create type test_struct as struct(
        test_field text
      );
    """)

    NIF.query(conn, """
      CREATE TABLE test_table(
        test_struct test_struct
      );
    """)

    assert {:ok, stmt} =
             NIF.prepare_statement(conn, """
               INSERT INTO test_table (test_struct)
               VALUES (struct_pack(test_field := ?))
             """)
    assert {:ok, _res} = NIF.execute_statement(stmt, ["test"])

    assert {:ok, stmt} = NIF.prepare_statement(conn, "SELECT * FROM test_table;")
    assert {:ok, res} = NIF.execute_statement(stmt)
    assert [[%{"test_field" => "test"}]] = NIF.fetch_all(res)
  end
end
