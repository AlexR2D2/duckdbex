defmodule Duckdbex.AppenderTest do
  use ExUnit.Case

  setup ctx do
    {:ok, db} = Duckdbex.open(":memory:", nil)
    {:ok, conn} = Duckdbex.connection(db)
    Map.put(ctx, :conn, conn)
  end

  test "append one row", %{conn: conn} do
    {:ok, _} =
      Duckdbex.query(conn, """
        CREATE TABLE appender_test_1(
          bigint BIGINT,
          boolean BOOLEAN,
          varchar VARCHAR,
          timestamp TIMESTAMP);
      """)

    assert {:ok, appender} = Duckdbex.appender(conn, "appender_test_1")

    assert :ok =
             Duckdbex.appender_add_row(appender, [
               123,
               true,
               "the one",
               "2022-10-20 23:59:59.999"
             ])

    assert :ok = Duckdbex.appender_flush(appender)

    {:ok, r} = Duckdbex.query(conn, "SELECT * FROM appender_test_1;")

    assert [[123, true, "the one", {{2022, 10, 20}, {23, 59, 59, 999_000}}]] =
             Duckdbex.fetch_all(r)
  end

  test "append one row with the incorrect columns count", %{conn: conn} do
    {:ok, _} =
      Duckdbex.query(conn, """
        CREATE TABLE appender_test_1_1(bigint BIGINT, boolean BOOLEAN);
      """)

    assert {:ok, appender} = Duckdbex.appender(conn, "appender_test_1_1")

    assert_raise(ArgumentError, fn ->
      Duckdbex.appender_add_row(appender, [123, true, 456])
    end)
  end

  test "append multiple rows", %{conn: conn} do
    {:ok, _} =
      Duckdbex.query(conn, """
        CREATE TABLE appender_test_2(bigint BIGINT, boolean BOOLEAN);
      """)

    assert {:ok, appender} = Duckdbex.appender(conn, "appender_test_2")
    assert :ok = Duckdbex.appender_add_rows(appender, [[123, true], [456, false]])
    assert :ok = Duckdbex.appender_flush(appender)

    {:ok, r} = Duckdbex.query(conn, "SELECT * FROM appender_test_2;")

    assert [[123, true], [456, false]] = Duckdbex.fetch_all(r)
  end

  test "append multiple rows with the incorrect columns count", %{conn: conn} do
    {:ok, _} =
      Duckdbex.query(conn, """
        CREATE TABLE appender_test_3(bigint BIGINT, boolean BOOLEAN);
      """)

    assert {:ok, appender} = Duckdbex.appender(conn, "appender_test_3")

    assert_raise(ArgumentError, fn ->
      Duckdbex.appender_add_rows(appender, [[123, true], [456], [true]])
    end)
  end

  test "appender schema table", %{conn: conn} do
    {:ok, _} =
      Duckdbex.query(conn, """
          create schema schema_1;
      """)

    {:ok, _} =
      Duckdbex.query(conn, """
        CREATE TABLE schema_1.appender_test_1(
          bigint BIGINT,
        );
      """)

    assert {:ok, appender} = Duckdbex.appender(conn, "schema_1", "appender_test_1")

    assert :ok =
             Duckdbex.appender_add_row(appender, [
               123
             ])

    assert :ok = Duckdbex.appender_flush(appender)

    {:ok, r} = Duckdbex.query(conn, "SELECT * FROM schema_1.appender_test_1;")

    assert [[123]] =
             Duckdbex.fetch_all(r)
  end
end
