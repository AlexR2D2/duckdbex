defmodule DuckdbexTest do
  use ExUnit.Case, async: true
  doctest Duckdbex

  test "open/2" do
    assert {:ok, _db} = Duckdbex.open(":memory:", %Duckdbex.Config{})
  end

  test "open/1" do
    assert {:ok, _db} = Duckdbex.open(":memory:")
    assert {:ok, _db} = Duckdbex.open(%Duckdbex.Config{})
  end

  test "open/0" do
    assert {:ok, _db} = Duckdbex.open()
  end

  test "connection/1" do
    assert {:ok, db} = Duckdbex.open()
    assert {:ok, _conn} = Duckdbex.connection(db)
  end

  test "query/2" do
    assert {:ok, db} = Duckdbex.open()
    assert {:ok, conn} = Duckdbex.connection(db)
    assert {:ok, _res} = Duckdbex.query(conn, "SELECT 1;")
  end

  test "query/3" do
    assert {:ok, db} = Duckdbex.open()
    assert {:ok, conn} = Duckdbex.connection(db)
    assert {:ok, _res} = Duckdbex.query(conn, "SELECT 1 WHERE 1 = $1;", [1])
  end

  test "fetch_chunk/1" do
    assert {:ok, db} = Duckdbex.open()
    assert {:ok, conn} = Duckdbex.connection(db)
    assert {:ok, result} = Duckdbex.query(conn, "SELECT 1 WHERE 1 = $1;", [1])
    assert [[1]] = Duckdbex.fetch_chunk(result)
  end

  test "fetch_all/1" do
    assert {:ok, db} = Duckdbex.open()
    assert {:ok, conn} = Duckdbex.connection(db)
    assert {:ok, result} = Duckdbex.query(conn, "SELECT 1 WHERE 1 = $1;", [1])
    assert [[1]] = Duckdbex.fetch_all(result)
  end

  test "appender/2" do
    assert {:ok, db} = Duckdbex.open()
    assert {:ok, conn} = Duckdbex.connection(db)
    assert {:error, "Table 'table_1' could not be found"} = Duckdbex.appender(conn, "table_1")

    assert {:ok, _res} = Duckdbex.query(conn, "CREATE TABLE table_1(data INTEGER)")
    assert {:ok, _res} = Duckdbex.appender(conn, "table_1")
  end

  test "appender_add_row/2" do
    assert {:ok, db} = Duckdbex.open()
    assert {:ok, conn} = Duckdbex.connection(db)
    assert {:ok, _res} = Duckdbex.query(conn, "CREATE TABLE table_1(the_n1 INTEGER, the_n2 INTEGER)")
    assert {:ok, appender} = Duckdbex.appender(conn, "table_1")
    assert :ok = Duckdbex.appender_add_row(appender, [1, 2])
  end

  test "appender_add_rows/2" do
    assert {:ok, db} = Duckdbex.open()
    assert {:ok, conn} = Duckdbex.connection(db)
    assert {:ok, _res} = Duckdbex.query(conn, "CREATE TABLE table_1(the_n1 INTEGER, the_n2 INTEGER)")
    assert {:ok, appender} = Duckdbex.appender(conn, "table_1")
    assert :ok = Duckdbex.appender_add_rows(appender, [[1, 2], [3, 4]])
  end

  test "appender_flush/2" do
    assert {:ok, db} = Duckdbex.open()
    assert {:ok, conn} = Duckdbex.connection(db)
    assert {:ok, _res} = Duckdbex.query(conn, "CREATE TABLE table_1(the_n1 INTEGER, the_n2 INTEGER)")
    assert {:ok, appender} = Duckdbex.appender(conn, "table_1")
    assert :ok = Duckdbex.appender_add_rows(appender, [[1, 2], [3, 4]])
    assert :ok = Duckdbex.appender_flush(appender)
  end

  test "appender_close/2" do
    assert {:ok, db} = Duckdbex.open()
    assert {:ok, conn} = Duckdbex.connection(db)
    assert {:ok, _res} = Duckdbex.query(conn, "CREATE TABLE table_1(the_n1 INTEGER, the_n2 INTEGER)")
    assert {:ok, appender} = Duckdbex.appender(conn, "table_1")
    assert :ok = Duckdbex.appender_add_rows(appender, [[1, 2], [3, 4]])
    assert :ok = Duckdbex.appender_close(appender)
  end

  test "integer_to_hugeint/1" do
    assert {0, 0} =
      Duckdbex.integer_to_hugeint(-0)

    assert {0, 1} =
      Duckdbex.integer_to_hugeint(1)

    assert {0, 18446744073709551615} =
      Duckdbex.integer_to_hugeint(0xFFFFFFFFFFFFFFFF)

    assert {-1, 1} =
      Duckdbex.integer_to_hugeint(-0x0FFFFFFFFFFFFFFFF)

    assert {-16, 16} =
      Duckdbex.integer_to_hugeint(-0xFFFFFFFFFFFFFFFF0)

    assert {18446744073709551615, 18446744073709551615} =
      Duckdbex.integer_to_hugeint(0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF)
  end

  test "hugeint_to_integer/1" do
    assert 0 = Duckdbex.hugeint_to_integer({0,0})

    assert 1 = Duckdbex.hugeint_to_integer({0,1})

    assert 18446744073709551616 = Duckdbex.hugeint_to_integer({1,0})

    assert 18446744073709551615 =
      Duckdbex.hugeint_to_integer({0,0xFFFFFFFFFFFFFFFF})

    assert 340282366920938463444927863358058659840 =
      Duckdbex.hugeint_to_integer({0xFFFFFFFFFFFFFFFF,0})

    assert -340282366920938463426481119284349108225 =
      Duckdbex.hugeint_to_integer({-0xFFFFFFFFFFFFFFFF,0xFFFFFFFFFFFFFFFF})
  end
end
