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

  test "prepare/execute statement" do
    assert {:ok, db} = Duckdbex.open()
    assert {:ok, conn} = Duckdbex.connection(db)
    assert {:ok, stmt} = Duckdbex.prepare_statement(conn, "SELECT 1;")
    assert {:ok, res} = Duckdbex.execute_statement(stmt)
    assert [[1]] = Duckdbex.fetch_all(res)
  end

  test "prepare/execute statement with params" do
    assert {:ok, db} = Duckdbex.open()
    assert {:ok, conn} = Duckdbex.connection(db)
    assert {:ok, stmt} = Duckdbex.prepare_statement(conn, "SELECT 1 WHERE $1 = 1;")
    assert {:ok, res} = Duckdbex.execute_statement(stmt, [1])
    assert [[1]] = Duckdbex.fetch_all(res)
  end

  test "begin_transaction/1" do
    assert {:ok, db} = Duckdbex.open()
    assert {:ok, conn} = Duckdbex.connection(db)
    assert :ok = Duckdbex.begin_transaction(conn)
  end

  test "commit/1" do
    assert {:ok, db} = Duckdbex.open()
    assert {:ok, conn} = Duckdbex.connection(db)
    assert :ok = Duckdbex.begin_transaction(conn)
    assert :ok = Duckdbex.commit(conn)
  end

  test "rollback/1" do
    assert {:ok, db} = Duckdbex.open()
    assert {:ok, conn} = Duckdbex.connection(db)
    assert :ok = Duckdbex.begin_transaction(conn)
    assert :ok = Duckdbex.rollback(conn)
  end

  test "set_auto_commit/2" do
    assert {:ok, db} = Duckdbex.open()
    assert {:ok, conn} = Duckdbex.connection(db)
    assert :ok = Duckdbex.set_auto_commit(conn, true)
  end

  test "is_auto_commit/1" do
    assert {:ok, db} = Duckdbex.open()
    assert {:ok, conn} = Duckdbex.connection(db)
    assert {:ok, true} = Duckdbex.is_auto_commit(conn)
    assert :ok = Duckdbex.set_auto_commit(conn, false)
    assert {:ok, false} = Duckdbex.is_auto_commit(conn)
  end

  test "has_active_transaction/1" do
    assert {:ok, db} = Duckdbex.open()
    assert {:ok, conn} = Duckdbex.connection(db)
    assert {:ok, false} = Duckdbex.has_active_transaction(conn)
    assert :ok = Duckdbex.begin_transaction(conn)
    assert {:ok, true} = Duckdbex.has_active_transaction(conn)
    assert :ok = Duckdbex.rollback(conn)
    assert {:ok, false} = Duckdbex.has_active_transaction(conn)
  end

  test "columns/1" do
    assert {:ok, db} = Duckdbex.open()
    assert {:ok, conn} = Duckdbex.connection(db)

    assert {:ok, result} =
             Duckdbex.query(conn, "SELECT 1 as 'one_column_name', 2 WHERE 1 = $1;", [1])

    assert ["one_column_name", "2"] = Duckdbex.columns(result)
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

    assert {:ok, _res} =
             Duckdbex.query(conn, "CREATE TABLE table_1(the_n1 INTEGER, the_n2 INTEGER)")

    assert {:ok, appender} = Duckdbex.appender(conn, "table_1")
    assert :ok = Duckdbex.appender_add_row(appender, [1, 2])
  end

  test "appender_add_rows/2" do
    assert {:ok, db} = Duckdbex.open()
    assert {:ok, conn} = Duckdbex.connection(db)

    assert {:ok, _res} =
             Duckdbex.query(conn, "CREATE TABLE table_1(the_n1 INTEGER, the_n2 INTEGER)")

    assert {:ok, appender} = Duckdbex.appender(conn, "table_1")
    assert :ok = Duckdbex.appender_add_rows(appender, [[1, 2], [3, 4]])
  end

  test "appender_flush/2" do
    assert {:ok, db} = Duckdbex.open()
    assert {:ok, conn} = Duckdbex.connection(db)

    assert {:ok, _res} =
             Duckdbex.query(conn, "CREATE TABLE table_1(the_n1 INTEGER, the_n2 INTEGER)")

    assert {:ok, appender} = Duckdbex.appender(conn, "table_1")
    assert :ok = Duckdbex.appender_add_rows(appender, [[1, 2], [3, 4]])
    assert :ok = Duckdbex.appender_flush(appender)
  end

  test "appender_close/2" do
    assert {:ok, db} = Duckdbex.open()
    assert {:ok, conn} = Duckdbex.connection(db)

    assert {:ok, _res} =
             Duckdbex.query(conn, "CREATE TABLE table_1(the_n1 INTEGER, the_n2 INTEGER)")

    assert {:ok, appender} = Duckdbex.appender(conn, "table_1")
    assert :ok = Duckdbex.appender_add_rows(appender, [[1, 2], [3, 4]])
    assert :ok = Duckdbex.appender_close(appender)
  end

  test "release/1" do
    assert {:ok, db} = Duckdbex.open()
    assert :ok = Duckdbex.release(db)

    assert_raise ArgumentError, fn ->
      Duckdbex.number_of_threads(db)
    end
  end

  test "integer_to_hugeint/1" do
    assert {0, 0} =
             Duckdbex.integer_to_hugeint(-0)

    assert {0, 1} =
             Duckdbex.integer_to_hugeint(1)

    assert {0, 18_446_744_073_709_551_615} =
             Duckdbex.integer_to_hugeint(0xFFFFFFFFFFFFFFFF)

    assert {-1, 1} =
             Duckdbex.integer_to_hugeint(-0x0FFFFFFFFFFFFFFFF)

    assert {-16, 16} =
             Duckdbex.integer_to_hugeint(-0xFFFFFFFFFFFFFFFF0)

    assert {18_446_744_073_709_551_615, 18_446_744_073_709_551_615} =
             Duckdbex.integer_to_hugeint(0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF)
  end

  test "hugeint_to_integer/1" do
    assert 0 = Duckdbex.hugeint_to_integer({0, 0})

    assert 1 = Duckdbex.hugeint_to_integer({0, 1})

    assert 18_446_744_073_709_551_616 = Duckdbex.hugeint_to_integer({1, 0})

    assert 18_446_744_073_709_551_615 =
             Duckdbex.hugeint_to_integer({0, 0xFFFFFFFFFFFFFFFF})

    assert 340_282_366_920_938_463_444_927_863_358_058_659_840 =
             Duckdbex.hugeint_to_integer({0xFFFFFFFFFFFFFFFF, 0})

    assert -340_282_366_920_938_463_426_481_119_284_349_108_225 =
             Duckdbex.hugeint_to_integer({-0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF})
  end

  test "library_version/0" do
    assert Duckdbex.library_version() =~ "v"
  end

  test "storage_format_version/0" do
    assert is_number(Duckdbex.storage_format_version())
  end

  test "library_version/1" do
    assert "v0.6.0 or v0.6.1" == Duckdbex.library_version(39)
  end

  test "source_id/0" do
    assert is_binary(Duckdbex.source_id())
  end

  test "platform/0" do
    assert is_binary(Duckdbex.platform())
  end

  test "extension_is_loaded/1" do
    assert {:ok, db} = Duckdbex.open()
    refute Duckdbex.extension_is_loaded(db, "parquet")
  end

  test "number_of_threads/1" do
    assert {:ok, db} = Duckdbex.open()
    assert is_integer(Duckdbex.number_of_threads(db))
  end

  test "when integer parameter is implicitly converted to double" do
    assert {:ok, db} = Duckdbex.open()
    assert {:ok, conn} = Duckdbex.connection(db)

    assert {:ok, r} = Duckdbex.query(conn, "SELECT 1 WHERE 3434.2323/1000 < $1;", [10])

    assert [[1]] = Duckdbex.fetch_all(r)
  end

  test "when double parameter is implicitly converted to integer" do
    assert {:ok, db} = Duckdbex.open()
    assert {:ok, conn} = Duckdbex.connection(db)

    assert {:error, "invalid type of parameter #1"} =
             Duckdbex.query(conn, "SELECT 1 WHERE 10 <= $1;", [10.0])
  end

  test "multiple statement query without parameters" do
    assert {:ok, db} = Duckdbex.open()
    assert {:ok, conn} = Duckdbex.connection(db)

    assert {:ok, res} = Duckdbex.query(conn, "SELECT 3; SELECT 2; SELECT 1;")

    assert [[3]] = Duckdbex.fetch_all(res)
  end

  test "multiple statement query with parameters" do
    assert {:ok, db} = Duckdbex.open()
    assert {:ok, conn} = Duckdbex.connection(db)

    assert {:error, "Invalid Input Error: Cannot prepare multiple statements at once!"} =
             Duckdbex.query(conn, "SELECT 1 WHERE 1 = $1; SELECT 2;", [1])
  end
end
