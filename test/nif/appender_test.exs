defmodule Duckdbex.Nif.AppenderTest do
  use ExUnit.Case

  alias Duckdbex.NIF

  setup ctx do
    {:ok, db} = NIF.open(":memory:", nil)
    {:ok, conn} = NIF.connection(db)
    Map.put(ctx, :conn, conn)
  end

  test "append one row", %{conn: conn} do
    {:ok, _} =
      Duckdbex.NIF.query(conn, """
        CREATE TABLE appender_test_1(
          bigint BIGINT,
          boolean BOOLEAN,
          varchar VARCHAR,
          timestamp TIMESTAMP);
      """)

    assert {:ok, appender} = Duckdbex.NIF.appender(conn, "appender_test_1")

    assert :ok =
             Duckdbex.NIF.appender_add_row(appender, [
               123,
               true,
               "the one",
               "2022-10-20 23:59:59.999"
             ])

    assert :ok = Duckdbex.NIF.appender_flush(appender)

    {:ok, r} = Duckdbex.NIF.query(conn, "SELECT * FROM appender_test_1;")

    assert [[123, true, "the one", {{2022, 10, 20}, {23, 59, 59, 999_000}}]] =
             Duckdbex.NIF.fetch_all(r)
  end

  test "append one row with the incorrect columns count", %{conn: conn} do
    {:ok, _} =
      Duckdbex.NIF.query(conn, """
        CREATE TABLE appender_test_1_1(bigint BIGINT, boolean BOOLEAN);
      """)

    assert {:ok, appender} = Duckdbex.NIF.appender(conn, "appender_test_1_1")

    assert_raise(ArgumentError, fn ->
      Duckdbex.NIF.appender_add_row(appender, [123, true, 456])
    end)
  end

  test "append multiple rows", %{conn: conn} do
    {:ok, _} =
      Duckdbex.NIF.query(conn, """
        CREATE TABLE appender_test_2(bigint BIGINT, boolean BOOLEAN);
      """)

    assert {:ok, appender} = Duckdbex.NIF.appender(conn, "appender_test_2")
    assert :ok = Duckdbex.NIF.appender_add_rows(appender, [[123, true], [456, false]])
    assert :ok = Duckdbex.NIF.appender_flush(appender)

    {:ok, r} = Duckdbex.NIF.query(conn, "SELECT * FROM appender_test_2;")

    assert [[123, true], [456, false]] = Duckdbex.NIF.fetch_all(r)
  end

  test "append multiple rows with the incorrect columns count", %{conn: conn} do
    {:ok, _} =
      Duckdbex.NIF.query(conn, """
        CREATE TABLE appender_test_3(bigint BIGINT, boolean BOOLEAN);
      """)

    assert {:ok, appender} = Duckdbex.NIF.appender(conn, "appender_test_3")

    assert_raise(ArgumentError, fn ->
      Duckdbex.NIF.appender_add_rows(appender, [[123, true], [456], [true]])
    end)
  end

  describe "types" do
    test "append BIGINT", %{conn: conn} do
      test_appending_type(conn, "BIGINT", [1_234_567_890], [[1_234_567_890]])
    end

    test "append BIGINT nil", %{conn: conn} do
      test_appending_type(conn, "BIGINT", [nil], [[nil]])
    end

    test "append BOOLEAN", %{conn: conn} do
      test_appending_type(conn, "BOOLEAN", [true], [[true]])
    end

    test "append BOOLEAN nil", %{conn: conn} do
      test_appending_type(conn, "BOOLEAN", [nil], [[nil]])
    end

    test "append DOUBLE", %{conn: conn} do
      test_appending_type(conn, "DOUBLE", [1.0], [[1.0]])
    end

    test "append DOUBLE nil", %{conn: conn} do
      test_appending_type(conn, "DOUBLE", [nil], [[nil]])
    end

    test "append DECIMAL", %{conn: conn} do
      test_appending_type(conn, "DECIMAL(5,3)", [{:decimal, {-22174, 5, 3}}], [[{-22174, 5, 3}]])
    end

    test "append DECIMAL nil", %{conn: conn} do
      test_appending_type(conn, "DECIMAL(5,3)", [nil], [[nil]])
    end

    test "append DECIMAL native", %{conn: conn} do
      test_appending_type(conn, "DECIMAL(5,3)", [-22.174], [[{-22174, 5, 3}]])
    end

    test "append INTEGER", %{conn: conn} do
      test_appending_type(conn, "INTEGER", [-15], [[-15]])
    end

    test "append INTEGER nil", %{conn: conn} do
      test_appending_type(conn, "INTEGER", [nil], [[nil]])
    end

    test "append FLOAT", %{conn: conn} do
      test_appending_type(conn, "FLOAT", [-1.0], [[-1.0]])
    end

    test "append FLOAT nil", %{conn: conn} do
      test_appending_type(conn, "FLOAT", [nil], [[nil]])
    end

    test "append SMALLINT", %{conn: conn} do
      test_appending_type(conn, "SMALLINT", [-127], [[-127]])
    end

    test "append SMALLINT nil", %{conn: conn} do
      test_appending_type(conn, "SMALLINT", [nil], [[nil]])
    end

    test "append TINYINT", %{conn: conn} do
      test_appending_type(conn, "TINYINT", [-1], [[-1]])
    end

    test "append TINYINT nil", %{conn: conn} do
      test_appending_type(conn, "TINYINT", [nil], [[nil]])
    end

    test "append UBIGINT", %{conn: conn} do
      test_appending_type(conn, "UBIGINT", [1_234_567_890], [[1_234_567_890]])
    end

    test "append UBIGINT nil", %{conn: conn} do
      test_appending_type(conn, "UBIGINT", [nil], [[nil]])
    end

    test "append UINTEGER", %{conn: conn} do
      test_appending_type(conn, "UINTEGER", [1234], [[1234]])
    end

    test "append UINTEGER nil", %{conn: conn} do
      test_appending_type(conn, "UINTEGER", [nil], [[nil]])
    end

    test "append USMALLINT", %{conn: conn} do
      test_appending_type(conn, "USMALLINT", [1], [[1]])
    end

    test "append USMALLINT nil", %{conn: conn} do
      test_appending_type(conn, "USMALLINT", [nil], [[nil]])
    end

    test "append UTINYINT", %{conn: conn} do
      test_appending_type(conn, "UTINYINT", [1], [[1]])
    end

    test "append UTINYINT nil", %{conn: conn} do
      test_appending_type(conn, "UTINYINT", [nil], [[nil]])
    end

    test "append VARCHAR", %{conn: conn} do
      test_appending_type(conn, "VARCHAR", ["str"], [["str"]])
    end

    test "append VARCHAR nil", %{conn: conn} do
      test_appending_type(conn, "VARCHAR", [nil], [[nil]])
    end

    test "append UUID", %{conn: conn} do
      test_appending_type(
        conn,
        "UUID",
        ["5e740554-23ad-11ed-861d-0242ac120002"],
        [["5e740554-23ad-11ed-861d-0242ac120002"]]
      )
    end

    test "append UUID nil", %{conn: conn} do
      test_appending_type(conn, "UUID", [nil], [[nil]])
    end

    test "append DATE str", %{conn: conn} do
      test_appending_type(conn, "DATE", ["2022-12-12"], [[{2022, 12, 12}]])
    end

    test "append DATE nil", %{conn: conn} do
      test_appending_type(conn, "DATE", [nil], [[nil]])
    end

    test "append DATE tuple", %{conn: conn} do
      test_appending_type(conn, "DATE", [{2022, 12, 12}], [[{2022, 12, 12}]])
    end

    test "append TIME str", %{conn: conn} do
      test_appending_type(conn, "TIME", ["01:59:59.999"], [[{1, 59, 59, 999_000}]])
    end

    test "append TIME nil", %{conn: conn} do
      test_appending_type(conn, "TIME", [nil], [[nil]])
    end

    test "append TIME tuple", %{conn: conn} do
      test_appending_type(conn, "TIME", [{21, 59, 59, 999}], [[{21, 59, 59, 999}]])
    end

    test "append TIMESTAMP str", %{conn: conn} do
      test_appending_type(
        conn,
        "TIMESTAMP",
        ["2022-10-20 23:59:59.999123"],
        [[{{2022, 10, 20}, {23, 59, 59, 999_123}}]]
      )
    end

    test "append TIMESTAMP nil", %{conn: conn} do
      test_appending_type(conn, "TIMESTAMP", [nil], [[nil]])
    end

    test "append TIMESTAMP tuple", %{conn: conn} do
      test_appending_type(
        conn,
        "TIMESTAMP",
        [{{2022, 10, 20}, {23, 59, 59, 999_123}}],
        [[{{2022, 10, 20}, {23, 59, 59, 999_123}}]]
      )
    end

    test "append TIMESTAMPTZ str with Z", %{conn: conn} do
      test_appending_type(
        conn,
        "TIMESTAMPTZ",
        ["2022-10-20T23:59:59.999Z"],
        [[{{2022, 10, 20}, {23, 59, 59, 999_000}}]]
      )
    end

    test "append TIMESTAMPTZ nil with Z", %{conn: conn} do
      test_appending_type(conn, "TIMESTAMPTZ", [nil], [[nil]])
    end

    test "append TIMESTAMPTZ str with +05:00 offset", %{conn: conn} do
      test_appending_type(
        conn,
        "TIMESTAMPTZ",
        ["2022-10-20T23:59:59.999+05:00"],
        [[{{2022, 10, 20}, {18, 59, 59, 999_000}}]]
      )
    end

    test "append TIMESTAMPTZ tuple", %{conn: conn} do
      test_appending_type(
        conn,
        "TIMESTAMPTZ",
        [{{2022, 10, 20}, {23, 59, 59, 999_123}}],
        [[{{2022, 10, 20}, {23, 59, 59, 999_123}}]]
      )
    end

    test "append TIMESTAMPTZ tuple +05:05 offset", %{conn: conn} do
      test_appending_type(
        conn,
        "TIMESTAMPTZ",
        [{{2022, 10, 20}, {23, 59, 59, 999_123}, {5, 0}}],
        [[{{2022, 10, 20}, {18, 59, 59, 999_123}}]]
      )
    end

    test "append TIMESTAMPTZ tuple -05:05 offset", %{conn: conn} do
      test_appending_type(
        conn,
        "TIMESTAMPTZ",
        [{{2022, 10, 20}, {23, 59, 59, 999_123}, {-5, 30}}],
        [[{{2022, 10, 21}, {4, 29, 59, 999_123}}]]
      )
    end

    test "append BLOB as string", %{conn: conn} do
      test_appending_type(conn, "BLOB", ["data"], [["data"]])
    end

    test "append BLOB nil", %{conn: conn} do
      test_appending_type(conn, "BLOB", [nil], [[nil]])
    end

    test "append BLOB as charlist", %{conn: conn} do
      test_appending_type(conn, "BLOB", ['data'], [["data"]])
    end

    test "append BLOB as bitstring", %{conn: conn} do
      test_appending_type(conn, "BLOB", [<<42, 42, 42>>], [["***"]])
    end

    test "append INTERVAL as string", %{conn: conn} do
      test_appending_type(conn, "INTERVAL", ["5 DAYS"], [[432_000_000_000]])
    end

    test "append INTERVAL nil", %{conn: conn} do
      test_appending_type(conn, "INTERVAL", [nil], [[nil]])
    end

    test "append INTERVAL as unsigned integer", %{conn: conn} do
      test_appending_type(conn, "INTERVAL", [432_000_000_000], [[432_000_000_000]])
    end

    test "append HUGEINT", %{conn: conn} do
      hugeint = Duckdbex.integer_to_hugeint(123)
      test_appending_type(conn, "HUGEINT", [{:hugeint, hugeint}], [[hugeint]])
    end

    test "append HUGEINT nil", %{conn: conn} do
      test_appending_type(conn, "HUGEINT", [nil], [[nil]])
    end

    test "append ENUM", %{conn: conn} do
      assert {:ok, _} = NIF.query(conn, "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');")
      test_appending_type(conn, "mood", ["happy"], [["happy"]])
    end

    test "append ENUM nil", %{conn: conn} do
      assert {:ok, _} = NIF.query(conn, "CREATE TYPE nilmood AS ENUM ('sad', 'ok', 'happy');")
      test_appending_type(conn, "nilmood", [nil], [[nil]])
    end

    test "append LIST", %{conn: conn} do
      test_appending_type(conn, "INT[]", [[1, 2, 3]], [[[1, 2, 3]]])
    end

    test "append LIST nil", %{conn: conn} do
      test_appending_type(conn, "INT[]", [nil], [[nil]])
    end

    test "append MAP", %{conn: conn} do
      test_appending_type(conn, "MAP(INT, DOUBLE)", [%{1 => 3.14}], [[%{1 => 3.14}]])
    end

    test "append MAP nil", %{conn: conn} do
      test_appending_type(conn, "MAP(INT, DOUBLE)", [nil], [[nil]])
    end

    test "append STRUCT", %{conn: conn} do
      test_appending_type(conn, "STRUCT(i INT, j VARCHAR)", [%{"i" => 123, "j" => "hello"}], [
        [%{"i" => 123, "j" => "hello"}]
      ])
    end

    test "append STRUCT nil", %{conn: conn} do
      test_appending_type(conn, "STRUCT(i INT, j VARCHAR)", [nil], [[nil]])
    end
  end

  defp test_appending_type(conn, column_type, row, result) do
    {:ok, _} = Duckdbex.NIF.query(conn, "CREATE TABLE appender_test(data #{column_type});")

    {:ok, appender} = Duckdbex.NIF.appender(conn, "appender_test")
    :ok = Duckdbex.NIF.appender_add_row(appender, row)
    :ok = Duckdbex.NIF.appender_flush(appender)

    {:ok, r} = Duckdbex.NIF.query(conn, "SELECT * FROM appender_test;")

    assert ^result = Duckdbex.NIF.fetch_all(r)
  end
end
