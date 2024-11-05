defmodule Duckdbex.TypesTest do
  use ExUnit.Case, async: true

  setup ctx do
    assert {:ok, db} = Duckdbex.open(":memory:", %Duckdbex.Config{})
    assert {:ok, conn} = Duckdbex.connection(db)
    Map.put(ctx, :conn, conn)
  end

  # TODO : duckdb::LogicalTypeId::BIT
  # TODO : duckdb::LogicalTypeId::VARINT
  # TODO : duckdb::LogicalTypeId::POINTER
  # TODO : duckdb::LogicalTypeId::AGGREGATE_STATE

  # TODO ?: duckdb::LogicalTypeId::USER  : user defined type
  # TODO ?: duckdb::LogicalTypeId::INVALID
  # TODO ?: duckdb::LogicalTypeId::UNKNOWN
  # TODO ?: duckdb::LogicalTypeId::ANY
  # TODO ?: duckdb::LogicalTypeId::STRING_LITERAL
  # TODO ?: duckdb::LogicalTypeId::INTEGER_LITERAL
  # TODO ?: duckdb::LogicalTypeId::VALIDITY
  # TODO ?: duckdb::LogicalTypeId::TABLE
  # TODO ?: duckdb::LogicalTypeId::LAMBDAs

  # UNION
  # A union of multiple alternative data types, storing one of them in each value at a time.
  # https://duckdb.org/docs/sql/data_types/union
  describe "UNION" do
    setup %{conn: conn} do
      assert {:ok, _} =
               Duckdbex.query(conn, "CREATE TABLE table1 (col1 UNION(num INTEGER, str VARCHAR));")

      assert {:ok, _} =
               Duckdbex.query(conn, """
                 INSERT INTO table1 VALUES (1), ('two'), (union_value(str := 'three'));
               """)

      %{conn: conn}
    end

    test "inspect", %{conn: conn} do
      assert {:ok, r} = Duckdbex.query(conn, "SELECT * FROM table1;")

      assert [[{"num", 1}], [{"str", "two"}], [{"str", "three"}]] =
               Duckdbex.fetch_all(r)
    end

    test "native input/output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [{"str", "two"}])

      assert [[{"str", "two"}]] = Duckdbex.fetch_all(r)
    end
  end

  # STRUCT
  # A dictionary of multiple named values, where each key is a string,
  # but the value can be a different type for each key.
  # https://duckdb.org/docs/sql/data_types/struct
  describe "STRUCT" do
    setup %{conn: conn} do
      assert {:ok, _} =
               Duckdbex.query(
                 conn,
                 "CREATE TABLE table1 (col1 STRUCT(name VARCHAR, age INTEGER));"
               )

      assert {:ok, _} =
               Duckdbex.query(conn, """
                 INSERT INTO table1 VALUES (row('Bob', 30)), (row('Alice', 25));
               """)

      %{conn: conn}
    end

    test "inspect", %{conn: conn} do
      assert {:ok, r} = Duckdbex.query(conn, "SELECT * FROM table1;")

      assert [[%{"age" => 30, "name" => "Bob"}], [%{"age" => 25, "name" => "Alice"}]] =
               Duckdbex.fetch_all(r)
    end

    test "'map' input/output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [
                 %{"age" => 25, "name" => "Alice"}
               ])

      assert [[%{"age" => 25, "name" => "Alice"}]] = Duckdbex.fetch_all(r)
    end

    test "'map' wrong order input/output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [
                 %{"name" => "Alice", "age" => 25}
               ])

      assert [[%{"age" => 25, "name" => "Alice"}]] = Duckdbex.fetch_all(r)
    end
  end

  # MAP A dictionary of multiple named values, each key having the same type and each value having the same type.
  # Keys and values can be any type and can be different types from one another.
  # https://duckdb.org/docs/sql/data_types/map
  describe "MAP" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1 (col1 MAP(DOUBLE, VARCHAR));")

      assert {:ok, _} =
               Duckdbex.query(conn, """
                 INSERT INTO table1
                 VALUES (MAP {1.1: 'one', 5.1: 'five'}), (MAP {7.1: 'seven'});
               """)

      %{conn: conn}
    end

    test "inspect", %{conn: conn} do
      assert {:ok, r} = Duckdbex.query(conn, "SELECT * FROM table1;")

      assert [[[{1.1, "one"}, {5.1, "five"}]], [[{7.1, "seven"}]]] =
               Duckdbex.fetch_all(r)
    end

    test "'map' input/output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [
                 [{1.1, "one"}, {5.1, "five"}]
               ])

      assert [[[{1.1, "one"}, {5.1, "five"}]]] = Duckdbex.fetch_all(r)
    end

    test "'map' wrong order input/output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [
                 [{5.1, "five"}, {1.1, "one"}]
               ])

      assert [] = Duckdbex.fetch_all(r)
      # because MAP is ordered!
    end
  end

  # ARRAY An ordered, fixed-length sequence of data values of the same type.
  # https://duckdb.org/docs/sql/data_types/array
  describe "ARRAY" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1 (col1 INT[3]);")

      assert {:ok, _} =
               Duckdbex.query(
                 conn,
                 "INSERT INTO table1 VALUES ([1, null, null]), ([null, 4, 5]);"
               )

      %{conn: conn}
    end

    test "list input/output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [[1, nil, nil]])

      assert [[[1, nil, nil]]] = Duckdbex.fetch_all(r)
    end
  end

  # LIST An ordered sequence of data values of the same type.
  # https://duckdb.org/docs/sql/data_types/list
  describe "LIST" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1 (col1 INT[]);")

      assert {:ok, _} =
               Duckdbex.query(
                 conn,
                 "INSERT INTO table1 VALUES ([1]), ([4, 5]), ([]), ([1, 2, NULL]);"
               )

      %{conn: conn}
    end

    test "list input/output", %{conn: conn} do
      assert {:ok, r} = Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [[4, 5]])
      assert [[[4, 5]]] = Duckdbex.fetch_all(r)
    end

    test "empty list input/output", %{conn: conn} do
      assert {:ok, r} = Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [[]])
      assert [[[]]] = Duckdbex.fetch_all(r)
    end
  end

  # ENUM Dictionary Encoding representing all possible string values of a column.
  # https://duckdb.org/docs/sql/data_types/enum
  describe "ENUM" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');")
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1 (col1 mood);")

      assert {:ok, _} =
               Duckdbex.query(
                 conn,
                 "INSERT INTO table1 VALUES ('happy'), (NULL), ('sad'), ('ok');"
               )

      %{conn: conn}
    end

    test "binary input/output", %{conn: conn} do
      assert {:ok, r} = Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1;", ["sad"])
      assert [["sad"]] = Duckdbex.fetch_all(r)
    end
  end

  # HUGEINT
  # https://duckdb.org/docs/sql/data_types/numeric
  describe "UHUGEINT" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1(col1 UHUGEINT);")

      assert {:ok, _} =
               Duckdbex.query(conn, """
               INSERT INTO table1
               VALUES (10::hugeint), (7.6::uhugeint), (98233720368547758080000::uhugeint);
               """)

      %{conn: conn}
    end

    test "integer_to_hugeint" do
      _uhugeint =
        {18_446_744_073_709_551_615, 18_446_744_073_709_551_615} =
        Duckdbex.integer_to_hugeint(_uinteger = 0xFFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF)

      assert _uinteger =
               340_282_366_920_938_463_463_374_607_431_768_211_455 =
               Duckdbex.hugeint_to_integer(
                 _uhugeint = {18_446_744_073_709_551_615, 18_446_744_073_709_551_615}
               )

      assert 340_282_366_920_938_463_463_374_607_431_768_211_455 ==
               0xFFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF
    end

    test "native uhugeint input/output", %{conn: conn} do
      {_, _} = uhugeint = Duckdbex.integer_to_hugeint(_uinteger = 98_233_720_368_547_758_080_000)

      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [uhugeint])

      assert [[_uhugeint = {5325, 4_808_176_044_395_724_800}]] = Duckdbex.fetch_all(r)

      assert _uinteger =
               98_233_720_368_547_758_080_000 =
               Duckdbex.hugeint_to_integer(_uhugeint = {5325, 4_808_176_044_395_724_800})
    end

    test "double input, hugeint output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1;", [_double = 7.9])

      assert [] = Duckdbex.fetch_all(r)
    end

    test "binary input, uhugeint output", %{conn: conn} do
      assert {:ok, r} = Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1;", ["8"])
      assert [[{0, 8}]] = Duckdbex.fetch_all(r)

      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1;", [
                 "98233720368547758080000"
               ])

      assert [[{5325, 4_808_176_044_395_724_800}]] = Duckdbex.fetch_all(r)
    end
  end

  # HUGEINT
  # https://duckdb.org/docs/sql/data_types/numeric
  describe "HUGEINT" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1(col1 HUGEINT);")

      assert {:ok, _} =
               Duckdbex.query(conn, """
               INSERT INTO table1
               VALUES (-10::hugeint), (5::hugeint), (6.4::hugeint), (7.6::hugeint), (10::hugeint), (98233720368547758080000::hugeint);
               """)

      %{conn: conn}
    end

    test "integer_to_hugeint" do
      assert {5325, 4_808_176_044_395_724_800} =
               Duckdbex.integer_to_hugeint(98_233_720_368_547_758_080_000)

      assert 98_233_720_368_547_758_080_000 =
               Duckdbex.hugeint_to_integer({5325, 4_808_176_044_395_724_800})
    end

    test "native hugeint input/output", %{conn: conn} do
      {_, _} = hugeint = Duckdbex.integer_to_hugeint(_integer = 98_233_720_368_547_758_080_000)

      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [hugeint])

      assert [[_hugeint = {5325, 4_808_176_044_395_724_800}]] = Duckdbex.fetch_all(r)

      assert _integer =
               98_233_720_368_547_758_080_000 =
               Duckdbex.hugeint_to_integer(_hugeint = {5325, 4_808_176_044_395_724_800})
    end

    test "integer input, hugeint output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1;", [_integer = -10])

      assert [[_hugeint = {-1, 18_446_744_073_709_551_606}]] = Duckdbex.fetch_all(r)

      assert _integer =
               -10 = Duckdbex.hugeint_to_integer(_hugeint = {-1, 18_446_744_073_709_551_606})
    end

    test "double input, hugeint output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1;", [_double = 6.4])

      assert [[_hugeint = {0, 6}]] = Duckdbex.fetch_all(r)

      assert _integer = 6 = Duckdbex.hugeint_to_integer(_hugeint = {0, 6})
    end

    test "binary input, hugeint output", %{conn: conn} do
      assert {:ok, r} = Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1;", ["5"])
      assert [[{0, 5}]] = Duckdbex.fetch_all(r)

      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1;", [
                 "98233720368547758080000"
               ])

      assert [[{5325, 4_808_176_044_395_724_800}]] = Duckdbex.fetch_all(r)
    end

    test "invalid input", %{conn: conn} do
      invalid_inputs = [
        "sdfsfsfd",
        {2022, 10, 232},
        {"323", 12},
        %{}
      ]

      Enum.each(invalid_inputs, fn invalid_input ->
        assert {:error, "invalid type of parameter #1"} =
                 Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1;", [invalid_input])
      end)
    end
  end

  # UBIGINT max 18446744073709551615
  # https://duckdb.org/docs/sql/data_types/numeric#integer-types
  describe "UBIGINT (-)" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1(col1 UBIGINT);")

      assert {:ok, _} = Duckdbex.query(conn, "INSERT INTO table1 VALUES (18446744073709551615);")

      %{conn: conn}
    end

    test "native input/output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 == $1;", [
                 18_446_744_073_709_551_615
               ])

      assert [[18_446_744_073_709_551_615]] = Duckdbex.fetch_all(r)
    end
  end

  # BIGINT max 9223372036854775807
  # https://duckdb.org/docs/sql/data_types/numeric#integer-types
  describe "BIGINT (INT8, INT64 LONG)" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1(col1 BIGINT);")

      assert {:ok, _} =
               Duckdbex.query(
                 conn,
                 "INSERT INTO table1 VALUES (-9223372036854775808), (9223372036854775807);"
               )

      %{conn: conn}
    end

    test "native input/output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 == $1;", [
                 9_223_372_036_854_775_807
               ])

      assert [[9_223_372_036_854_775_807]] = Duckdbex.fetch_all(r)
    end
  end

  # UINTEGER
  # https://duckdb.org/docs/sql/data_types/numeric#integer-types
  describe "UINTEGER (-)" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1(col1 UINTEGER);")

      assert {:ok, _} = Duckdbex.query(conn, "INSERT INTO table1 VALUES (1), (0), (245);")

      %{conn: conn}
    end

    test "native input/output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 == $1;", [0])

      assert [[0]] = Duckdbex.fetch_all(r)
    end
  end

  # INTEGER
  # https://duckdb.org/docs/sql/data_types/numeric#integer-types
  describe "INTEGER (INT4, INT32, INT, SIGNED)" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1(col1 INTEGER);")

      assert {:ok, _} = Duckdbex.query(conn, "INSERT INTO table1 VALUES (1), (0), (-245);")

      %{conn: conn}
    end

    test "native input/output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 == $1;", [0])

      assert [[0]] = Duckdbex.fetch_all(r)
    end
  end

  # USMALLINT
  # https://duckdb.org/docs/sql/data_types/numeric#integer-types
  describe "USMALLINT (INT2, INT16 SHORT)" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1(col1 USMALLINT);")

      assert {:ok, _} = Duckdbex.query(conn, "INSERT INTO table1 VALUES (1);")

      %{conn: conn}
    end

    test "native input/output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 == $1;", [1])

      assert [[1]] = Duckdbex.fetch_all(r)
    end
  end

  # SMALLINT
  # https://duckdb.org/docs/sql/data_types/numeric#integer-types
  describe "SMALLINT (INT2, INT16 SHORT)" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1(col1 SMALLINT);")

      assert {:ok, _} = Duckdbex.query(conn, "INSERT INTO table1 VALUES (1), (-1);")

      %{conn: conn}
    end

    test "native input/output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 == $1;", [-1])

      assert [[-1]] = Duckdbex.fetch_all(r)
    end
  end

  # UTINYINT
  # https://duckdb.org/docs/sql/data_types/numeric#integer-types
  describe "UTINYINT (UINT1)" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1(col1 UTINYINT);")

      assert {:ok, _} = Duckdbex.query(conn, "INSERT INTO table1 VALUES (0), (255);")

      %{conn: conn}
    end

    test "native input/output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 == $1;", [255])

      assert [[255]] = Duckdbex.fetch_all(r)
    end
  end

  # TINYINT
  # https://duckdb.org/docs/sql/data_types/numeric#integer-types
  describe "TINYINT (INT1)" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1(col1 TINYINT);")

      assert {:ok, _} = Duckdbex.query(conn, "INSERT INTO table1 VALUES (127), (-127);")

      %{conn: conn}
    end

    test "native input/output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 == $1;", [-127])

      assert [[-127]] = Duckdbex.fetch_all(r)
    end
  end

  # DOUBLE double precision floating-point number (8 bytes)
  # https://duckdb.org/docs/sql/data_types/numeric#floating-point-types
  describe "DOUBLE(FLOAT8)" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1(col1 DOUBLE);")

      assert {:ok, _} =
               Duckdbex.query(conn, """
                INSERT INTO table1 VALUES (3.14), (0.8), (-2.17), (5.0);
               """)

      %{conn: conn}
    end

    test "double input/output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(
                 conn,
                 """
                   SELECT * FROM table1
                   WHERE col1 > $1 AND col1 <= $2
                   ORDER BY col1;
                 """,
                 [-2.18, 0.81]
               )

      assert [[a], [b]] = Duckdbex.fetch_all(r)

      assert -2.16 == Float.ceil(a, 2)
      assert 0.81 == Float.ceil(b, 2)
    end

    test "integer input, double output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 == $1;", [5])

      assert [[5.0]] = Duckdbex.fetch_all(r)
    end
  end

  # FLOAT single precision floating-point number (4 bytes)
  # https://duckdb.org/docs/sql/data_types/numeric#floating-point-types
  describe "FLOAT(FLOAT4, REAL)" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1(col1 FLOAT);")

      assert {:ok, _} =
               Duckdbex.query(conn, """
                INSERT INTO table1 VALUES (3.14), (0.8), (-2.17), (5.0);
               """)

      %{conn: conn}
    end

    test "float input/output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(
                 conn,
                 """
                   SELECT * FROM table1
                   WHERE col1 > $1 AND col1 <= $2
                   ORDER BY col1;
                 """,
                 [-2.18, 0.81]
               )

      assert [[a], [b]] = Duckdbex.fetch_all(r)

      assert -2.17 == Float.ceil(a, 2)
      assert 0.81 == Float.ceil(b, 2)
    end

    test "integer input, float output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 == $1;", [5])

      assert [[5.0]] = Duckdbex.fetch_all(r)
    end
  end

  # DECIMAL(WIDTH, SCALE) represents an exact fixed-point decimal value (alias NUMERIC(WIDTH, SCALE))
  # https://duckdb.org/docs/sql/data_types/numeric#fixed-point-decimals
  describe "DECIMAL" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1(col1 DECIMAL(5,3));")

      assert {:ok, _} =
               Duckdbex.query(conn, """
                INSERT INTO table1
                VALUES (3.14::decimal), (0.8), (-22.174::decimal), (10::decimal);
               """)

      %{conn: conn}
    end

    test "decimal input/output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [
                 {__value = -22174, _width = 5, _scale = 3}
               ])

      assert [[{_value = -22174, _width = 5, _scale = 3}]] = Duckdbex.fetch_all(r)
    end

    test "double input, decimal output", %{conn: conn} do
      assert {:ok, r} = Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [0.8])
      assert [[{800, 5, 3}]] = Duckdbex.fetch_all(r)
    end

    test "binary input, decimal output", %{conn: conn} do
      assert {:ok, r} = Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", ["3.14"])
      assert [[{3140, 5, 3}]] = Duckdbex.fetch_all(r)
    end

    test "hugeint input, decimal output", %{conn: conn} do
      hugeint = Duckdbex.integer_to_hugeint(10)

      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [hugeint])

      assert [[{10000, 5, 3}]] = Duckdbex.fetch_all(r)
    end
  end

  # INTERVAL represent periods of time that can be added to or subtracted from DATE, TIMESTAMP, TIMESTAMPTZ, or TIME values.
  # Interval returned in duckdb format {months, days, micros}. In one month - 30 days.
  # https://duckdb.org/docs/sql/data_types/interval
  describe "INTERVAL" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1(col1 INTERVAL);")

      assert {:ok, _} =
               Duckdbex.query(conn, """
               INSERT INTO table1
               VALUES (INTERVAL 1 HOUR), (INTERVAL 1 DAY), (INTERVAL 2 MONTHS);
               """)

      %{conn: conn}
    end

    test "inspect", %{conn: conn} do
      assert {:ok, r} = Duckdbex.query(conn, "SELECT * FROM table1")

      assert [
               [{_months = 0, _days = 0, _micros = 3_600_000_000}],
               [{0, 1, 0}],
               [{2, 0, 0}]
             ] = Duckdbex.fetch_all(r)
    end

    test "interval input/output", %{conn: conn} do
      microseconds =
        (_hour = 1) * (_min_in_hour = 60) * (_sec_in_min = 60) * (_millsec_in_sec = 1000) *
          (_micrsec_in_millsec = 1000)

      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [
                 {_months = 0, _days = 0, microseconds}
               ])

      assert [[{0, 0, ^microseconds}]] = Duckdbex.fetch_all(r)
    end

    test "microseconds input/output", %{conn: conn} do
      microseconds =
        (_hour = 1) * (_min_in_hour = 60) * (_sec_in_min = 60) * (_millsec_in_sec = 1000) *
          (_micrsec_in_millsec = 1000)

      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [microseconds])

      assert [[{0, 0, ^microseconds}]] = Duckdbex.fetch_all(r)
    end

    test "binary input, microseconds output", %{conn: conn} do
      microseconds =
        (_hour = 1) * (_min_in_hour = 60) * (_sec_in_min = 60) * (_millsec_in_sec = 1000) *
          (_micrsec_in_millsec = 1000)

      assert {:ok, r} = Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", ["1 HOUR"])
      assert [[{0, 0, ^microseconds}]] = Duckdbex.fetch_all(r)
    end
  end

  # Timestamp with second precision (ignores time zone and all smaller than the seconds)
  # https://duckdb.org/docs/sql/data_types/timestamp
  describe "TIMESTAMP_S" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1(col1 TIMESTAMP_S);")

      assert {:ok, _} =
               Duckdbex.query(conn, """
               INSERT INTO table1
               VALUES ('1992-09-20 11:30:00.001'), ('2024-12-22 12:20:00.1233232323'), ('2000-05-12 10:20:00.0001')
               """)

      %{conn: conn}
    end

    test "native input/output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [
                 {{2024, 12, 22}, {12, 20, 0, _ignored = 321}}
               ])

      assert [[{{2024, 12, 22}, {12, 20, 0, _ignored = 0}}]] = Duckdbex.fetch_all(r)

      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [
                 {{2000, 05, 12}, {10, 20, 0, _ignored = 0}}
               ])

      assert [[{{2000, 05, 12}, {10, 20, 0, _ignored = 0}}]] = Duckdbex.fetch_all(r)
    end

    test "binary input, native output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [
                 "2024-12-22 12:20:00"
               ])

      assert [[{{2024, 12, 22}, {12, 20, 0, _ignored = 0}}]] = Duckdbex.fetch_all(r)

      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [
                 "2024-12-22 12:20:00.123"
               ])

      assert [] = Duckdbex.fetch_all(r)
    end

    test "invalid input", %{conn: conn} do
      invalid_inputs = [
        {{2022, 10, 20, 23}, {23, 59, 59, 999_000}},
        {{2022, 10}, {23, 59, 59, 999_000}},
        {{2022, 10, 23}, {23, 23, 59, 59, 999_000}},
        {{2022, 10, 23}, {23, 23}},
        {{2022, 10, 23}, {23, "23"}},
        {{"2022", 10, 23}, {23, "23"}},
        {"2022", 10, 23, 23, "23"},
        {{1992, 9, 20}, {11, 30, -10}},
        "sdfsdfsdf"
      ]

      Enum.each(invalid_inputs, fn invalid_input ->
        assert {:error, "invalid type of parameter #1"} =
                 Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1;", [invalid_input])
      end)
    end
  end

  # Timestamp with millisecond precision (ignores time zone)
  # https://duckdb.org/docs/sql/data_types/timestamp
  describe "TIMESTAMP_MS" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1(col1 TIMESTAMP_MS);")

      assert {:ok, _} =
               Duckdbex.query(conn, """
               INSERT INTO table1
               VALUES ('1992-09-20 11:30:00.001'), ('2024-12-22 12:20:00.1233232323'), ('2000-05-12 10:20:00.0001')
               """)

      %{conn: conn}
    end

    test "native input/output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [
                 {{2024, 12, 22}, {12, 20, 0, _milliseconds = 123}}
               ])

      assert [[{{2024, 12, 22}, {12, 20, 0, _milliseconds = 123}}]] = Duckdbex.fetch_all(r)

      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [
                 {{2000, 05, 12}, {10, 20, 0, _milliseconds = 0}}
               ])

      assert [[{{2000, 05, 12}, {10, 20, 0, _milliseconds = 0}}]] = Duckdbex.fetch_all(r)
    end

    test "binary input, native output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [
                 "2024-12-22 12:20:00.123"
               ])

      assert [[{{2024, 12, 22}, {12, 20, 0, _milliseconds = 123}}]] = Duckdbex.fetch_all(r)
    end

    test "invalid input", %{conn: conn} do
      invalid_inputs = [
        {{2022, 10, 20, 23}, {23, 59, 59, 999_000}},
        {{2022, 10}, {23, 59, 59, 999_000}},
        {{2022, 10, 23}, {23, 23, 59, 59, 999_000}},
        {{2022, 10, 23}, {23, 23}},
        {{2022, 10, 23}, {23, "23"}},
        {{"2022", 10, 23}, {23, "23"}},
        {"2022", 10, 23, 23, "23"},
        {{1992, 9, 20}, {11, 30, 0, 2_999_999_999}},
        "sdfsdfsdf"
      ]

      Enum.each(invalid_inputs, fn invalid_input ->
        assert {:error, "invalid type of parameter #1"} =
                 Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1;", [invalid_input])
      end)
    end
  end

  # Timestamp with nanosecond precision (ignores time zone)
  # https://duckdb.org/docs/sql/data_types/timestamp
  describe "TIMESTAMP_NS" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1(col1 TIMESTAMP_NS);")

      assert {:ok, _} =
               Duckdbex.query(conn, """
               INSERT INTO table1
               VALUES ('1992-09-20 11:30:00.999999999'), ('2024-12-22 12:20:00.000000001'),
               """)

      %{conn: conn}
    end

    test "native input/output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [
                 {{1992, 09, 20}, {11, 30, 0, _nanoseconds = 999_999_999}}
               ])

      assert [[{{1992, 9, 20}, {11, 30, 0, _nanoseconds = 999_999_999}}]] = Duckdbex.fetch_all(r)

      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [
                 {{2024, 12, 22}, {12, 20, 0, _nanoseconds = 1}}
               ])

      assert [[{{2024, 12, 22}, {12, 20, 0, _nanoseconds = 1}}]] = Duckdbex.fetch_all(r)
    end

    test "binary input, native output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [
                 "1992-09-20 11:30:00.999999999"
               ])

      assert [[{{1992, 9, 20}, {11, 30, 0, _nanoseconds = 999_999_999}}]] = Duckdbex.fetch_all(r)
    end

    test "invalid input", %{conn: conn} do
      invalid_inputs = [
        {{2022, 10, 20, 23}, {23, 59, 59, 999_000}},
        {{2022, 10}, {23, 59, 59, 999_000}},
        {{2022, 10, 23}, {23, 23, 59, 59, 999_000}},
        {{2022, 10, 23}, {23, 23}},
        {{2022, 10, 23}, {23, "23"}},
        {{"2022", 10, 23}, {23, "23"}},
        {"2022", 10, 23, 23, "23"},
        {{1992, 9, 20}, {11, 30, 0, 2_999_999_999}},
        "sdfsdfsdf"
      ]

      Enum.each(invalid_inputs, fn invalid_input ->
        assert {:error, "invalid type of parameter #1"} =
                 Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1;", [invalid_input])
      end)
    end
  end

  # Timestamp (uses time zone), with microsecond precision
  # https://duckdb.org/docs/sql/data_types/timestamp
  describe "TIMESTAMPTZ" do
    # Instances can be created using the type names as a keyword,
    # where the data must be formatted according
    # to the ISO 8601 format (hh:mm:ss[.zzzzzz][+-TT[:tt]]).

    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1(col1 TIMESTAMPTZ);")

      assert {:ok, _} =
               Duckdbex.query(conn, """
               INSERT INTO table1
               VALUES ('1992-09-20 11:30:00.123456789'),
                      ('2022-10-20T23:59:59.999Z'),
                      ('2022-10-20 11:30:00.123456+05:30'),
                      ('2022-10-20 10:30:10.123456-14:21');
               """)

      %{conn: conn}
    end

    test "native input/output, positive UTC offset", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(
                 conn,
                 "SELECT * FROM table1 WHERE col1 = $1;",
                 [
                   {{2022, 10, 20},
                    {_hh = 11, _mm = 30, _ss = 0, _zzzzzz = 123_456,
                     {_TT = 5, _tt = _microseconds = 30}}}
                 ]
               )

      # TIMESTAMPTZ converted to utc TIMESTAMP, so, OFFSET substracted and result is of UTC TIMESTAMP format
      assert [[{{2022, 10, 20}, {6, 0, 0, _microseconds = 123_456}}]] =
               Duckdbex.fetch_all(r)
    end

    test "native input/output, negative UTC offset", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(
                 conn,
                 "SELECT * FROM table1 WHERE col1 = $1;",
                 [{{2022, 10, 20}, {10, 30, 10, _microseconds = 123_456, {-14, 21}}}]
               )

      # TIMESTAMPTZ converted to utc TIMESTAMP, so, OFFSET substracted and result is of UTC TIMESTAMP format
      assert [[{{2022, 10, 21}, {0, 51, 10, _microseconds = 123_456}}]] =
               Duckdbex.fetch_all(r)
    end

    test "binary input, native output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [
                 "2022-10-20 11:30:00.123456+05:30"
               ])

      assert [[{{2022, 10, 20}, {6, 0, 0, 123_456}}]] = Duckdbex.fetch_all(r)
    end
  end

  # Timestamp with microsecond precision (ignores time zone) (DATETIME)
  # https://duckdb.org/docs/sql/data_types/timestamp
  describe "TIMESTAMP" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1(col1 TIMESTAMP);")

      assert {:ok, _} =
               Duckdbex.query(conn, """
               INSERT INTO table1
               VALUES ('1992-09-20 11:30:00.123456789'), ('2022-10-20T23:59:59.999Z');
               """)

      %{conn: conn}
    end

    test "native input/output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [
                 {{2022, 10, 20}, {23, 59, 59, _microseconds = 999_000}}
               ])

      assert [[{{2022, 10, 20}, {23, 59, 59, _microseconds = 999_000}}]] = Duckdbex.fetch_all(r)
    end

    test "binary input, native output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [
                 "2022-10-20 23:59:59.999"
               ])

      assert [[{{2022, 10, 20}, {23, 59, 59, _microseconds = 999_000}}]] = Duckdbex.fetch_all(r)
    end

    test "invalid input", %{conn: conn} do
      invalid_inputs = [
        {{2022, 10, 20, 23}, {23, 59, 59, 999_000}},
        {{2022, 10}, {23, 59, 59, 999_000}},
        {{2022, 10, 23}, {23, 23, 59, 59, 999_000}},
        {{2022, 10, 23}, {23, 23}},
        {{2022, 10, 23}, {23, "23"}},
        {{"2022", 10, 23}, {23, "23"}},
        {"2022", 10, 23, 23, "23"},
        "sdfsdfsdf"
      ]

      Enum.each(invalid_inputs, fn invalid_input ->
        assert {:error, "invalid type of parameter #1"} =
                 Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1;", [invalid_input])
      end)
    end
  end

  # Time of day with microsecond precision (uses time zone)
  # https://duckdb.org/docs/sql/data_types/time
  describe "TIMETZ" do
    # Instances can be created using the type names as a keyword,
    # where the data must be formatted according
    # to the ISO 8601 format (hh:mm:ss[.zzzzzz][+-TT[:tt]]).

    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1(col1 TIMETZ);")

      assert {:ok, _} =
               Duckdbex.query(conn, """
               INSERT INTO table1
               VALUES ('11:30:00.123456'), ('11:30:00.123456+05:30'), ('10:30:10.123456-14:21');
               """)

      %{conn: conn}
    end

    test "native input/output, positive UTC offset", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(
                 conn,
                 "SELECT * FROM table1 WHERE col1 = $1;",
                 [
                   {_hh = 11, _mm = 30, _ss = 0, _zzzzzz = _microseconds = 123_456,
                    {_TT = 5, _tt = 30}}
                 ]
               )

      assert [[{11, 30, 0, _microseconds = 123_456, {5, 30}}]] =
               Duckdbex.fetch_all(r)
    end

    test "native input/output, negative UTC offset", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(
                 conn,
                 "SELECT * FROM table1 WHERE col1 = $1;",
                 [{10, 30, 10, _microseconds = 123_456, {-14, 21}}]
               )

      assert [[{10, 30, 10, _microseconds = 123_456, {-14, 21}}]] =
               Duckdbex.fetch_all(r)
    end

    test "binary input, native output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(
                 conn,
                 "SELECT * FROM table1 WHERE col1 = $1;",
                 ["10:30:10.123456-14:21"]
               )

      assert [[{10, 30, 10, _microseconds = 123_456, {-14, 21}}]] =
               Duckdbex.fetch_all(r)
    end

    test "invalid input", %{conn: conn} do
      invalid_inputs = [
        {50, 30, 10, 123_456, {-14, 21}},
        {-50, 30, 10, 123_456, {-14, 21}},
        {10, -30, 10, 123_456, {3, 21}},
        {10, 70, 10, 123_456, {3, 21}},
        {10, 30, -10, 123_456, {-13, 65}},
        {10, 30, 80, 123_456, {-13, 65}},
        {10, 30, 10, 123_456, {-17, 21}},
        {10, 30, 10, 123_456, {17, 21}},
        {10, 30, 10, 123_456, {-14, -21}},
        {10, 30, 10, 123_456, {-14, 63}}
      ]

      Enum.each(invalid_inputs, fn invalid_input ->
        assert {:error, "invalid type of parameter #1"} =
                 Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1;", [invalid_input])
      end)
    end

    test "duckdb bugs?", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "select TIMETZ '26:11:02.123456-04:20';")

      assert [[{26, 11, 2, 123_456, {-4, 20}}]] =
               Duckdbex.fetch_all(r)
    end
  end

  # Time of day with microsecond precision (ignores time zone)
  # https://duckdb.org/docs/sql/data_types/time
  describe "TIME" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1(col1 TIME);")

      assert {:ok, _} =
               Duckdbex.query(conn, "INSERT INTO table1 VALUES ('00:00:00'), ('01:59:59.999');")

      %{conn: conn}
    end

    test "native input/output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [
                 {1, 59, 59, _microseconds = 999_000}
               ])

      assert [[{1, 59, 59, _microseconds = 999_000}]] = Duckdbex.fetch_all(r)
    end

    test "binary input, native output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(
                 conn,
                 "SELECT * FROM table1 WHERE col1 = $1;",
                 ["00:00:00"]
               )

      assert [[{0, 0, 0, _microseconds = 0}]] = Duckdbex.fetch_all(r)
    end

    test "invalid input", %{conn: conn} do
      invalid_inputs = [
        {50, 30, 10, 123_456},
        {-50, 30, 10, 123_456},
        {10, -30, 10, 123_456},
        {10, 70, 10, 123_456},
        {10, 30, -10, 123_456},
        {10, 30, 80, 123_456},
        {10, 30, 10, -23}
      ]

      Enum.each(invalid_inputs, fn invalid_input ->
        assert {:error, "invalid type of parameter #1"} =
                 Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1;", [invalid_input])
      end)
    end
  end

  # "https://duckdb.org/docs/sql/data_types/date"
  describe "DATE" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1(col1 DATE);")

      assert {:ok, _} =
               Duckdbex.query(conn, "INSERT INTO table1 VALUES ('1992-09-20'), ('2024-09-20');")

      %{conn: conn}
    end

    test "native input/output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [{2024, 9, 20}])

      assert [[{2024, 9, 20}]] = Duckdbex.fetch_all(r)
    end

    test "binary input, native output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(
                 conn,
                 "SELECT * FROM table1 WHERE col1 = $1;",
                 ["2024-09-20"]
               )

      assert [[{2024, 9, 20}]] = Duckdbex.fetch_all(r)
    end

    test "invalid input", %{conn: conn} do
      invalid_inputs = [
        {50, 30, 10},
        "22wfddasda"
      ]

      Enum.each(invalid_inputs, fn invalid_input ->
        assert {:error, "invalid type of parameter #1"} =
                 Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1;", [invalid_input])
      end)
    end
  end

  # "https://duckdb.org/docs/sql/data_types/overview"
  describe "UUID" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1(col1 UUID);")

      assert {:ok, _} =
               Duckdbex.query(conn, """
                 INSERT INTO table1
                 VALUES ('5e740554-23ad-11ed-861d-0242ac120002'), ('6dba373a-150c-4e41-a54c-383cf7352c6a');
               """)

      %{conn: conn}
    end

    test "binary input/output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [
                 "5e740554-23ad-11ed-861d-0242ac120002"
               ])

      assert [["5e740554-23ad-11ed-861d-0242ac120002"]] =
               Duckdbex.fetch_all(r)
    end

    test "hugeint input, binary output", %{conn: conn} do
      hugeint = Duckdbex.integer_to_hugeint(0x5E74055423AD11ED861D0242AC120002)

      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [hugeint])

      assert [["5e740554-23ad-11ed-861d-0242ac120002"]] =
               Duckdbex.fetch_all(r)
    end
  end

  # BLOB variable-length binary data
  # While blobs can hold objects up to 4 GB in size,
  # typically it is not recommended to store very large objects within the database system.
  # https://duckdb.org/docs/sql/data_types/blob
  describe "BLOB(BYTEA, BINARY, VARBINARY)" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1(col1 BLOB);")

      assert {:ok, _} =
               Duckdbex.query(
                 conn,
                 "INSERT INTO table1 VALUES ('hello!'::BLOB), ('world'::BLOB);"
               )

      %{conn: conn}
    end

    test "iolist input/output", %{conn: conn} do
      data = ~c"world"
      assert [119, 111, 114, 108, 100] == data
      assert {:ok, r} = Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [data])
      assert [["world"]] = Duckdbex.fetch_all(r)
    end

    test "binary input/output", %{conn: conn} do
      assert {:ok, r} = Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", ["world"])
      assert [["world"]] = Duckdbex.fetch_all(r)
    end

    #  Bitstrings with an arbitrary bit length have no support yet.
    # https://www.erlang.org/docs/25/man/erl_nif#enif_inspect_binary
    test "bitstring input/output", %{conn: conn} do
      assert {:error, "invalid type of parameter #1"} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 = $1", [<<1::1, 1::1>>])
    end
  end

  # VARCHAR(n) Variable-length character string. The maximum length n has no effect and is only provided for compatibility.
  # https://duckdb.org/docs/sql/data_types/text
  describe "VARCHAR(n) (CHAR(n), BPCHAR(n), STRING(n), TEXT(n))" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1(col1 VARCHAR(1));")

      assert {:ok, _} = Duckdbex.query(conn, "INSERT INTO table1 VALUES ('hello!'), ('world');")

      %{conn: conn}
    end

    test "native input/output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 == $1;", ["world"])

      assert [["world"]] = Duckdbex.fetch_all(r)
    end
  end

  # VARCHAR Variable-length character string
  # https://duckdb.org/docs/sql/data_types/text
  describe "VARCHAR(CHAR, BPCHAR, STRING, TEXT)" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1(col1 VARCHAR);")

      assert {:ok, _} = Duckdbex.query(conn, "INSERT INTO table1 VALUES ('hello!'), ('world');")

      %{conn: conn}
    end

    test "native input/output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 == $1;", ["world"])

      assert [["world"]] = Duckdbex.fetch_all(r)
    end
  end

  # BOOLEAN logical boolean (true/false)
  # https://duckdb.org/docs/sql/data_types/boolean
  describe "BOOLEAN(BOOL)" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1(col1 BOOLEAN);")

      assert {:ok, _} =
               Duckdbex.query(conn, "INSERT INTO table1 VALUES (true), (false), (NULL::BOOLEAN);")

      %{conn: conn}
    end

    test "native input/output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 == $1;", [false])

      assert [[false]] = Duckdbex.fetch_all(r)
    end

    test "nil input/output", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 == $1;", [nil])

      assert [] = Duckdbex.fetch_all(r)
    end
  end

  # "https://duckdb.org/docs/sql/data_types/nulls"
  describe "NULL" do
    setup %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "CREATE TABLE table1(col1 VARCHAR);")
      %{conn: conn}
    end

    test "insert, select", %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "INSERT INTO table1 VALUES ($1)", [nil])
      assert {:ok, r} = Duckdbex.query(conn, "SELECT * FROM table1 WHERE col1 IS NULL")
      [[nil]] = Duckdbex.fetch_all(r)
    end
  end
end
