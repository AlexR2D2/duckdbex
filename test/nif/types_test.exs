defmodule Duckdbex.Nif.QueryTest do
  use ExUnit.Case

  alias Duckdbex.NIF

  setup ctx do
    {:ok, db} = NIF.open(":memory:", nil)
    {:ok, conn} = NIF.connection(db)
    Map.put(ctx, :conn, conn)
  end

  test "NULL", %{conn: conn} do
    assert {:ok, r} = NIF.query(conn, "SELECT NULL")
    [[nil]] = NIF.fetch_all(r)
  end

  test "DATE", %{conn: conn} do
    assert {:ok, _} = NIF.query(conn, "CREATE TABLE dates(value DATE);")

    assert {:ok, _} =
             NIF.query(
               conn,
               "INSERT INTO dates VALUES ('2000-01-01'), ('2022-01-31'), ('1999-12-31');"
             )

    assert {:ok, r} =
             NIF.query(
               conn,
               "SELECT * FROM dates WHERE value = $1;",
               ["1999-12-31"]
             )

    assert [[{1999, 12, 31}]] =
             NIF.fetch_all(r)
  end

  test "TIME", %{conn: conn} do
    assert {:ok, _} = NIF.query(conn, "CREATE TABLE times(value TIME);")

    assert {:ok, _} =
             NIF.query(
               conn,
               "INSERT INTO times VALUES ('00:00:00'), ('01:59:59.999'), ('12:00:00');"
             )

    assert {:ok, r} =
             NIF.query(
               conn,
               "SELECT * FROM times WHERE value = $1;",
               ["01:59:59.999"]
             )

    assert [[{1, 59, 59, 999_000}]] =
             NIF.fetch_all(r)
  end

  test "TIMESTAMP", %{conn: conn} do
    assert {:ok, _} = NIF.query(conn, "CREATE TABLE timestampts(value TIMESTAMP);")

    assert {:ok, _} =
             NIF.query(conn, """
               INSERT INTO timestampts VALUES
                 ('2022-10-10T00:00:00.000Z'),
                 ('2022-10-20T23:59:59.999Z'),
                 ('2022-09-01T12:00:00'),
                 ('2022-01-01T00:00:00.000+01:00');
             """)

    assert {:ok, r} =
             NIF.query(
               conn,
               "SELECT * FROM timestampts WHERE value = $1;",
               ["2022-10-20 23:59:59.999"]
             )

    assert [[{{2022, 10, 20}, {23, 59, 59, 999_000}}]] =
             NIF.fetch_all(r)
  end

  test "TIMESTAMPTZ", %{conn: conn} do
    assert {:ok, _} = NIF.query(conn, "CREATE TABLE timestamptzs(value TIMESTAMPTZ);")

    assert {:ok, _} =
             NIF.query(conn, """
               INSERT INTO timestamptzs VALUES
                 ('2022-10-10T00:00:00.000Z'),
                 ('2022-10-20T23:59:59.999Z'),
                 ('2022-09-01T12:00:00'),
                 ('2022-01-01T00:00:00.000+01:00');
             """)

    assert {:ok, r} =
             NIF.query(
               conn,
               "SELECT * FROM timestamptzs WHERE value = $1;",
               ["2022-10-20 23:59:59.999+00"]
             )

    assert [[{{2022, 10, 20}, {23, 59, 59, 999_000}}]] =
             NIF.fetch_all(r)
  end

  test "HUGEINT", %{conn: conn} do
    assert {:ok, _} = NIF.query(conn, "CREATE TABLE hugeints(value HUGEINT);")

    assert {:ok, _} =
             NIF.query(
               conn,
               "INSERT INTO hugeints VALUES (-10::hugeint), (5::hugeint), (10::hugeint), (98233720368547758080000::hugeint);"
             )

    assert {:ok, r} = NIF.query(conn, "SELECT * FROM hugeints WHERE value = $1;", ["5"])
    assert [[{0, 5}]] = NIF.fetch_all(r)

    assert {:ok, r} =
             NIF.query(conn, "SELECT * FROM hugeints WHERE value = $1;", [
               "98233720368547758080000"
             ])

    assert [[{5325, 4_808_176_044_395_724_800}]] = NIF.fetch_all(r)

    assert {:ok, r} = NIF.query(conn, "SELECT * FROM hugeints WHERE value = $1;", [-10])
    assert [[{-1, 18_446_744_073_709_551_606}]] = NIF.fetch_all(r)

    hugeint = Duckdbex.integer_to_hugeint(98_233_720_368_547_758_080_000)

    assert {:ok, r} =
             NIF.query(conn, "SELECT * FROM hugeints WHERE value = $1", [{:hugeint, hugeint}])

    assert [[{5325, 4_808_176_044_395_724_800}]] = NIF.fetch_all(r)
  end

  test "DECIMAL", %{conn: conn} do
    assert {:ok, _} = NIF.query(conn, "CREATE TABLE decimals(value DECIMAL(5,3));")

    assert {:ok, _} =
             NIF.query(
               conn,
               "INSERT INTO decimals VALUES (3.14::decimal), (0.8), (-22.174::decimal), (10::decimal);"
             )

    assert {:ok, r} =
             NIF.query(conn, "SELECT * FROM decimals WHERE value = $1", [
               {:decimal, {-22174, 5, 3}}
             ])

    # {value, width, scale}
    assert [[{-22174, 5, 3}]] = NIF.fetch_all(r)

    assert {:ok, r} = NIF.query(conn, "SELECT * FROM decimals WHERE value = $1", [0.8])
    assert [[{800, 5, 3}]] = NIF.fetch_all(r)

    assert {:ok, r} = NIF.query(conn, "SELECT * FROM decimals WHERE value = $1", ["3.14"])
    assert [[{3140, 5, 3}]] = NIF.fetch_all(r)

    hugeint = Duckdbex.integer_to_hugeint(10)

    assert {:ok, r} =
             NIF.query(conn, "SELECT * FROM decimals WHERE value = $1", [{:hugeint, hugeint}])

    assert [[{10000, 5, 3}]] = NIF.fetch_all(r)
  end

  test "REAL", %{conn: conn} do
    assert {:ok, _} = NIF.query(conn, "CREATE TABLE reals(value REAL);")
    assert {:ok, _} = NIF.query(conn, "INSERT INTO reals VALUES (3.14), (0.8), (-2.17);")

    assert {:ok, r} =
             NIF.query(conn, "SELECT * FROM reals WHERE value > $1 AND value <= $2;", [
               -2.18,
               0.81
             ])

    assert [_, _] = NIF.fetch_all(r)
  end

  test "SMALLINT", %{conn: conn} do
    assert {:ok, _} = NIF.query(conn, "CREATE TABLE smallint(value SMALLINT);")
    assert {:ok, _} = NIF.query(conn, "INSERT INTO smallint VALUES (1), (-1);")
    assert {:ok, r} = NIF.query(conn, "SELECT * FROM smallint WHERE value = $1;", [-1])
    assert [_] = NIF.fetch_all(r)
  end

  test "UBIGINT", %{conn: conn} do
    assert {:ok, _} = NIF.query(conn, "CREATE TABLE ubigints(value UBIGINT);")
    assert {:ok, _} = NIF.query(conn, "INSERT INTO ubigints VALUES (9223372036854775890);")

    assert {:ok, r} =
             NIF.query(conn, "SELECT * FROM ubigints WHERE value = $1", [
               9_223_372_036_854_775_890
             ])

    assert [_] = NIF.fetch_all(r)
  end

  test "UINTEGER", %{conn: conn} do
    assert {:ok, _} = NIF.query(conn, "CREATE TABLE uintegers(value UINTEGER);")
    assert {:ok, _} = NIF.query(conn, "INSERT INTO uintegers VALUES (1), (0), (245);")
    assert {:ok, r} = NIF.query(conn, "SELECT * FROM uintegers WHERE value = $1", [0])
    assert [_] = NIF.fetch_all(r)
  end

  test "USMALLINT", %{conn: conn} do
    assert {:ok, _} = NIF.query(conn, "CREATE TABLE usmallints(value USMALLINT);")
    assert {:ok, _} = NIF.query(conn, "INSERT INTO usmallints VALUES (0), (1);")
    assert {:ok, r} = NIF.query(conn, "SELECT * FROM usmallints WHERE value = $1;", [1])
    assert [_] = NIF.fetch_all(r)
  end

  test "UTINYINT", %{conn: conn} do
    assert {:ok, _} = NIF.query(conn, "CREATE TABLE utinyints(value UTINYINT);")
    assert {:ok, _} = NIF.query(conn, "INSERT INTO utinyints VALUES (0), (255);")

    assert {:ok, r} =
             NIF.query(conn, "SELECT * FROM utinyints WHERE value = $1 OR value = $2;", [1, 255])

    assert [_] = NIF.fetch_all(r)
  end

  test "TINYINT", %{conn: conn} do
    assert {:ok, _} = NIF.query(conn, "CREATE TABLE tinyints(value TINYINT);")
    assert {:ok, _} = NIF.query(conn, "INSERT INTO tinyints VALUES (1), (127), (-127);")

    assert {:ok, r} =
             NIF.query(conn, "SELECT * FROM tinyints WHERE value = $1 OR value = $2;", [1, -127])

    assert [_, _] = NIF.fetch_all(r)
  end

  test "BLOB", %{conn: conn} do
    assert {:ok, _} = NIF.query(conn, "CREATE TABLE blobs(value BLOB);")

    assert {:ok, _} =
             NIF.query(conn, "INSERT INTO blobs VALUES ('hello!'::BLOB), ('world'::BLOB);")

    assert {:ok, r} = NIF.query(conn, "SELECT * FROM blobs WHERE value = $1", ["world"])
    assert [_] = NIF.fetch_all(r)
  end

  test "INTERVAL", %{conn: conn} do
    assert {:ok, _} = NIF.query(conn, "CREATE TABLE intervals(value INTERVAL);")

    assert {:ok, _} =
             NIF.query(
               conn,
               "INSERT INTO intervals VALUES (INTERVAL 1 HOUR), (INTERVAL 1 DAY), (INTERVAL 2 MONTHS);"
             )

    # returns interval in milliseconds
    assert {:ok, r} = NIF.query(conn, "SELECT * FROM intervals WHERE value = $1", ["1 HOUR"])
    assert [_] = NIF.fetch_all(r)
  end

  test "UUID", %{conn: conn} do
    assert {:ok, _} = NIF.query(conn, "CREATE TABLE uuids(value UUID);")

    assert {:ok, _} =
             NIF.query(conn, """
               INSERT INTO uuids VALUES ('5e740554-23ad-11ed-861d-0242ac120002'), ('6dba373a-150c-4e41-a54c-383cf7352c6a');
             """)

    assert {:ok, r} =
             NIF.query(conn, "SELECT * FROM uuids WHERE value = $1", [
               "5e740554-23ad-11ed-861d-0242ac120002"
             ])

    assert [["5e740554-23ad-11ed-861d-0242ac120002"]] =
             NIF.fetch_all(r)
  end

  test "VARCHAR", %{conn: conn} do
    assert {:ok, _} = NIF.query(conn, "CREATE TABLE varchars(value VARCHAR);")
    assert {:ok, _} = NIF.query(conn, "INSERT INTO varchars VALUES ('hello!'), ('world');")
    assert {:ok, r} = NIF.query(conn, "SELECT * FROM varchars WHERE value = $1", ["world"])
    assert [_] = NIF.fetch_all(r)
  end

  test "BOOLEAN", %{conn: conn} do
    assert {:ok, _} = NIF.query(conn, "CREATE TABLE booleans(value BOOLEAN);")
    assert {:ok, _} = NIF.query(conn, "INSERT INTO booleans VALUES (true), (false);")

    assert {:ok, r} =
             NIF.query(conn, "SELECT * FROM booleans WHERE value = $1 OR value = $2", [
               true,
               false
             ])

    assert [_, _] = NIF.fetch_all(r)

    assert {:ok, r} = NIF.query(conn, "SELECT * FROM booleans WHERE value = $1", [true])
    assert [_] = NIF.fetch_all(r)
  end

  test "BIGINT", %{conn: conn} do
    assert {:ok, _} = NIF.query(conn, "CREATE TABLE bigints(value BIGINT);")

    assert {:ok, _} =
             NIF.query(
               conn,
               "INSERT INTO bigints VALUES (-9223372036854775808), (9223372036854775807);"
             )

    assert {:ok, r} =
             NIF.query(conn, "SELECT * FROM bigints WHERE value = $1 OR value = $2;", [
               -9_223_372_036_854_775_808,
               0
             ])

    assert [_] = NIF.fetch_all(r)
  end

  test "INTEGER", %{conn: conn} do
    assert {:ok, _} = NIF.query(conn, "CREATE TABLE integers(value INTEGER);")
    assert {:ok, _} = NIF.query(conn, "INSERT INTO integers VALUES (1), (0), (-245);")

    assert {:ok, r} =
             NIF.query(conn, "SELECT * FROM integers WHERE value = $1 OR value = $2;", [0, -245])

    assert [_, _] = NIF.fetch_all(r)
  end

  test "DOUBLE", %{conn: conn} do
    assert {:ok, _} = NIF.query(conn, "CREATE TABLE doubles(value DOUBLE);")
    assert {:ok, _} = NIF.query(conn, "INSERT INTO doubles VALUES (3.14), (0.8), (-2.17);")

    assert {:ok, r} =
             NIF.query(conn, "SELECT * FROM doubles WHERE value = $1 OR value = $2;", [0.8, -2.17])

    assert [_, _] = NIF.fetch_all(r)
  end

  test "ENUM", %{conn: conn} do
    assert {:ok, _} = NIF.query(conn, "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');")
    assert {:ok, _} = NIF.query(conn, "CREATE TABLE person (name text, current_mood mood);")

    assert {:ok, _} =
             NIF.query(
               conn,
               "INSERT INTO person VALUES ('Pedro','happy'), ('Mark', NULL), ('Pagliacci', 'sad'), ('Mr. Mackey', 'ok');"
             )

    assert {:ok, r} = NIF.query(conn, "SELECT * FROM person WHERE current_mood = 'sad';")
    assert [["Pagliacci", "sad"]] = NIF.fetch_all(r)

    assert {:ok, r} = NIF.query(conn, "SELECT * FROM person WHERE current_mood = $1;", ["sad"])
    assert [["Pagliacci", "sad"]] = NIF.fetch_all(r)
  end

  test "LIST", %{conn: conn} do
    assert {:ok, r} = NIF.query(conn, "SELECT [1, 2, 3];")
    assert [[[1, 2, 3]]] = NIF.fetch_all(r)

    assert {:ok, r} = NIF.query(conn, "SELECT ['duck', 'goose', NULL, 'heron'];")
    assert [[["duck", "goose", nil, "heron"]]] = NIF.fetch_all(r)

    assert {:ok, _} =
             NIF.query(conn, "CREATE TABLE list_table (int_list INT[], varchar_list VARCHAR[]);")

    assert {:ok, _} =
             NIF.query(
               conn,
               "INSERT INTO list_table VALUES ([1, 2], ['one', 'two']), ([4, 5], ['three', NULL]), ([6, 7], NULL);"
             )

    assert {:ok, r} = NIF.query(conn, "SELECT * FROM list_table")
    assert [[[1, 2], ["one", "two"]], [[4, 5], ["three", nil]], [[6, 7], nil]] = NIF.fetch_all(r)

    assert {:ok, r} = NIF.query(conn, "SELECT * FROM list_table WHERE int_list = [4, 5]")
    assert [[[4, 5], ["three", nil]]] = NIF.fetch_all(r)

    assert {:ok, r} = NIF.query(conn, "SELECT * FROM list_table WHERE int_list = [5, 4]")
    assert [] = NIF.fetch_all(r)

    assert {:ok, r} = NIF.query(conn, "SELECT * FROM list_table WHERE int_list = $1", [[4, 5]])
    assert [[[4, 5], ["three", nil]]] = NIF.fetch_all(r)

    assert {:ok, r} = NIF.query(conn, "SELECT * FROM list_table WHERE int_list = $1", [[]])
    assert [] = NIF.fetch_all(r)
  end

  test "MAP", %{conn: conn} do
    assert {:ok, r} = NIF.query(conn, "select map();")
    assert [[%{}]] = NIF.fetch_all(r)

    assert {:ok, r} = NIF.query(conn, "select map([], []);")
    assert [[%{}]] = NIF.fetch_all(r)

    assert {:ok, r} = NIF.query(conn, "select map([1, 5, 6], ['a', 'e', 'b']);")
    assert [[%{1 => "a", 5 => "e", 6 => "b"}]] = NIF.fetch_all(r)

    assert {:ok, r} = NIF.query(conn, "select map([1, 5], [NULL, 'e']);")
    assert [[%{1 => nil, 5 => "e"}]] = NIF.fetch_all(r)

    assert {:ok, r} = NIF.query(conn, "select map(['1', '5'], [1, 2]);")
    assert [[%{"1" => 1, "5" => 2}]] = NIF.fetch_all(r)

    assert {:error, "Conversion Error: Could not convert string \"a\" to DECIMAL(12,2)"} =
      NIF.query(conn, "select map([1, 2, 3], [1, 'a', 2.4::DECIMAL(3, 2)]);")

    assert {:ok, r} =
             NIF.query(conn, "select map([['a', 'b'], ['c', 'd']], [[1.1, 2.2], [3.3, 4.4]]);")

    assert [[%{["a", "b"] => [{11, 2, 1}, {22, 2, 1}], ["c", "d"] => [{33, 2, 1}, {44, 2, 1}]}]] =
             NIF.fetch_all(r)

    assert {:ok, r} = NIF.query(conn, "SELECT map([100, 5], [42, 43])[100];")
    assert [[[42]]] = NIF.fetch_all(r)

    assert {:ok, r} = NIF.query(conn, "SELECT map([100, 5], [42, 43])[100][1];")
    assert [[42]] = NIF.fetch_all(r)

    # Wait for a duckdb update
    # assert {:error, "Invalid Input Error: Map keys have to be unique"} =
    #  NIF.query(conn, "select map([1, 1, 1], ['a', 'b', 'c']);")

    assert {:error, "Invalid Input Error: Map keys can not be NULL"} =
             NIF.query(conn, "select map([NULL, 5], ['a', 'e']);")

    assert {:ok, _} = NIF.query(conn, "CREATE TABLE map_table (map_col MAP(INT, DOUBLE));")

    assert {:ok, _} =
             NIF.query(
               conn,
               "INSERT INTO map_table VALUES (map([1, 2], [2.98, 3.14])), (map([3, 4], [9.8, 1.6]));"
             )

    assert {:ok, r} = NIF.query(conn, "SELECT * FROM map_table")
    assert [[%{1 => 2.98, 2 => 3.14}], [%{3 => 9.8, 4 => 1.6}]] = NIF.fetch_all(r)

    assert {:ok, r} =
             NIF.query(conn, "SELECT * FROM map_table WHERE map_col = $1", [
               %{1 => 2.98, 2 => 3.14}
             ])

    assert [[%{1 => 2.98, 2 => 3.14}]] = NIF.fetch_all(r)
  end

  test "STRUCT", %{conn: conn} do
    assert {:ok, r} = NIF.query(conn, "SELECT {'x': 1, 'y': 2, 'z': 3};")
    assert [[%{"x" => 1, "y" => 2, "z" => 3}]] = NIF.fetch_all(r)

    assert {:ok, r} =
             NIF.query(
               conn,
               "SELECT {'yes': 'duck', 'maybe': 'goose', 'huh': NULL, 'no': 'heron'};"
             )

    assert [[%{"huh" => nil, "maybe" => "goose", "no" => "heron", "yes" => "duck"}]] =
             NIF.fetch_all(r)

    assert {:ok, r} = NIF.query(conn, "SELECT {'key1': 'string', 'key2': 1, 'key3': 12.345};")
    assert [[%{"key1" => "string", "key2" => 1, "key3" => {12345, 5, 3}}]] = NIF.fetch_all(r)

    assert {:ok, r} = NIF.query(conn, "SELECT struct_pack(key1 := 'value1',key2 := 42);")
    assert [[%{"key1" => "value1", "key2" => 42}]] = NIF.fetch_all(r)

    assert {:ok, r} =
             NIF.query(conn, """
               SELECT {
                 'birds': {'yes': 'duck', 'maybe': 'goose', 'huh': NULL, 'no': 'heron'},
                 'aliens': NULL,
                 'amphibians': {'yes':'frog', 'maybe': 'salamander', 'huh': 'dragon', 'no':'toad'}
               };
             """)

    assert [
             [
               %{
                 "aliens" => nil,
                 "amphibians" => %{
                   "huh" => "dragon",
                   "maybe" => "salamander",
                   "no" => "toad",
                   "yes" => "frog"
                 },
                 "birds" => %{"huh" => nil, "maybe" => "goose", "no" => "heron", "yes" => "duck"}
               }
             ]
           ] =
             NIF.fetch_all(r)

    # Create a struct from columns and/or expressions using the row function.
    # This returns {'x': 1, 'v2': 2, 'y': a}
    # did't work in the 0.9.1, because no values names were specified
    # assert {:ok, r} = NIF.query(conn, "SELECT row(x, x + 1, y) FROM (SELECT 1 as x, 'a' as y);")
    # assert [[%{"v2" => 2, "x" => 1, "y" => "a"}]] = NIF.fetch_all(r)

    # If using multiple expressions when creating a struct, the row function is optional
    # This also returns {'x': 1, 'v2': 2, 'y': a}
    # did't work in the 0.9.1, because no values names were specified
    # assert {:ok, r} = NIF.query(conn, "SELECT (x, x + 1, y) FROM (SELECT 1 as x, 'a' as y);")
    # assert [[%{"v2" => 2, "x" => 1, "y" => "a"}]] = NIF.fetch_all(r)

    # Add to a Struct of integers
    assert {:ok, r} = NIF.query(conn, "SELECT struct_insert({'a': 1, 'b': 2, 'c': 3}, d := 4);")
    assert [[%{"a" => 1, "b" => 2, "c" => 3, "d" => 4}]] = NIF.fetch_all(r)

    # Use dot notation to retrieve the value at a key's location. This returns 1
    # The subquery generates a struct column "a", which we then query with a.x
    assert {:ok, r} = NIF.query(conn, "SELECT a.x FROM (SELECT {'x':1, 'y':2, 'z':3} as a);")
    assert [[1]] = NIF.fetch_all(r)
  end

  test "UNION", %{conn: conn} do
    assert {:ok, _} = NIF.query(conn, "CREATE TABLE tbl1(u UNION(num INT, str VARCHAR));")
    assert {:ok, r} = NIF.query(conn, "SELECT u from tbl1;")
    assert [] = NIF.fetch_all(r)

    assert {:ok, _} =
             NIF.query(
               conn,
               "INSERT INTO tbl1 values (1) , ('two') , (union_value(str := 'three'));"
             )

    assert {:ok, r} = NIF.query(conn, "SELECT u from tbl1;")
    assert [[%{"num" => 1}], [%{"str" => "two"}], [%{"str" => "three"}]] = NIF.fetch_all(r)

    assert {:ok, r} = NIF.query(conn, "SELECT union_extract(u, 'str') FROM tbl1;")
    assert [[nil], ["two"], ["three"]] = NIF.fetch_all(r)

    assert {:ok, r} = NIF.query(conn, "SELECT u.str FROM tbl1;")
    assert [[nil], ["two"], ["three"]] = NIF.fetch_all(r)

    assert {:ok, r} = NIF.query(conn, "SELECT union_tag(u) FROM tbl1;")
    assert [["num"], ["str"], ["str"]] = NIF.fetch_all(r)
  end
end
