defmodule Duckdbex.IssuesTest do
  use ExUnit.Case

  setup ctx do
    {:ok, db} = Duckdbex.open(":memory:", nil)
    {:ok, conn} = Duckdbex.connection(db)
    Map.put(ctx, :conn, conn)
  end

  describe "https://github.com/AlexR2D2/duckdbex/issues/27#issue-2621748162" do
    test "appender failed when column is of MAP(text, UNION(text, text[])) type", %{conn: conn} do
      {:ok, _} = Duckdbex.query(conn, "create schema schema_1;")

      {:ok, _} =
        Duckdbex.query(conn, """
          CREATE TABLE schema_1.appender_test_1(
            attributes map(text, UNION(single_text text, multiple_text text[])),
          );
        """)

      assert {:ok, appender} =
               Duckdbex.appender(conn, "schema_1", "appender_test_1")

      # a row
      assert :ok =
               Duckdbex.appender_add_row(appender, [
                 # attributes col
                 [{"key1", {"single_text", "sval1"}}]
               ])

      # rows
      assert :ok =
               Duckdbex.appender_add_rows(appender, [
                 # row
                 [
                   # attributes col
                   [{"key2", {"multiple_text", ["mval1", "mval2"]}}]
                 ],
                 # row
                 [
                   # attributes col
                   [{"key3", {"single_text", "sval2"}}]
                 ]
               ])

      assert :ok = Duckdbex.appender_flush(appender)

      {:ok, r} = Duckdbex.query(conn, "SELECT * FROM schema_1.appender_test_1;")

      assert [
               [[{"key1", {"single_text", "sval1"}}]],
               [[{"key2", {"multiple_text", ["mval1", "mval2"]}}]],
               [[{"key3", {"single_text", "sval2"}}]]
             ] = Duckdbex.fetch_all(r)
    end
  end

  describe "https://github.com/AlexR2D2/duckdbex/pull/28" do
    test "insert union without params", %{conn: conn} do
      Duckdbex.query(
        conn,
        """
          CREATE TABLE test_table(
            test_field UNION(single_text text, multiple_text text[])
          );
        """
      )

      assert {:ok, stmt} =
               Duckdbex.prepare_statement(
                 conn,
                 """
                   INSERT INTO test_table (test_field)
                   VALUES ('test')
                 """
               )

      assert {:ok, _res} = Duckdbex.execute_statement(stmt)

      assert {:ok, stmt} = Duckdbex.prepare_statement(conn, "SELECT * FROM test_table;")
      assert {:ok, res} = Duckdbex.execute_statement(stmt)
      assert [[{"single_text", "test"}]] = Duckdbex.fetch_all(res)
    end

    test "insert union with params", %{conn: conn} do
      Duckdbex.query(
        conn,
        """
          CREATE TABLE test_table(
            test_field UNION(single_text text, multiple_text text[])
          );
        """
      )

      assert {:ok, stmt} =
               Duckdbex.prepare_statement(
                 conn,
                 """
                   INSERT INTO test_table (test_field)
                   VALUES (?), (?)
                 """
               )

      assert {:ok, _res} =
               Duckdbex.execute_statement(stmt, [
                 {"multiple_text", ["one", "two"]},
                 {"single_text", "one"}
               ])

      assert {:ok, stmt} = Duckdbex.prepare_statement(conn, "SELECT * FROM test_table;")
      assert {:ok, res} = Duckdbex.execute_statement(stmt)

      assert [[{"multiple_text", ["one", "two"]}], [{"single_text", "one"}]] =
               Duckdbex.fetch_all(res)
    end

    test "insert struct without params", %{conn: conn} do
      Duckdbex.query(conn, """
        create type test_struct as struct(
          test_field text
        );
      """)

      Duckdbex.query(conn, """
        CREATE TABLE test_table(
          test_struct test_struct
        );
      """)

      assert {:ok, stmt} =
               Duckdbex.prepare_statement(conn, """
                 INSERT INTO test_table (test_struct)
                 VALUES (struct_pack(test_field := 'test'))
               """)

      assert {:ok, _res} = Duckdbex.execute_statement(stmt)

      assert {:ok, stmt} = Duckdbex.prepare_statement(conn, "SELECT * FROM test_table;")
      assert {:ok, res} = Duckdbex.execute_statement(stmt)
      assert [[%{"test_field" => "test"}]] = Duckdbex.fetch_all(res)
    end

    test "insert struct with struct as param", %{conn: conn} do
      Duckdbex.query(conn, """
        create type test_struct as struct(
          test_field text
        );
      """)

      Duckdbex.query(conn, """
        CREATE TABLE test_table(
          test_struct test_struct
        );
      """)

      assert {:ok, stmt} =
               Duckdbex.prepare_statement(conn, """
                 INSERT INTO test_table (test_struct)
                 VALUES (?)
               """)

      assert {:ok, _res} =
               Duckdbex.execute_statement(stmt, [%{"test_field" => "test_value"}])

      assert {:ok, stmt} = Duckdbex.prepare_statement(conn, "SELECT * FROM test_table;")
      assert {:ok, res} = Duckdbex.execute_statement(stmt)
      assert [[%{"test_field" => "test_value"}]] = Duckdbex.fetch_all(res)
    end

    test "insert struct with a value of strut fields as param", %{conn: conn} do
      Duckdbex.query(conn, """
        create type test_struct as struct(
          test_field text
        );
      """)

      Duckdbex.query(conn, """
        CREATE TABLE test_table(
          test_struct test_struct
        );
      """)

      assert {:ok, stmt} =
               Duckdbex.prepare_statement(conn, """
                 INSERT INTO test_table (test_struct)
                 VALUES (struct_pack(test_field := ?))
               """)

      # TODO: create an issue to DuckDB?
      # DuckDB PreparedStatement.GetExpectedParameterTypes() do not returns requiered parameters
      # Is this a bug?

      assert {:error,
              "Invalid Input Error: Values were not provided for the following prepared statement parameters: 1"} =
               Duckdbex.execute_statement(stmt, ["test_value"])
    end
  end
end
