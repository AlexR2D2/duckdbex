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
end
