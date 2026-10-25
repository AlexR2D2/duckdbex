defmodule Duckdbex.NamedParametersTest do
  use ExUnit.Case, async: true

  setup do
    assert {:ok, db} = Duckdbex.open(":memory:")
    assert {:ok, conn} = Duckdbex.connection(db)
    %{conn: conn}
  end

  describe "query/3 with a map" do
    test "binds named parameters", %{conn: conn} do
      assert {:ok, r} = Duckdbex.query(conn, "SELECT $name::VARCHAR;", %{"name" => "world"})
      assert [["world"]] = Duckdbex.fetch_all(r)
    end

    test "binds multiple named parameters", %{conn: conn} do
      assert {:ok, r} =
               Duckdbex.query(conn, "SELECT $left::INTEGER + $right::INTEGER;", %{
                 "left" => 1,
                 "right" => 2
               })

      assert [[3]] = Duckdbex.fetch_all(r)
    end

    test "matches names case-insensitively", %{conn: conn} do
      assert {:ok, r} = Duckdbex.query(conn, "SELECT $name::VARCHAR;", %{"NAME" => "world"})
      assert [["world"]] = Duckdbex.fetch_all(r)
    end

    test "accepts atom keys", %{conn: conn} do
      assert {:ok, r} = Duckdbex.query(conn, "SELECT $name::VARCHAR;", %{name: "world"})
      assert [["world"]] = Duckdbex.fetch_all(r)
    end

    test "rejects unknown named parameters", %{conn: conn} do
      assert {:error, "unknown named parameter \"nope\""} =
               Duckdbex.query(conn, "SELECT $name::VARCHAR;", %{"nope" => "world"})
    end

    test "rejects values of the wrong type", %{conn: conn} do
      assert {:error, "invalid value for named parameter \"value\""} =
               Duckdbex.query(conn, "SELECT $value::INTEGER;", %{"value" => "not an integer"})
    end

    test "reports missing named parameters without crashing", %{conn: conn} do
      assert {:error, message} = Duckdbex.query(conn, "SELECT $name::VARCHAR;", %{})
      assert message =~ "name"
    end

    test "falls back to user variables", %{conn: conn} do
      assert {:ok, _} = Duckdbex.query(conn, "SET VARIABLE answer = 42;")
      assert {:ok, r} = Duckdbex.query(conn, "SELECT $answer::INTEGER;", %{})
      assert [[42]] = Duckdbex.fetch_all(r)
    end
  end

  describe "execute_statement/2 with a map" do
    test "binds named parameters", %{conn: conn} do
      assert {:ok, stmt} = Duckdbex.prepare_statement(conn, "SELECT $value::INTEGER;")

      assert {:ok, r} = Duckdbex.execute_statement(stmt, %{"value" => 1})
      assert [[1]] = Duckdbex.fetch_all(r)
    end
  end

  describe "get_named_parameters/1" do
    test "returns names and types", %{conn: conn} do
      assert {:ok, stmt} =
               Duckdbex.prepare_statement(conn, "SELECT $name::VARCHAR, $count::INTEGER;")

      assert [{"count", :integer}, {"name", :string}] =
               Enum.sort(Duckdbex.get_named_parameters(stmt))
    end

    test "returns an empty list for positional parameters", %{conn: conn} do
      assert {:ok, stmt} = Duckdbex.prepare_statement(conn, "SELECT $1::INTEGER;")
      assert [] = Duckdbex.get_named_parameters(stmt)
    end
  end
end
