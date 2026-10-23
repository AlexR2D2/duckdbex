defmodule Duckdbex.QuackTest do
  use ExUnit.Case, async: false

  # Quack is a pre-release extension and `INSTALL quack` needs network access.
  # Run with: mix test --include quack
  @moduletag :quack

  test "serves a database and queries it over the quack protocol" do
    assert {:ok, db} = Duckdbex.open(":memory:")
    assert {:ok, server} = Duckdbex.connection(db)
    assert {:ok, client} = Duckdbex.connection(db)

    assert {:ok, _} = Duckdbex.query(server, "INSTALL quack; LOAD quack;")
    assert {:ok, _} = Duckdbex.query(server, "CREATE TABLE main.hello (s VARCHAR);")
    assert {:ok, _} = Duckdbex.query(server, "INSERT INTO main.hello VALUES ('world');")

    assert {:ok, r} =
             Duckdbex.query(
               server,
               "CALL quack_serve('quack:localhost', token = 'duckdbex_test');"
             )

    assert [["quack:localhost", _url, "duckdbex_test"]] = Duckdbex.fetch_all(r)

    assert {:ok, _} = Duckdbex.query(client, "INSTALL quack; LOAD quack;")
    assert {:ok, _} = Duckdbex.query(client, "CREATE SECRET (TYPE quack, TOKEN 'duckdbex_test');")
    assert {:ok, _} = Duckdbex.query(client, "ATTACH 'quack:localhost' AS remote;")

    assert {:ok, r} = Duckdbex.query(client, "SELECT * FROM remote.hello;")
    assert [["world"]] = Duckdbex.fetch_all(r)
  end
end
