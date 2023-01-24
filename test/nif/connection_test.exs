defmodule Duckdbex.Nif.ConnectionTest do
  use ExUnit.Case

  test "connection" do
    {:ok, db} = Duckdbex.NIF.open(":memory:", nil)
    assert match? {:ok, _}, Duckdbex.NIF.connection(db)
  end
end
