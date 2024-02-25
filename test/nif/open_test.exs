defmodule Duckdbex.Nif.OpenTest do
  use ExUnit.Case

  test "opens DB using file" do
    assert {:ok, _} = Duckdbex.NIF.open("test_duckdb", nil)
    File.rm("test_duckdb")
  end

  test "opens DB in memory" do
    assert {:ok, _} = Duckdbex.NIF.open(":memory:", nil)
  end

  test "directory of DB file not found" do
    assert {:error, <<"{\"exception_type\":\"IO\",\"exception_message\":\"Cannot open file", _rest::binary>>} =
             Duckdbex.NIF.open("/root/user/asf#fdscgwgj4/db.duckdb", nil)
  end
end
