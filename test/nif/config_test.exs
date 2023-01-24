defmodule Duckdbex.Nif.ConfigTest do
  use ExUnit.Case

  alias Duckdbex.NIF

  test "no config" do
    assert {:ok, _db} = NIF.open(":memory:", nil)
  end

  test "invalid config type" do
    assert_raise(ArgumentError, fn ->
      NIF.open(":memory:", [invalid: "config"])
    end)
  end

  test "default config" do
    assert {:ok, _db} = NIF.open(":memory:", %Duckdbex.Config{})
  end

  test "erlang memory allocator" do
    {:ok, _db} = NIF.open(":memory:", %Duckdbex.Config{memory_allocator: :erlang})
  end

  test "invalid config option" do
    assert_raise(ArgumentError, fn ->
      NIF.open(":memory:", %{invalid: "config"})
    end)
  end
end
