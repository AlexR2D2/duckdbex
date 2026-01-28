defmodule Duckdbex.Nif.ConfigTest do
  use ExUnit.Case

  test "config defaults" do
    config = %Duckdbex.Config{}
    assert config.extension_directory == System.get_env("DUCKDBEX_EXTENSION_DIRECTORY")
    assert config.temporary_directory == System.get_env("DUCKDBEX_TEMPORARY_DIRECTORY")
  end

  test "explicit nil config" do
    assert {:ok, _db} = Duckdbex.open(":memory:", nil)
  end

  test "invalid config type" do
    assert_raise(ArgumentError, fn ->
      Duckdbex.open(":memory:", invalid: "config")
    end)
  end

  test "open with default config" do
    assert {:ok, _db} = Duckdbex.open(":memory:", %Duckdbex.Config{})
  end

  test "erlang memory allocator" do
    {:ok, _db} = Duckdbex.open(":memory:", %Duckdbex.Config{memory_allocator: :erlang})
  end

  test "disabled optimizers" do
    {:ok, _db} =
      Duckdbex.open(":memory:", %Duckdbex.Config{
        disabled_optimizers: [:invalid, :expression_rewriter]
      })
  end

  test "user options" do
    {:ok, _db} =
      Duckdbex.open(":memory:", %Duckdbex.Config{
        user_options: [{"my_option", 42}, {"option2", "mem"}]
      })
  end

  test "invalid config option" do
    assert_raise(ArgumentError, fn ->
      Duckdbex.open(":memory:", %{invalid: "config"})
    end)
  end
end
