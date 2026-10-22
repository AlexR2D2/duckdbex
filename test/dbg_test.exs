defmodule Duckdbex.Nif.DBGTest do
  use ExUnit.Case

  defp config_value(%{default: value}) when not is_nil(value), do: value

  defp config_value(%{name: "access_mode"}), do: "automatic"
  defp config_value(%{name: "allocator_bulk_deallocation_flush_threshold"}), do: "512.0 MiB"
  defp config_value(%{name: "allocator_flush_threshold"}), do: "128.0 MiB"
  defp config_value(%{name: "checkpoint_threshold"}), do: "16MB"
  defp config_value(%{name: "custom_user_agent"}), do: "duckdbex"
  defp config_value(%{name: "extension_directory"}), do: "/tmp"
  defp config_value(%{name: "file_search_path"}), do: "/tmp"
  defp config_value(%{name: "home_directory"}), do: "/tmp"
  defp config_value(%{name: "http_logging_output"}), do: "/tmp/duckdbex_http.log"
  defp config_value(%{name: "http_proxy"}), do: "http://localhost:8080"
  defp config_value(%{name: "http_proxy_password"}), do: "duckdbex"
  defp config_value(%{name: "http_proxy_username"}), do: "duckdbex"
  defp config_value(%{name: "max_memory"}), do: "1GB"
  defp config_value(%{name: "memory_limit"}), do: "1GB"
  defp config_value(%{name: "password"}), do: "duckdbex"
  defp config_value(%{name: "temp_directory"}), do: "/tmp"
  defp config_value(%{name: "user"}), do: "duckdbex"
  defp config_value(%{name: "username"}), do: "duckdbex"
  defp config_value(%{type: :boolean}), do: true
  defp config_value(%{type: :list}), do: []
  defp config_value(%{type: :double}), do: 0.8
  defp config_value(%{type: type}) when type in [:bigint, :ubigint], do: 1
  defp config_value(%{type: :string}), do: ""
  defp config_value(%{type: :map}), do: %{}

  test "dbg config options that fail with create_config" do
    failures =
      Enum.flat_map(Duckdbex.get_config_options(), fn option ->
        with {:ok, config} <- Duckdbex.create_config(),
             :ok <- Duckdbex.set_config_option(config, option.name, config_value(option)),
             {:ok, _db} <- Duckdbex.open(":memory:", config) do
          []
        else
          other -> [{option.name, other}]
        end
      end)

    if System.get_env("DUCKDBEX_DBG") == "1" do
      IO.inspect(failures, label: "config option failures")
    end

    assert is_list(failures)
  end
end
