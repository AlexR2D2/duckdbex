defmodule Duckdbex.AppenderLifetimeTest do
  use ExUnit.Case

  describe "appender exceptions" do
    for operation <- ["flush", "close"] do
      @tag timeout: 30_000
      test "#{operation} returns an error after the connection is collected" do
        run_fixture("collected_connection", unquote(operation))
      end

      @tag timeout: 30_000
      test "#{operation} returns a constraint error without crashing BEAM" do
        run_fixture("constraint_error", unquote(operation))
      end
    end

    for operation <- ["add_row", "add_rows"] do
      @tag timeout: 30_000
      test "#{operation} returns a conversion error without crashing BEAM" do
        run_fixture("conversion_error", unquote(operation))
      end
    end

    for arity <- ["2", "3"] do
      @tag timeout: 30_000
      test "appender/#{arity} returns a constructor error without crashing BEAM" do
        run_fixture("readonly_database", unquote(arity))
      end
    end
  end

  defp run_fixture(scenario, operation) do
    # A NIF crash must fail this test without taking down the test runner.
    elixir = System.find_executable("elixir") || flunk("elixir executable not found")
    ebin = Duckdbex |> :code.which() |> List.to_string() |> Path.dirname()
    fixture = Path.join(__DIR__, "support/appender_gc.exs")

    {output, status} =
      System.cmd(
        elixir,
        ["--erl", "+S 2:2 +SDcpu 1 +SDio 1", "-pa", ebin, fixture, scenario, operation],
        stderr_to_stdout: true
      )

    assert status == 0, """
    Appender scenario #{scenario}/#{operation} failed (child BEAM exit status #{status}).
    #{output}
    """

    assert output =~ "APPENDER_GC_OK"
  end
end
