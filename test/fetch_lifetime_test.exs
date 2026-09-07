defmodule Duckdbex.FetchLifetimeTest do
  use ExUnit.Case

  describe "fetch_all after releasing its connection" do
    for query <- ["query", "query_params", "statement", "statement_params"],
        release <- ["connection", "all_owners"] do
      @tag timeout: 30_000
      test "#{query} returns all rows after releasing #{release}" do
        # Isolate native crashes from the main test runner.
        elixir = System.find_executable("elixir") || flunk("elixir executable not found")
        ebin = Duckdbex |> :code.which() |> List.to_string() |> Path.dirname()
        fixture = Path.join(__DIR__, "support/fetch_lifetime.exs")

        {output, status} =
          System.cmd(
            elixir,
            [
              "--erl",
              "+S 2:2 +SDcpu 1 +SDio 1",
              "-pa",
              ebin,
              fixture,
              unquote(query),
              unquote(release)
            ],
            stderr_to_stdout: true
          )

        assert status == 0, """
        Fetch scenario #{unquote(query)}/#{unquote(release)} failed (child BEAM exit status #{status}).
        #{output}
        """

        assert output =~ "FETCH_LIFETIME_OK"
      end
    end
  end
end
