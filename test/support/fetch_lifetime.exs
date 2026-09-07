# Run in a separate BEAM so a native crash does not kill the test runner.
defmodule FetchLifetimeFixture do
  import ExUnit.Assertions

  @row_count 10_000
  @sql "SELECT i FROM range(1, #{@row_count + 1}) AS t(i) ORDER BY i"
  @parameterized_sql "SELECT i FROM range(1, $1::BIGINT) AS t(i) ORDER BY i"

  def run(query, release) do
    {:ok, db} = Duckdbex.open()
    {:ok, conn} = Duckdbex.connection(db)
    {result, statement} = create_result(query, conn)

    :ok = Duckdbex.release(conn)
    release_other_owners(release, db, statement)
    :erlang.garbage_collect()

    # Check every value, not just whether the VM survived or a list was returned.
    expected = Enum.map(1..@row_count, &[&1])
    assert expected == Duckdbex.fetch_all(result)
    assert [] == Duckdbex.fetch_all(result)
    :ok = Duckdbex.release(result)

    # In the connection-only case, keep these resources reachable until after fetch.
    release_statement(statement)
    :ok = Duckdbex.release(db)
    IO.puts("FETCH_LIFETIME_OK: #{query}/#{release}, #{@row_count} rows")
  end

  defp create_result("query", conn) do
    {:ok, result} = Duckdbex.query(conn, @sql)
    {result, nil}
  end

  defp create_result("query_params", conn) do
    {:ok, result} = Duckdbex.query(conn, @parameterized_sql, [@row_count + 1])
    {result, nil}
  end

  defp create_result("statement", conn) do
    {:ok, statement} = Duckdbex.prepare_statement(conn, @sql)
    {:ok, result} = Duckdbex.execute_statement(statement)
    {result, statement}
  end

  defp create_result("statement_params", conn) do
    {:ok, statement} = Duckdbex.prepare_statement(conn, @parameterized_sql)
    {:ok, result} = Duckdbex.execute_statement(statement, [@row_count + 1])
    {result, statement}
  end

  defp release_other_owners("connection", _db, _statement), do: :ok

  defp release_other_owners("all_owners", db, statement) do
    release_statement(statement)
    :ok = Duckdbex.release(db)
  end

  defp release_statement(nil), do: :ok
  defp release_statement(statement), do: Duckdbex.release(statement)
end

[query, release] = System.argv()
FetchLifetimeFixture.run(query, release)
