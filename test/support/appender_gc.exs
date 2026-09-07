# Run only in a separate BEAM: an uncaught C++ exception can abort the VM.
defmodule AppenderExceptionFixture do
  import ExUnit.Assertions

  def run("collected_connection", operation) do
    {:ok, db} = Duckdbex.open()
    {:ok, observer} = Duckdbex.connection(db)
    {:ok, _} = Duckdbex.query(observer, "CREATE TABLE appender_gc(value BIGINT)")

    receiver = self()

    {pid, monitor} =
      spawn_monitor(fn ->
        {:ok, conn} = Duckdbex.connection(db)
        {:ok, appender} = Duckdbex.appender(conn, "appender_gc")
        :ok = Duckdbex.appender_add_row(appender, [42])
        :erlang.garbage_collect()
        send(receiver, {:appender, self(), appender})
      end)

    appender =
      receive do
        {:appender, ^pid, appender} -> appender
      after
        5_000 -> raise "appender creation timed out"
      end

    receive do
      {:DOWN, ^monitor, :process, ^pid, :normal} -> :ok
      {:DOWN, ^monitor, :process, ^pid, reason} -> raise "creator failed: #{inspect(reason)}"
    after
      5_000 -> raise "creator did not terminate"
    end

    :erlang.garbage_collect()
    assert {:error, reason} = finish(operation, appender)
    assert reason =~ "closed connection"
    assert :ok == Duckdbex.release(appender)
    {:ok, result} = Duckdbex.query(observer, "SELECT count(*) FROM appender_gc")
    assert [[0]] == Duckdbex.fetch_all(result)
    :ok = Duckdbex.release(observer)
    :ok = Duckdbex.release(db)
  end

  def run("constraint_error", operation) do
    {:ok, db} = Duckdbex.open()
    {:ok, conn} = Duckdbex.connection(db)
    {:ok, _} = Duckdbex.query(conn, "CREATE TABLE appender_gc(value BIGINT PRIMARY KEY)")
    {:ok, appender} = Duckdbex.appender(conn, "appender_gc")
    :ok = Duckdbex.appender_add_rows(appender, [[42], [42]])
    assert {:error, reason} = finish(operation, appender)
    assert reason =~ "Constraint"
    assert :ok == Duckdbex.release(appender)
    {:ok, result} = Duckdbex.query(conn, "SELECT count(*) FROM appender_gc")
    assert [[0]] == Duckdbex.fetch_all(result)
    :ok = Duckdbex.release(conn)
  end

  def run("conversion_error", operation) do
    {:ok, db} = Duckdbex.open()
    {:ok, conn} = Duckdbex.connection(db)
    {:ok, _} = Duckdbex.query(conn, "CREATE TYPE allowed AS ENUM ('ok')")
    {:ok, _} = Duckdbex.query(conn, "CREATE TABLE appender_gc(value allowed)")
    {:ok, appender} = Duckdbex.appender(conn, "appender_gc")
    assert {:error, reason} = append_invalid_value(operation, appender)
    assert reason =~ "Failed to cast value"
    assert :ok == Duckdbex.release(appender)
    # The connection is still usable after the appender exception.
    {:ok, result} = Duckdbex.query(conn, "SELECT 42")
    assert [[42]] == Duckdbex.fetch_all(result)
    :ok = Duckdbex.release(conn)
  end

  def run("readonly_database", arity) do
    path = Path.join(System.tmp_dir!(), "duckdbex-appender-#{System.pid()}.duckdb")

    try do
      create_database(path)
      {:ok, config} = Duckdbex.create_config()
      :ok = Duckdbex.set_config_option(config, "access_mode", "READ_ONLY")
      {:ok, db} = Duckdbex.open(path, config)
      {:ok, conn} = Duckdbex.connection(db)
      assert {:error, reason} = create_appender(arity, conn)
      assert reason =~ "readonly"
      {:ok, result} = Duckdbex.query(conn, "SELECT count(*) FROM appender_gc")
      assert [[0]] == Duckdbex.fetch_all(result)
      :ok = Duckdbex.release(result)
      :ok = Duckdbex.release(conn)
      :ok = Duckdbex.release(db)
    after
      File.rm(path)
      File.rm(path <> ".wal")
    end
  end

  defp create_database(path) do
    {:ok, db} = Duckdbex.open(path)
    {:ok, conn} = Duckdbex.connection(db)
    {:ok, result} = Duckdbex.query(conn, "CREATE TABLE appender_gc(value BIGINT)")
    :ok = Duckdbex.release(result)
    :ok = Duckdbex.release(conn)
    :ok = Duckdbex.release(db)
  end

  defp finish("flush", appender), do: Duckdbex.appender_flush(appender)
  defp finish("close", appender), do: Duckdbex.appender_close(appender)

  defp append_invalid_value("add_row", appender),
    do: Duckdbex.appender_add_row(appender, ["invalid"])

  defp append_invalid_value("add_rows", appender),
    do: Duckdbex.appender_add_rows(appender, [["invalid"]])

  defp create_appender("2", conn), do: Duckdbex.appender(conn, "appender_gc")
  defp create_appender("3", conn), do: Duckdbex.appender(conn, "main", "appender_gc")
end

[scenario, operation] = System.argv()
AppenderExceptionFixture.run(scenario, operation)
IO.puts("APPENDER_GC_OK")
