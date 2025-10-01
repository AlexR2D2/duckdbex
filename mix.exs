defmodule Duckdbex.MixProject do
  use Mix.Project

  @version "0.3.15"
  @duckdb_version "1.4.0"

  def project do
    [
      app: :duckdbex,
      version: @version,
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      description: description(),
      elixirc_paths: elixirc_paths(Mix.env()),
      compilers: [:elixir_make] ++ Mix.compilers(),
      make_targets: ["all"],
      make_clean: ["clean"],
      # elixir_make specific config
      make_precompiler: {:nif, CCPrecompiler},
      make_precompiler_url:
        "https://github.com/AlexR2D2/duckdbex/releases/download/v#{@version}/@{artefact_filename}",
      make_precompiler_filename: "duckdb_nif",
      make_precompiler_nif_versions: [
        versions: &nif_versions/1,
        fallback_version: &fallback_nif_versions/1
      ],
      cc_precompiler: cc_precompiler(),
      # Docs
      name: "Duckdbex",
      source_url: "https://github.com/AlexR2D2/duckdbex/",
      homepage_url: "https://github.com/AlexR2D2/duckdbex/",
      docs: docs()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      extra_applications: [:logger, :public_key]
    ]
  end

  defp deps do
    [
      {:elixir_make, "~> 0.8", runtime: false},
      {:cc_precompiler, "~> 0.1", runtime: false},
      {:dialyxir, "~> 1.4", only: :dev, runtime: false},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end

  defp nif_versions(_opts) do
    ["2.16", "2.17"]
  end

  defp fallback_nif_versions(opts) do
    hd(nif_versions(opts))
  end

  defp package do
    [
      files: ~w(
        lib
        c_src
        bin
        .formatter.exs
        Makefile*
        mix.exs
        checksum.exs
        README.md
        LICENSE
      ),
      name: "duckdbex",
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/AlexR2D2/duckdbex",
        "Changelog" => "https://github.com/AlexR2D2/duckdbex/blob/main/CHANGELOG.md"
      }
    ]
  end

  defp cc_precompiler do
    [
      cleanup: "clean",
      compilers: %{
        {:unix, :linux} => %{
          "x86_64-linux-gnu" => "x86_64-linux-gnu-",
          "aarch64-linux-gnu" => "aarch64-linux-gnu-",
          "riscv64-linux-gnu" => "riscv64-linux-gnu-"
        },
        {:unix, :darwin} => %{
          :include_default_ones => true
        }
      }
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: [
        "README.md": [title: "Duckdbex"],
        "CHANGELOG.md": []
      ],
      source_ref: "v#{@version}",
      source_url: "https://github.com/AlexR2D2/duckdbex"
    ]
  end

  defp description do
    "An Elixir DuckDB library"
  end

  def duckdb_version, do: @duckdb_version
end
