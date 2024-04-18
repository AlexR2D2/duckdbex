defmodule Duckdbex.MixProject do
  use Mix.Project

  @version "0.2.10"
  @duckdb_version "0.10.2"

  def project do
    [
      app: :duckdbex,
      version: @version,
      elixir: "~> 1.12",
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
        versions: ["2.15", "2.16"],
        availability: &target_available_for_nif_version?/2
      ],
      cc_precompiler: [cleanup: "clean"],
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
      {:elixir_make, "~> 0.7", runtime: false},
      {:cc_precompiler, "~> 0.1", runtime: false},
      {:ex_doc, "~> 0.24", only: :dev, runtime: false}
    ]
  end

  def target_available_for_nif_version?(target, nif_version) do
    if String.contains?(target, "windows") do
      nif_version == "2.16"
    else
      true
    end
  end

  defp package do
    [
      files: ~w(
        lib
        c_src/*
        c_src/duckdb/*
        bin
        .formatter.exs
        mix.exs
        checksum.exs
        README.md
        LICENSE
        Makefile*
      ),
      name: "duckdbex",
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/AlexR2D2/duckdbex/"
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
      source_url: "https://github.com/AlexR2D2/duckdbex/"
    ]
  end

  defp description do
    "An Elixir DuckDB library"
  end

  def duckdb_version, do: @duckdb_version
end
