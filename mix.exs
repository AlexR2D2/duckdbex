defmodule Duckdbex.MixProject do
  use Mix.Project

  @version "0.1.0"

  def project do
    [
      app: :duckdbex,
      version: @version,
      elixir: "~> 1.12",
      compilers: [:elixir_make] ++ Mix.compilers,
      make_targets: ["all"],
      make_clean: ["clean"],
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      package: package(),
      description: description(),
      elixirc_paths: elixirc_paths(Mix.env()),
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
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:elixir_make, "~> 0.6", runtime: false},
      {:ex_doc, "~> 0.24", only: :dev, runtime: false},
    ]
  end

  defp package do
    [
      files: ~w(
        lib
        c_src/*
        c_src/duckdb/*
        .formatter.exs
        mix.exs
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

end
