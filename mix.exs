defmodule Riak.Ecto.Mixfile do
  use Mix.Project

  def project do
    [app: :riak_ecto,
     version: "0.2.0",
     elixir: "~> 1.2",
     deps: deps()]
  end

  def application do
    [applications: [:hamcrest, :riakc, :ecto]]
  end

  defp deps do
    [{:riakc, "~> 2.4"},
     {:ecto, "~> 2.1"}]
  end
end
