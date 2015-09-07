defmodule Riak.Ecto.Mixfile do
  use Mix.Project

  def project do
    [app: :riak_ecto,
     version: "0.0.1",
     elixir: "~> 1.0",
     deps: deps]
  end

  def application do
    [applications: [:ecto, :flaky, :riakc]]
  end

  defp deps do
    [{:riakc, github: "basho/riak-erlang-client"},
     {:flaky, github: "pma/flaky"},
     {:ecto,  "~> 1.0.2"}]
  end
end
