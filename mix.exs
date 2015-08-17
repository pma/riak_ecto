defmodule Riak.Ecto.Mixfile do
  use Mix.Project

  def project do
    [app: :riak_ecto,
     version: "0.0.1",
     elixir: "~> 1.0",
     deps: deps]
  end

  def application do
    [applications: [:logger, :flaky, :riakc]]
  end

  defp deps do
    [{:riakc, github: "pma/riak-erlang-client", branch: "pma-ensure_reconnect_if_first_connect_fails"},
     {:flaky, github: "pma/flaky"},
     {:ecto, github: "elixir-lang/ecto"}]
  end
end
