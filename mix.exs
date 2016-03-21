defmodule Riak.Ecto.Mixfile do
  use Mix.Project

  def project do
    [app: :riak_ecto,
     version: "0.1.0",
     elixir: "~> 1.2",
     deps: deps]
  end

  def application do
    [applications: [:riakc, :ecto]]
  end

  defp deps do
    [{:riakc, github: "basho/riak-erlang-client"},
     {:ecto,  "~> 1.1"}]
  end
end
