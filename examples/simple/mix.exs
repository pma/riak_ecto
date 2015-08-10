defmodule Simple.Mixfile do
  use Mix.Project

  def project do
    [app: :simple,
     version: "0.0.1",
     deps: deps]
  end

  def application do
    [mod: {Simple.App, []},
     applications: [:riak_ecto, :ecto]]
  end

  defp deps do
    [{:riak_ecto, path: "../.."},
     {:ecto, path: "../../deps/ecto", override: true}]
  end
end
