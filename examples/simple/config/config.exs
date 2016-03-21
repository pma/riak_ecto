use Mix.Config

config :simple, Simple.Repo,
  adapter: Riak.Ecto,
  hostname: "localhost"
