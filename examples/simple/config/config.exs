use Mix.Config

config :simple, Simple.Repo,
  adapter: Riak.Ecto,
  hostname: "127.0.0.1"
