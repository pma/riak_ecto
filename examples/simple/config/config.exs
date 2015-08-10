use Mix.Config

config :simple, Simple.Repo,
  adapter: Riak.Ecto,
  bucket_type: "ecto_simple",
  hostname: "localhost"
