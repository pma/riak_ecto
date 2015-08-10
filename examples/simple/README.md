# Simple

To run this example, you need to ensure Riak is up and running with
default username and password. If you want to run with another
credentials, just change the settings in the `config/config.exs` file.

Then, from the command line:

* `mix do deps.get, compile`
* `iex -S mix`

Setup the bucket type and create a Solr Index

1. Create and activate a map bucket_type
```
  $ riak-admin bucket-type create ecto_simple '{"props":{"datatype":"map"}}'
  $ riak-admin bucket-type activate ecto_simple
```

2. Create Solr Index
```
  $ export RIAK_HOST="http://127.0.0.1:8098"
  $ curl -XPUT "$RIAK_HOST/search/index/weather" -H'content-type:application/json' -d'{"schema":"_yz_default"}'
  $ curl -XPUT $RIAK_HOST/types/pp_bitcask/buckets/weather/props -H 'Content-Type: application/json' -d '{"props":{"search_index":"weather"}}'
```

Inside IEx, run:

* `Simple.sample_query`

You can also run the tests with:

* `mix test`
