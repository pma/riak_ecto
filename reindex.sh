#!/bin/sh

export RIAK_HOST="http://127.0.0.1:8098"
curl -XPUT "$RIAK_HOST/types/ecto_simple/buckets/weather/props" -H 'Content-Type: application/json' -d '{"props":{"search_index":"_dont_index_"}}'
sleep 5
curl -XDELETE "$RIAK_HOST/search/index/weather"             
sleep 5
curl -XPUT "$RIAK_HOST/search/schema/weather"  -H'content-type:application/xml' --data-binary @weather.xml
sleep 5
curl -XPUT "$RIAK_HOST/search/index/weather" -H'content-type:application/json' -d'{"schema":"weather"}'
sleep 15
curl -XPUT "$RIAK_HOST/types/ecto_simple/buckets/weather/props" -H 'Content-Type: application/json' -d '{"props":{"search_index":"weather"}}' 
