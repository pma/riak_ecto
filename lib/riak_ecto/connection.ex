defmodule Riak.Ecto.Connection do
  @moduledoc false

  alias Riak.Ecto.NormalizedQuery.ReadQuery
  alias Riak.Ecto.NormalizedQuery.WriteQuery

  ## Worker
  require Logger

  ## Callbacks for adapter

  def all(pool, %ReadQuery{} = query, opts \\ []) do

    coll        = query.coll
    _projection  = query.projection
    opts        = query.opts ++ opts
    query       = query.query

    results = cond do
      Dict.size(query) == 1 and Dict.has_key?(query, :id) ->
        case Riak.fetch_type(pool, coll, query.id) do
          {:ok, map} ->
            [
              map
              |>:riakc_map.value
              |> value_to_map
              |> Map.put(:key, query.id)
            ]
          {:error, :not_found} ->
            []
        end
      true ->
        compiled_query = to_solr_query(query) |> IO.iodata_to_binary

        opts = [{:filter, compiled_query} | opts] #++ [{:fl, fl}]

        case Riak.search(pool, coll, "*:*", opts) do
          {:ok, results} ->
            Enum.map(results, &from_solr/1)
        end
    end

    results
  end

  # PRIV

  def to_solr_query(%{:"$or" => [left, right]}) do
    ['(', to_solr_query(left), ')', ' OR ', '(', to_solr_query(right), ')' ]
  end

  def to_solr_query(%{raw: expr}) do
    expr
  end

  def to_solr_query([{field, value}]) when is_binary(value) do
    [to_string(field), '_register', ':', value]
  end

  def to_solr_query([{field, true}]) do
    [to_string(field), '_flag', ':', 'true']
  end

  def to_solr_query([{field, false}]) do
    ['-', to_string(field), '_flag', ':', 'true']
  end

  def to_solr_query(%{} = map) do
    to_solr_query(Enum.into(map, []))
  end

  def to_solr_query([]) do
    '*:*'
  end

  def value_to_map(values) do
    values
    |> Enum.reduce(%{}, fn
      {{k, :flag}, v}, m     -> Dict.put(m, k, v)
      {{k, :register}, v}, m -> Dict.put(m, k, v)
      {{k, :map}, v}, m      -> Dict.put(m, k, value_to_map((v)))
    end)
  end

  @ignore_fields ~w(_yz_id _yz_rb _yz_rt score)
  def from_solr({_, fields}) do
    Enum.reduce(fields, %{}, fn
      {field, _}, map when field in @ignore_fields -> map
      {"_yz_rk", value}, map                       -> Dict.put(map, :key, value)
      {key, value}, map                            -> map_solr_field(key, value, map)
    end)
  end

  def map_solr_field(key, value, map) do
    case String.split(key, ".", parts: 2) do
      [k]          -> map_solr_field_value(k, value, "", map)
      [k | [rest]] -> map_solr_field_value(k, value, rest, map)
    end
  end

  def map_solr_field_value(key, value, key_rest, map) do
    case Regex.scan(~r/(.*)_(map|register|counter|flag|set)/r, key, capture: :all_but_first) do
      [[field, "register"]] -> Dict.put(map, field, value)
      [[field, "flag"]]     -> Dict.put(map, field, value == "true")
      [[field, "map"]]      -> Dict.update(Dict.put_new(map, field, %{}), field, %{}, &map_solr_field(key_rest, value, &1))
    end
  end
  # PRIV

#  def dt_to_op({k, nil})  do
#    {:remove, {to_string(k), :register}}
#  end

  def dt_to_op({k, v}) when is_binary(v) do
    {:update, {to_string(k), :register}, {:assign, v}}
  end

  def dt_to_op({k, true}) do
    {:update, {to_string(k), :flag}, :enable}
  end

  def dt_to_op({k, boolean}) when boolean in [false, nil] do
    {:update, {to_string(k), :flag}, :disable}
  end

  def dt_to_op({k, v}) when is_list(v) do
    updates = Enum.reduce(v, [], fn item, acc ->
      [dt_to_op(item) | acc]
    end)
    {:update, {to_string(k), :map}, updates}
  end

  def command_to_op(command, context \\ :undefined) do
    updates = command
    |> Enum.reduce([], fn x, acc ->
      [dt_to_op(x) | acc]
    end)

    {:map, {:update, updates}, context}
  end

  def update(pool,  %WriteQuery{} = query, _opts) do
    coll     = query.coll
    command  = query.command
    # opts     = query.opts ++ opts
    query    = query.query

    op = command_to_op(Dict.fetch!(command, :set))

    case Riak.update_type(pool, coll, query[:id], op) do
      :ok -> {:ok, []}
      _foo ->
        {:error, :stale}
    end
  end

  def insert(pool, %WriteQuery{} = query, opts) do
    coll        = query.coll
    command     = query.command
    _opts        = query.opts ++ opts

    id = command[:id] || :undefined


    op = command_to_op(command)

    case Riak.update_type(pool, coll, id, op) do
      :ok       -> {:ok, 1}
      {:ok, id} -> {:ok, %{inserted_id: id}}
    end
  end
end
