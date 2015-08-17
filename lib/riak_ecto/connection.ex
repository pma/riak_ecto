defmodule Riak.Ecto.Connection do
  @moduledoc false

  alias Riak.Ecto.NormalizedQuery.ReadQuery
  alias Riak.Ecto.NormalizedQuery.WriteQuery

  ## Worker
  require Logger

  ## Callbacks for adapter

  def all(pool, %ReadQuery{} = query, opts \\ []) do

    coll        = query.coll
    _projection = query.projection
    opts        = query.opts ++ opts
    filter      = query.filter
    query       = query.query

    results = cond do
      Dict.size(query) == 1 and Dict.has_key?(query, :id) ->
        case Riak.fetch_type(pool, coll, query.id) do
          {:ok, map} ->
            [
              map
              |> :riakc_map.value
              |> value_to_map
              |> Map.merge(%{id: query.id, context: map})
            ]
          {:error, :not_found} ->
            []
        end
      true ->
        opts = [{:filter, filter} | opts] #++ [{:fl, fl}]

        case Riak.search(pool, coll, "*:*", opts) do
          {:ok, results} ->
            Enum.map(results, &from_solr/1)
        end
    end

    results
  end

  # PRIV

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
      {"_yz_rk", value}, map                       -> Dict.put(map, :id, value)
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

  def erase_key(map, key, types \\ [:register, :flag, :map, :set], context)

  def erase_key(map, _key, _types, nil), do: map
  def erase_key(map, key, types, _context),
    do: Enum.reduce(types, map, &:riakc_map.erase({to_string(key), &1}, &2))

  def apply_change(map, {k, {type, nil}}, context) do
    erase_key(map, k, [type], context)
  end

  def apply_change(map, {k, nil}, context) do
    erase_key(map, k, context)
  end

  def apply_change(map, {k, []}, context) do
    erase_key(map, k, context)
  end

  def apply_change(map, {k, false}, context) do
    map = erase_key(map, k, [:register, :map, :set], context)
    :riakc_map.update({to_string(k), :flag}, &:riakc_flag.disable(&1), map)
  end

  def apply_change(map, {k, true}, context) do
    map = erase_key(map, k, [:register, :map, :set], context)
    :riakc_map.update({to_string(k), :flag}, &:riakc_flag.enable(&1), map)
  end

  def apply_change(map, {k, value}, context) when is_binary(value) do
    map = erase_key(map, k, [:flag, :map, :set], context)
    :riakc_map.update({to_string(k), :register}, &:riakc_register.set(value, &1), map)
  end

  def apply_change(map, {key, value}, context) when is_map(value) do
    map = erase_key(map, key, [:flag, :register, :set], context)
    Enum.reduce(value, map, fn {k, v}, acc ->
      :riakc_map.update({to_string(key), :map}, &apply_change(&1, {k, v}, context), acc)
    end)
  end

  def apply_change(map, {key, value}, context) when is_list(value) do
    map = erase_key(map, key, [:flag, :register, :set], context)
    Enum.reduce(value, map, fn
      item, acc when is_map(item) ->
        if Map.has_key?(item, :id) do
          :riakc_map.update({to_string(key), :map}, &apply_change(&1, {item[:id], item}, context), acc)
        end
    end)
  end

  def apply_changes(map, updates, context) do
    map = map || :riakc_map.new
    Enum.reduce(updates, map, &apply_change(&2, &1, context))
  end

  def update(pool,  %WriteQuery{} = query, opts) do
    coll    = query.coll
    command = query.command
    context = query.context
    _model  = query.model
    _opts   = query.opts ++ opts
    query   = query.query

    map = apply_changes(context, Dict.fetch!(command, :set), context)

    case Riak.update_type(pool, coll, query[:id], :riakc_map.to_op(map)) do
      :ok -> {:ok, []}
      _   -> {:error, :stale}
    end
  end

  def insert(pool, %WriteQuery{} = query, opts) do
    coll    = query.coll
    command = query.command
    context = query.context
    _opts   = query.opts ++ opts

    id = command[:id] || :undefined

    map = apply_changes(context, command, context)

    case Riak.update_type(pool, coll, id, :riakc_map.to_op(map)) do
      :ok       -> {:ok, 1}
      {:ok, id} -> {:ok, %{inserted_id: id}}
    end
  end

  def delete(pool, %WriteQuery{} = query, opts) do
    coll     = query.coll
    _context = query.context
    _opts    = query.opts ++ opts
    query    = query.query

    id = Dict.fetch!(query, :id)

    case Riak.delete(pool, coll, id) do
      :ok -> {:ok, []}
      _   -> {:error, :stale}
    end
  end

end
