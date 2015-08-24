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
              |> crdt_to_map
              |> Map.merge(%{id: query.id, context: map})
            ]
          {:error, :not_found} ->
            []
        end
      true ->
        opts = [{:filter, filter} | opts] #++ [{:fl, fl}]

        case Riak.search(pool, coll, "*:*", opts) do
          {:ok, results} ->
            Enum.map(results, &solr_to_map/1)
        end
    end

    results
  end

  # PRIV

  def crdt_to_map(values) do
    Enum.reduce(values, %{}, fn
      {{k, :flag}, v}, m     -> Dict.put(m, k, v)
      {{k, :register}, v}, m -> Dict.put(m, k, v)
      {{k, :counter}, v}, m  -> Dict.put(m, k, {:counter, v})
      {{k, :map}, v}, m      -> Dict.put(m, k, crdt_to_map((v)))
    end)
  end

  @ignore_fields ~w(_yz_id _yz_rb _yz_rt score)
  def solr_to_map({_, fields}) do
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

  @riak_types [:register, :flag, :map, :set]
  def erase_key_unless_type(map, key, exclude \\ [])
  def erase_key_unless_type(map, key, exclude) do
    Enum.reduce(@riak_types -- exclude, map, fn type, acc ->
      if :riakc_map.is_key({key, type}, acc) do
        :riakc_map.erase({key, type}, acc)
      else
        acc
      end
    end)
  end

  def apply_change(map, {key, empty}) when empty in [nil, []] do
    erase_key_unless_type(map, key)
  end

  def apply_change(map, {key, false}) do
    map = erase_key_unless_type(map, key, [:flag])
    :riakc_map.update({key, :flag}, &:riakc_flag.disable(&1), map)
  end

  def apply_change(map, {k, true}) do
    map = erase_key_unless_type(map, k, [:flag])
    :riakc_map.update({k, :flag}, &:riakc_flag.enable(&1), map)
  end

  def apply_change(map, {k, value}) when is_binary(value) do
    map = erase_key_unless_type(map, k, [:register])
    :riakc_map.update({k, :register}, &:riakc_register.set(value, &1), map)
  end

  def apply_change(map, {k, {:counter, value, increment}}) do
    Logger.debug "COUNTER #{inspect({:counter, value, increment})}"
    map = erase_key_unless_type(map, k, [:counter])
    :riakc_map.update({k, :counter}, &:riakc_counter.increment(increment, &1), map)
  end

  def apply_change(crdt_map, {key, value_map}) when is_map(value_map) do
    crdt_map = erase_key_unless_type(crdt_map, key, [:map])

    :riakc_map.update({key, :map}, fn inner_crdt_map ->
      value_map_keys = Map.keys(value_map) |> Enum.map(&to_string/1)
      inner_crdt_map =
        Enum.reduce(:riakc_map.fetch_keys(inner_crdt_map), inner_crdt_map, fn {k, dt}, acc1 ->
          if(k in value_map_keys, do: acc1, else: :riakc_map.erase({k, dt}, acc1))
        end)

      Enum.reduce(value_map, inner_crdt_map, fn {k, v}, acc ->
        apply_change(acc, {to_string(k), v})
      end)
    end, crdt_map)
  end

  def apply_change(crdt_map, {key, [%{id: id} | _] = value_list}) when is_list(value_list) do
    crdt_map = erase_key_unless_type(crdt_map, key, [:map])

    crdt_map =
      :riakc_map.update({key, :map}, fn inner_crdt_map ->
        ids = Enum.map(value_list, &Map.fetch!(&1, :id)) |> Enum.map(&to_string/1)
        Enum.reduce(:riakc_map.fetch_keys(inner_crdt_map), inner_crdt_map, fn {k, dt}, acc ->
          if(k in ids, do: acc, else: :riakc_map.erase({k, dt}, acc))
        end)
      end, crdt_map)

    Enum.reduce(value_list, crdt_map, fn %{id: id} = item, acc ->
      item = Map.delete(item, :id)
      :riakc_map.update({key, :map}, &apply_change(&1, {to_string(id), item}), acc)
    end)
  end

  def apply_change(crdt_map, {key, value_list}) when is_list(value_list) do
    crdt_map = erase_key_unless_type(crdt_map, key, [:map])
    Enum.reduce(value_list, crdt_map, fn item, acc ->
      :riakc_map.update({key, :map}, &apply_change(&1, {to_string(:erlang.phash2(item)), item}), acc)
    end)
  end

  def apply_changes(crdt_map, updates) do
    Enum.reduce(updates, crdt_map || :riakc_map.new, fn {key, new_value}, acc ->
      apply_change(acc, {to_string(key), new_value})
    end)
  end

  def update(pool,  %WriteQuery{} = query, opts) do
    coll    = query.coll
    command = query.command
    context = query.context
    model   = query.model
    _opts   = query.opts ++ opts
    query   = query.query

    map = apply_changes(context, Dict.fetch!(command, :set))
    op  =  :riakc_map.to_op(map)

    Logger.debug "OP = #{inspect(op)}"

    case Riak.update_type(pool, coll, query[:id], op) do
      :ok -> {:ok, []}
      _   -> {:error, :stale}
    end
  end

  def insert(pool, %WriteQuery{} = query, opts) do
    coll    = query.coll
    command = query.command
    context = query.context
    model   = query.model
    _opts   = query.opts ++ opts

    id = command[:id] || :undefined

    map = apply_changes(context, command)

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
