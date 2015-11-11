defmodule Riak.Ecto.Connection do
  @moduledoc false

  alias Riak.Ecto.NormalizedQuery.SearchQuery
  alias Riak.Ecto.NormalizedQuery.FetchQuery
  alias Riak.Ecto.NormalizedQuery.CountQuery
  alias Riak.Ecto.NormalizedQuery.WriteQuery

  ## Worker

  ## Callbacks for adapter

  def all(pool, query, opts \\ [])

  def all(pool, %FetchQuery{} = query, _opts) do
    coll        = query.coll
    _projection = query.projection

    case Riak.fetch_type(pool, coll, query.id) do
      {:ok, map} ->
        [map
         |>:riakc_map.value
         |> crdt_to_map
         |> Map.merge(%{id: query.id, context: %{map: map, total_count: 1}})]
      {:error, :not_found} ->
        []
    end
  end

  def all(pool, %SearchQuery{} = query, opts) do
    coll        = query.coll
    _projection = query.projection
    opts        = query.opts ++ opts
    filter      = query.filter
    order       = query.order
    query       = query.query

    opts = [{:filter, filter} | opts] ++ [{:sort, order}]

    case Riak.search(pool, coll, query, opts) do
      {:ok, {results, total_count}} ->
        Enum.map(results, fn result ->
          result
          |> solr_to_map
          |> Map.merge(%{context: %{map: nil, total_count: total_count}})
        end)
    end
  end

  def all(pool, %CountQuery{} = query, opts) do
    coll        = query.coll
    _projection = query.projection
    opts        = query.opts ++ opts
    filter      = query.filter
    query       = "*:*" #query.query

    opts = [filter: filter, rows: 0, start: 0] ++ opts

    case Riak.search(pool, coll, query, opts) do
      {:ok, {_, total_count}} ->
        [%{"value" => total_count}]
    end
  end

  defp crdt_to_map(values) do
    Enum.reduce(values, %{}, fn
      {{k, :flag}, v}, m     -> Dict.put(m, k, v)
      {{k, :register}, v}, m -> Dict.put(m, k, v)
      {{k, :counter}, v}, m  -> Dict.put(m, k, {:counter, v})
      {{k, :set}, v}, m      -> Dict.put(m, k, {:set, v})
      {{k, :map}, v}, m      -> Dict.put(m, k, crdt_to_map((v)))
    end)
  end

  @ignore_fields ~w(_yz_id _yz_rb _yz_rt score)
  defp solr_to_map({_, fields}) do
    Enum.reduce(fields, %{}, fn
      {field, _}, map when field in @ignore_fields -> map
      {"_yz_rk", value}, map                       -> Dict.put(map, :id, value)
      {key, value}, map                            -> map_solr_field(key, value, map)
    end)
  end

  defp map_solr_field(key, value, map) do
    case String.split(key, ".", parts: 2) do
      [k]          -> map_solr_field_value(k, value, "", map)
      [k | [rest]] -> map_solr_field_value(k, value, rest, map)
    end
  end

  defp map_solr_field_value(key, value, key_rest, map) do
    case Regex.scan(~r/(.*)_(map|register|counter|flag|set)/r, key, capture: :all_but_first) do
      [[field, "register"]] -> Dict.put(map, field, value)
      [[field, "flag"]]     -> Dict.put(map, field, value == "true")
      [[field, "counter"]]  -> Dict.put(map, field, {:counter, String.to_integer(value)})
      [[field, "map"]]      -> Dict.update(Dict.put_new(map, field, %{}), field, %{}, &map_solr_field(key_rest, value, &1))
      _                     -> map
    end
  end

  @riak_types [:register, :flag, :map, :set]
  defp erase_key_unless_type(map, key, exclude \\ [])
  defp erase_key_unless_type(map, key, exclude) do
    Enum.reduce(@riak_types -- exclude, map, fn type, acc ->
      if :riakc_map.is_key({key, type}, acc) do
        :riakc_map.erase({key, type}, acc)
      else
        acc
      end
    end)
  end

  defp apply_change(map, {key, empty}) when empty in [nil, []] do
    erase_key_unless_type(map, key)
  end

  defp apply_change(map, {key, false}) do
    map = erase_key_unless_type(map, key, [:flag])
    :riakc_map.update({key, :flag}, &:riakc_flag.disable(&1), map)
  end

  defp apply_change(map, {k, true}) do
    map = erase_key_unless_type(map, k, [:flag])
    :riakc_map.update({k, :flag}, &:riakc_flag.enable(&1), map)
  end

  defp apply_change(map, {k, value}) when is_binary(value) do
    map = erase_key_unless_type(map, k, [:register])
    :riakc_map.update({k, :register}, &:riakc_register.set(value, &1), map)
  end

  defp apply_change(map, {k, {:counter, _value, increment}}) do
    map = erase_key_unless_type(map, k, [:counter])
    :riakc_map.update({k, :counter}, &:riakc_counter.increment(increment, &1), map)
  end

  defp apply_change(map, {k, {:set, value}}) when is_list(value) do
    map = erase_key_unless_type(map, k, [:set])
    :riakc_map.update({k, :set}, fn set ->
      dirty_value = :riakc_set.value(set)
      to_add = value -- dirty_value
      to_rem = dirty_value -- value

      set = Enum.reduce(to_add, set, &:riakc_set.add_element(&1, &2))
      set = Enum.reduce(to_rem, set, &:riakc_set.del_element(&1, &2))
      set
    end, map)
  end

  defp apply_change(crdt_map, {key, value_map}) when is_map(value_map) do
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

  defp apply_change(crdt_map, {key, [%{id: _id} | _] = value_list}) when is_list(value_list) do
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

  defp apply_change(crdt_map, {key, value_list}) when is_list(value_list) do
    crdt_map = erase_key_unless_type(crdt_map, key, [:map])
    Enum.reduce(value_list, crdt_map, fn item, acc ->
      :riakc_map.update({key, :map}, &apply_change(&1, {to_string(:erlang.phash2(item)), item}), acc)
    end)
  end

  defp apply_changes(crdt_map, updates) do
    Enum.reduce(updates, crdt_map || :riakc_map.new, fn {key, new_value}, acc ->
      apply_change(acc, {to_string(key), new_value})
    end)
  end

  def update(pool,  %WriteQuery{} = query, opts) do
    coll    = query.coll
    command = query.command
    context = query.context || %{}
    _       = query.opts ++ opts
    query   = query.query

    map = apply_changes(Map.get(context, :map), Dict.fetch!(command, :set))
    op  =  :riakc_map.to_op(map)

    case Riak.update_type(pool, coll, query[:id], op) do
      :ok -> {:ok, []}
      _   -> {:error, :stale}
    end
  end

  def insert(pool, %WriteQuery{} = query, opts) do
    coll    = query.coll
    command = query.command
    context = query.context || %{}
    _       = query.opts ++ opts

    id = command[:id] || :undefined

    map = apply_changes(Map.get(context, :map), command)

    case Riak.update_type(pool, coll, id, :riakc_map.to_op(map)) do
      :ok       -> {:ok, 1}
      {:ok, id} -> {:ok, %{inserted_id: id}}
    end
  end

  def delete(pool, %WriteQuery{} = query, opts) do
    coll     = query.coll
    _        = query.context
    _        = query.opts ++ opts
    query    = query.query

    id = Dict.fetch!(query, :id)

    case Riak.delete(pool, coll, id) do
      :ok -> {:ok, []}
      _   -> {:error, :stale}
    end
  end

end
