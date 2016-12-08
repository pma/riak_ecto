defmodule Riak.Ecto.Connection do
  require Logger
  @moduledoc false

  alias Riak.Ecto.NormalizedQuery.SearchQuery
  alias Riak.Ecto.NormalizedQuery.FetchQuery

  ## Worker

  ## Callbacks for adapter

  defp bucket(nil, source), do: source
  defp bucket(prefix, _), do: to_string(prefix)

  def all(pool, query, opts \\ [])

  def all(pool, %FetchQuery{} = query, _opts) do
    prefix      = query.prefix
    source      = query.source
    fields      = query.fields

    case Riak.fetch_type(pool, source, bucket(prefix, source), query.id) do
      {:ok, map} ->
        [{{1, map},
          map
          |>:riakc_map.value
          |> crdt_to_map
          |> project_fields(fields)}]
      {:error, :not_found} ->
        []
    end
  end

  def all(pool, %SearchQuery{} = query, opts) do
    prefix      = query.prefix
    source      = query.source
    struct      = query.struct
    pk          = query.pk
    fields      = query.fields
    opts        = query.opts ++ opts
    filter      = query.filter_query
    order       = query.order
    query       = query.normal_query

    fl = fields
    |> Stream.map(&field_name_ecto_to_solr(&1, pk, struct))
    |> Enum.join(" ")

    opts = [filter: filter, sort: order, fl: fl] ++ opts

    case Riak.search(pool, source, bucket(prefix, source), query, opts) do
      {:ok, {results, total_count}} ->
        Enum.map(results, fn result ->
          {{total_count, nil},
           result
           |> solr_to_map
           |> project_fields(fields)}
        end)
    end
  end

  defp project_fields(map, fields) do
    Enum.map(fields, &Map.get(map, Atom.to_string(&1)))
  end

  def field_name_ecto_to_solr(pk, pk, _struct),
    do: "_yz_rk"
  def field_name_ecto_to_solr(field, _pk, struct) do
    riak_type =
      case Ecto.Type.type(struct.__schema__(:type, field)) do
        register when register in [:string, :integer, :float, :decimal,
                                   :date, :datetime,
                                   :naive_datetime, :utc_datetime,
                                   :binary_id, :id] ->
          "register"
        :boolean    -> "flag"
        :map        -> "map.*"
        {:embed, _} -> "map.*"
        {:array, _} -> "map.*"
      end

    Atom.to_string(field) <> "_" <> riak_type
  end

  defp crdt_to_map(values) do
    Enum.reduce(values, %{}, fn
      {{k, :register}, v}, m -> Map.put(m, k, v)
      {{k, :map}, v}, m      -> Map.put(m, k, crdt_to_map((v)))
      {{k, :flag}, v}, m     -> Map.put(m, k, v)
      {{k, :counter}, v}, m  -> Map.put(m, k, {:counter, v})
      {{k, :set}, v}, m      -> Map.put(m, k, {:set, v})
    end)
  end

  @ignore_fields ~w(_yz_id _yz_rb _yz_rt score)
  defp solr_to_map({_, fields}) do
    Enum.reduce(fields, %{}, fn
      {field, _}, map when field in @ignore_fields -> map
      {"_yz_rk", value}, map                       -> Map.put(map, "id", value)
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
      [[field, "register"]] -> Map.put(map, field, value)
      [[field, "flag"]]     -> Map.put(map, field, value == "true")
      [[field, "counter"]]  -> Map.put(map, field, {:counter, String.to_integer(value)})
      [[field, "map"]]      -> Map.update(Map.put_new(map, field, %{}), field, %{}, &map_solr_field(key_rest, value, &1))
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
    :riakc_map.update({key, :flag}, fn flag ->
      try do
        :riakc_flag.disable(flag)
      catch
        :context_required -> flag
      end
    end, map)
  end

  defp apply_change(map, {k, true}) do
    map = erase_key_unless_type(map, k, [:flag])
    :riakc_map.update({k, :flag}, &:riakc_flag.enable(&1), map)
  end

  defp apply_change(map, {k, value}) when is_binary(value) do
    map = erase_key_unless_type(map, k, [:register])
    :riakc_map.update({k, :register}, &:riakc_register.set(to_string(value), &1), map)
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

  defp apply_changes(crdt_map, updates) do
    Enum.reduce(updates, crdt_map || :riakc_map.new, fn {key, new_value}, acc ->
      apply_change(acc, {to_string(key), new_value})
    end)
  end

  def insert(pool, prefix, source, params, _options) do
    map = apply_changes(nil, params)
    bucket = bucket(prefix, source)

    case Riak.update_type(pool, source, bucket, Keyword.get(params, :id), :riakc_map.to_op(map)) do
      :ok       -> {:ok, []}
      {:ok, id} -> {:ok, [id: id]}
    end
  end

  def update(pool, prefix, source, {_, map} = _context, id, params, _options) do
    map = apply_changes(map, params)
    bucket = bucket(prefix, source)

    case Riak.update_type(pool, source, bucket, id, :riakc_map.to_op(map)) do
      :ok       -> {:ok, []}
      _         -> {:error, :stale}
    end
  end

  def delete(pool, prefix, source, id, _options) do
    case Riak.delete(pool, source, bucket(prefix, source), id) do
      :ok -> {:ok, []}
      _   -> {:error, :stale}
    end
  end
end
