defmodule Riak.Ecto.Encoder do
  @moduledoc false

  import Riak.Ecto.Utils
  alias Ecto.Query.Tagged

  def encode(doc, params, pk) when is_keyword(doc),
    do: document(doc, params, pk)
  def encode(list, params, pk) when is_list(list),
    do: map_list(list, &encode(&1, params, pk))
  def encode({:^, _, [idx]}, params, pk),
    do: elem(params, idx) |> encode(params, pk)
  def encode({:^, _, [idx | _]}, params, pk),
    do: elem(params, idx) |> encode(params, pk)
  def encode(%Tagged{value: value, type: type}, params, _pk),
    do: {:ok, typed_value(value, type, params)}
  def encode(%{__struct__: _} = struct, _params, pk),
    do: encode(struct, pk) # Pass down other structs
  def encode(map, params, pk) when is_map(map),
    do: document(map, params, pk)
  def encode(value, _params, pk),
    do: encode(value, pk)

  def encode(doc, pk) when is_keyword(doc),
    do: document(doc, pk)
  def encode(string, _pk) when is_binary(string),
    do: {:ok, string}
  def encode(boolean, _pk) when is_boolean(boolean),
    do: {:ok, boolean}
  def encode(nil, _pk) do
    {:ok, nil}
  end
  def encode(list, pk) when is_list(list),
    do: map_list(list, &encode(&1, pk))
  def encode(%Tagged{value: value, type: type}, _pk),
    do: {:ok, typed_value(value, type)}
  def encode(%Riak.Ecto.Counter{value: value, increment: increment}, _pk) do
    {:ok, {:counter, value, increment}}
  end
  def encode(%Riak.Ecto.Set{} = set, _pk) do
    {:ok, {:set, Enum.into(set, [])}}
  end
  def encode(%{__struct__: change, field: field, value: value}, pk)
  when change in [Riak.Ecto.ChangeMap, Riak.Ecto.ChangeArray] do
    case encode(value, pk) do
      {:ok, value} -> {:ok, {field, value}}
      :error       -> :error
    end
  end
  def encode(%{__struct__: _}, _pk),
    do: :error # Other structs are not supported
  def encode(map, pk) when is_map(map),
    do: document(map, pk)
  def encode({{_, _, _} = date, {hour, min, sec, _usec}}, _pk) do
    iso8601 = Ecto.DateTime.from_erl({date, {hour, min, sec}}) |> Ecto.DateTime.to_iso8601
    {:ok, iso8601}
  end
  def encode(_value, _pk) do
    :error
  end

  defp document(doc, pk) do
    map(doc, fn {key, value} ->
      pair(key, value, pk, &encode(&1, pk))
    end)
  end

  defp document(doc, params, pk) do
    map(doc, fn {key, value} ->
      pair(key, value, pk, &encode(&1, params, pk))
    end)
  end

  defp pair(key, value, pk, fun) do
    case fun.(value) do
      {:ok, encoded} -> {:ok, {key(key, pk), encoded}}
      :error         -> :error
    end
  end

  defp key(pk, pk), do: :id
  defp key(key, _), do: key

  defp typed_value({:^, _, [idx]}, type, params),
    do: typed_value(elem(params, idx), type)
  defp typed_value(value, type, _params),
    do: typed_value(value, type)

  defp typed_value(nil, _type),
    do: nil

  defp typed_value(value, :any),
    do: value
  require Logger
  defp typed_value(value, {:array, type}) do
    Logger.debug "TYPED VALUE #{inspect(value)} :: #{inspect(type)}"
    Enum.map(value, &typed_value(&1, type))
  end
  defp typed_value(value, :binary),
    do: value
  defp typed_value(value, :uuid),
    do: value
  defp typed_value(value, :binary_id),
    do: value

  defp map(list, fun) do
    return =
      Enum.reduce(list, {%{}, :ok}, fn
        elem, {acc, :ok} ->
          case fun.(elem) do
            {:ok, {k, v}} -> {Map.put(acc, k, v), :ok}
            :error        -> {:halt, :error}
          end
        _, {:halt, :error} -> {:halt, :error}
      end)

    case return do
      {values,  :ok}    -> {:ok, values}
      {_values, :error} -> :error
    end
  end

  defp map_list(list, fun) do
    return =
      Enum.flat_map_reduce(list, :ok, fn elem, :ok ->
        case fun.(elem) do
          {:ok, value} -> {[value], :ok}
          :error       -> {:halt, :error}
        end
      end)

    case return do
      {values,  :ok}    -> {:ok, values}
      {_values, :error} -> :error
    end
  end

end
