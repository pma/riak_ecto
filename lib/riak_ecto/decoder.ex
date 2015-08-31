defmodule Riak.Ecto.Decoder do
  @moduledoc false

  def decode_document(document, pk) do
    Enum.reduce(document, %{}, fn
      {:id, value}, acc  -> Map.put(acc, to_string(pk), value)
      {:context, v}, acc -> Map.put(acc, :context, v)
      {key, value}, acc  -> Map.put(acc, key, decode_value(value, pk))
    end)
  end

  def decode_value(string, _pk) when is_binary(string),
    do: string

  def decode_value(boolean, _pk) when is_boolean(boolean),
    do: boolean

  def decode_value(value, _pk) when is_integer(value),
    do: %Riak.Ecto.Counter{value: value, increment: :undefined}

  def decode_value(value, _pk) when is_list(value),
    do: Enum.into(value, Riak.Ecto.Set.new)

  def decode_value(map, pk) when is_map(map),
    do: decode_document(map, pk)
end
