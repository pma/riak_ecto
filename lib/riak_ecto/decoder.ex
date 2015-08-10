defmodule Riak.Ecto.Decoder do
  @moduledoc false

  def decode_document(document, pk) do
    Enum.into(document, %{}, fn
      {:key, value} -> {to_string(pk), value}
      {key,  value} -> {key, decode_value(value, pk)}
    end)
  end

  def decode_value(string, _pk) when is_binary(string) do
    string
  end

  def decode_value(boolean, _pk) when is_boolean(boolean) do
    boolean
  end

  def decode_value(map, pk) when is_map(map) do
    decode_document(map, pk)
  end

end
