defmodule Riak.Ecto.Utils do
  @moduledoc false

  # Make sure you use this before is_list/1
  defmacro is_keyword(doc) do
    quote do
      unquote(doc) |> hd |> tuple_size == 2
    end
  end

  def unique_id_62 do
    rand = :crypto.hash(:sha, :erlang.term_to_binary({make_ref(), :os.timestamp()}))
    <<i::integer-160>> = rand
    encode(i)
  end

  @dlen 64
  alphabet = Enum.with_index '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'

  for {encoding, value} <- alphabet do
    defp enc(unquote(value)), do: <<unquote(encoding)>>
  end

  defp encode(data) when is_binary(data) do
    <<n::size(@dlen)>> = data
    encode(n)
  end

  defp encode(n) when is_integer(n) do
    encode(n, "")
  end

  defp encode(n, r) do
    d = rem(n, 62)
    i = div(n, 62)
    r = enc(d) <> r

    cond do
      i === 0 -> r
      true    -> encode(i, r)
    end
  end

end
