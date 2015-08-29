defmodule Riak.Ecto.Counter do
  @moduledoc """
  An Ecto type to represent a partial update of a nested document
  ## Using in queries
  change = Riak.Ecto.Helpers.change_map("name", "name")
  MyRepo.update_all Post, set: [author: change]
  """

  @behaviour Ecto.Type

  defstruct value: 0, increment: nil
  @type t :: %__MODULE__{value: integer, increment: integer}

  @doc """
  The Ecto primitive type
  """
  def type, do: :any

  @doc """
  Casts to database format
  """
  def cast(%__MODULE__{} = value), do: {:ok, value}
  def cast(_), do: :eror

  @doc """
  Converts to a database format
  """
  def dump(%__MODULE__{} = value), do: {:ok, value}
  def dump(_), do: :error

  @doc """
  Change is not a value - it can't be loaded
  """

  def load(%__MODULE__{} = value), do: {:ok, value}
  def load(_), do: :error

  def inc(%__MODULE__{} = counter, amount \\ 1) do
    value = counter.value
    increment = case counter.increment do
                  :undefined ->
                    0
                  increment when is_integer(increment) ->
                    increment
                end
    %{counter | value: value, increment: increment+amount}
  end

  def dec(%__MODULE__{} = counter, amount \\ 1) do
    inc(counter, -amount)
  end


  defimpl Inspect do
    import Inspect.Algebra

    def inspect(counter, _opts) do
      if counter.increment == :undefined do
        concat ["#Riak.Ecto.Counter<", "#{counter.value}", ">"]
      else
        concat ["#Riak.Ecto.Counter<", "#{counter.value+counter.increment}", ">"]
      end
    end
  end

end
