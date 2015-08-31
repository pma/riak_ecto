defmodule Riak.Ecto.Set do
  @moduledoc """
  An Ecto type to represent a partial update of a nested document
  ## Using in queries
  change = Riak.Ecto.Helpers.change_map("name", "name")
  MyRepo.update_all Post, set: [author: change]
  """

  @behaviour Ecto.Type
  @behaviour Set

  defstruct set: HashSet.new

  @doc """
  The Ecto primitive type
  """
  def type, do: :any

  @doc """
  Casts to database format
  """
  def cast(%__MODULE__{} = value), do: {:ok, value}
  def cast(_), do: :error

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

  defimpl Inspect do
    import Inspect.Algebra

    def inspect(%Riak.Ecto.Set{set: set}, opts) do
      concat ["#Riak.Ecto.Set<", Inspect.List.inspect(HashSet.to_list(set), opts), ">"]
    end
  end

  def delete(%__MODULE__{set: set}, term) do
    %__MODULE__{set: HashSet.delete(set, term)}
  end

  def difference(%__MODULE__{set: set1}, %__MODULE__{set: set2}) do
    %__MODULE__{set: HashSet.difference(set1, set2)}
  end

  def disjoint?(%__MODULE__{set: set1}, %__MODULE__{set: set2}) do
    HashSet.disjoint?(set1, set2)
  end

  def equal?(%__MODULE__{set: set1}, %__MODULE__{set: set2}) do
    HashSet.equal?(set1, set2)
  end

  def intersection(%__MODULE__{set: set1}, (%__MODULE__{set: set2})) do
    %__MODULE__{set: HashSet.intersection(set1, set2)}
  end

  def member?(%__MODULE__{set: set}, term) do
    HashSet.member?(set, term)
  end

  def new(), do: %Riak.Ecto.Set{set: HashSet.new}

  def put(%__MODULE__{set: set}, term) when is_binary(term) do
    %__MODULE__{set: HashSet.put(set, term)}
  end

  def size(%__MODULE__{set: set}), do: HashSet.size(set)

  def subset?(%__MODULE__{set: set1}, %__MODULE__{set: set2}) do
    HashSet.subset?(set1, set2)
  end

  def to_list(%__MODULE__{set: set}) do
    HashSet.to_list(set)
  end

  def union(%__MODULE__{set: set1}, (%__MODULE__{set: set2})) do
    %__MODULE__{set: HashSet.union(set1, set2)}
  end

  defimpl Enumerable, for: Riak.Ecto.Set do
    def reduce(%Riak.Ecto.Set{set: set}, acc, fun), do: HashSet.reduce(set, acc, fun)
    def member?(set, v),       do: {:ok, Riak.Ecto.Set.member?(set, v)}
    def count(set),            do: {:ok, Riak.Ecto.Set.size(set)}
  end

  defimpl Collectable, for: Riak.Ecto.Set do
    def empty(_dict) do
      Riak.Ecto.Set.new
    end

    def into(original) do
      {original, fn
        set, {:cont, x} -> Riak.Ecto.Set.put(set, x)
      set, :done -> set
      _, :halt -> :ok
      end}
    end
  end

end
