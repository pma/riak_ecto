defmodule Riak do
  alias Riak.Pool
  alias Riak.Connection

  def fetch_type(pool, bucket, id) do
    Pool.run_with_log(pool, :fetch_type, [bucket, id], [], fn pid ->
      case Connection.fetch_type(pid, bucket, id) do
        {:ok, map}                  -> {:ok, map}
        {:error, {:notfound, :map}} -> {:error, :not_found}
        {:error, reason}            -> {:error, reason}
      end
    end)
  end

  def update_type(pool, bucket, id, update) do
    Pool.run_with_log(pool, :update_type, [bucket, id], [], fn pid ->
      case Connection.update_type(pid, bucket, id, update) do
        :ok              -> :ok
        {:ok, id}        -> {:ok, id}
        {:error, reason} -> {:error, reason}
      end
    end)
  end

  def delete(pool, bucket, id) do
    Pool.run_with_log(pool, :delete_one, [bucket, id], [], fn pid ->
      case Connection.delete(pid, bucket, id) do
        :ok              -> :ok
        {:error, reason} -> {:error, reason}
      end
    end)
  end

  def search(pool, index, query, opts) do
    opts = opts ++ [{:sort, opts[:order]}]
    |> filter_nils

    Pool.run_with_log(pool, :search, [index, opts[:filter]], [], fn pid ->
      case Connection.search(pid, index, query, search_options(opts)) do
        {:ok, {:search_results, results, _score, _total_count}} ->
        {:ok, results}
      end
    end)
  end

  defp search_options(opts) do
    Keyword.take(opts, [:rows, :start, :sort, :filter, :df, :op, :fl, :presort])
  end

  defp filter_nils(keyword) when is_list(keyword) do
    Enum.reject(keyword, fn {_key, value} -> is_nil(value) end)
  end

  defp filter_nils(map) when is_map(map) do
    map
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Enum.into(%{})
  end
end
