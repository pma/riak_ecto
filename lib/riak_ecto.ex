defmodule Riak.Ecto do

  @moduledoc """
  Adapter module for Riak, using a map bucket_type to store models.
  It uses `riakc` for communicating with the database and manages
  a connection pool using `poolboy`.
  ## Features
  * WIP
  """

  @behaviour Ecto.Adapter

  alias Riak.Ecto.NormalizedQuery
  alias Riak.Ecto.NormalizedQuery.SearchQuery
  alias Riak.Ecto.NormalizedQuery.FetchQuery
  alias Riak.Ecto.NormalizedQuery.CountQuery
  alias Riak.Ecto.NormalizedQuery.WriteQuery
  alias Riak.Ecto.Decoder
  alias Riak.Ecto.Connection

  ## Adapter

  @doc false
  defmacro __before_compile__(env) do
    module = env.module
    config = Module.get_attribute(module, :config)
    adapter = Keyword.get(config, :pool, Riak.Pool.Poolboy)

    quote do
      defmodule Pool do
        use Riak.Pool, name: __MODULE__, adapter: unquote(adapter)

        def log(return, queue_time, query_time, fun, args) do
          Riak.Ecto.log(unquote(module), return, queue_time, query_time, fun, args)
        end
      end

      def __riak_pool__, do: unquote(module).Pool
    end
  end

  @doc false
  def start_link(repo, opts) do
    {:ok, _} = Application.ensure_all_started(:riak_ecto)

    repo.__riak_pool__.start_link(opts)
  end

  @doc false
  def load(:binary_id, data),
    do: Ecto.Type.load(:string, data, &load/2)
  def load(Ecto.DateTime, data) when is_binary(data) do
    case Ecto.DateTime.cast(data) do
      {:ok, datetime} ->
        Ecto.Type.load(Ecto.DateTime, Ecto.DateTime.to_erl(datetime), &load/2)
      :error ->
        :error
    end
  end
  def load(Ecto.Date, data) when is_binary(data) do
    case Ecto.Date.cast(data) do
      {:ok, date} ->
        Ecto.Type.load(Ecto.Date, Ecto.Date.to_erl(date), &load/2)
      :error ->
        :error
    end
  end

  def load(Riak.Ecto.Counter, data) do
    Ecto.Type.load(Riak.Ecto.Counter, data, &load/2)
  end

  def load(Riak.Ecto.Set, data) do
    Ecto.Type.load(Riak.Ecto.Set, data, &load/2)
  end

  def load(:float, data) when is_binary(data),
    do: Ecto.Type.load(:float, String.to_float(data), &load/2)
  def load(:integer, data) when is_binary(data),
    do: Ecto.Type.load(:integer, String.to_integer(data), &load/2)

  def load({:embed, %Ecto.Embedded{cardinality: :many}} = type, nil),
    do: Ecto.Type.load(type, nil, &load/2)

  def load({:embed, %Ecto.Embedded{cardinality: :many}} = type, data) do
    data = Enum.reduce(data, [], fn {k, v}, acc ->
      [Map.put(v, "id", k) | acc]
    end)
    Ecto.Type.load(type, data, &load/2)
  end

  def load({:array, _} = type, nil),
    do: Ecto.Type.load(type, nil, &load/2)

  def load({:array, _} = type, data) do
    data = data
    |> Enum.into([])
    |> Enum.map(&elem(&1, 1))
    Ecto.Type.load(type, data, &load/2)
  end

  def load(type, data) do
    Ecto.Type.load(type, data, &load/2)
  end

  @doc false
  def dump(:binary_id, data),
    do: Ecto.Type.dump(:string, data, &dump/2)
  def dump(:float, data) when is_float(data),
    do: Ecto.Type.dump(:string, String.Chars.Float.to_string(data), &dump/2)
  def dump(:integer, data) when is_integer(data),
    do: Ecto.Type.dump(:string, String.Chars.Integer.to_string(data), &dump/2)
  def dump(Ecto.Date, %Ecto.DateTime{} = data),
    do: Ecto.Type.dump(:string, Ecto.DateTime.to_iso8601(data), &dump/2)
  def dump(Ecto.Date, %Ecto.Date{} = data),
    do: Ecto.Type.dump(:string, Ecto.Date.to_iso8601(data), &dump/2)
  def dump(type, data) do
    Ecto.Type.dump(type, data, &dump/2)
  end

  @doc false
  def embed_id(_), do: Flaky.alpha

  @doc false
  def prepare(function, query) do
    {:nocache, {function, query}}
  end

  @doc false
  def execute(_repo, _meta, {:update_all, _query}, _params, _preprocess, _opts) do
    raise ArgumentError, "Riak adapter does not support update_all."
  end

  def execute(_repo, _meta, {:delete_all, _query}, _params, _preprocess, _opts) do
    raise ArgumentError, "Riak adapter does not support delete_all."
  end

  @read_queries [SearchQuery, FetchQuery]

  def execute(repo, _meta, {function, query}, params, preprocess, opts) do
    case apply(NormalizedQuery, function, [query, params]) do
      %{__struct__: read} = query when read in [FetchQuery, SearchQuery, CountQuery] ->
        {rows, count} =
          Connection.all(repo.__riak_pool__, query, opts)
          |> Enum.map_reduce(0, &{process_document(&1, query, preprocess), &2 + 1})
        {count, rows}
      %WriteQuery{} = write ->
        result = apply(Connection, function, [repo.__riak_pool__, write, opts])
        {result, nil}
    end
  end

  @doc false
  def insert(_repo, meta, _params, {key, :id, _}, _returning, _opts) do
    raise ArgumentError,
      "Riak adapter does not support :id field type in models. " <>
      "The #{inspect key} field in #{inspect meta.model} is tagged as such."
  end

  def insert(_repo, meta, _params, _autogen, [_] = returning, _opts) do
    raise ArgumentError,
      "Riak adapter does not support :read_after_writes in models. " <>
      "The following fields in #{inspect meta.model} are tagged as such: #{inspect returning}"
  end

  def insert(repo, meta, params, nil, [], opts) do
    normalized = NormalizedQuery.insert(meta, params, nil)

    case Connection.insert(repo.__riak_pool__, normalized, opts) do
      {:ok, _} -> {:ok, []}
      other    -> other
    end
  end

  def insert(repo, meta, params, {pk, :binary_id, nil}, [], opts) do
    normalized = NormalizedQuery.insert(meta, params, pk)

    case Connection.insert(repo.__riak_pool__, normalized, opts) do
      {:ok, %{inserted_id: value}} -> {:ok, [{pk, value}]}
      other -> other
    end
  end

  def insert(repo, meta, params, {pk, :binary_id, _value}, [], opts) do
    normalized = NormalizedQuery.insert(meta, params, pk)

    case Connection.insert(repo.__riak_pool__, normalized, opts) do
      {:ok, _} -> {:ok, []}
      other    -> other
    end
  end

  @doc false
  def update(_repo, meta, _fields, _filter, {key, :id, _}, _returning, _opts) do
    raise ArgumentError,
      "Riak adapter does not support :id field type in models. " <>
      "The #{inspect key} field in #{inspect meta.model} is tagged as such."
  end

  def update(_repo, meta, _fields, _filter, _autogen, [_|_] = returning, _opts) do
    raise ArgumentError,
      "Riak adapter does not support :read_after_writes in models. " <>
      "The following fields in #{inspect meta.model} are tagged as such: #{inspect returning}"
  end

  def update(_repo, %{context: nil} = meta, _fields, _filter, _, _, _opts) do
    raise ArgumentError,
      "No causal context in #{inspect meta.model}. " <>
      "Get the model by id before trying to update it."
  end

  def update(repo, meta, fields, filter, {pk, :binary_id, _value}, [], opts) do
    normalized = NormalizedQuery.update(meta, fields, filter, pk)
    Connection.update(repo.__riak_pool__, normalized, opts)
  end

  @doc false
  def delete(_repo, meta, _filter, {key, :id, _}, _opts) do
    raise ArgumentError,
      "Riak adapter does not support :id field type in models. " <>
      "The #{inspect key} field in #{inspect meta.model} is tagged as such."
  end

  def delete(repo, meta, filter, {pk, :binary_id, _value}, opts) do
    normalized = NormalizedQuery.delete(meta.source, meta.context, filter, pk)

    Connection.delete(repo.__riak_pool__, normalized, opts)
  end

  defp process_document(document, %{fields: fields, pk: pk}, preprocess) do
    document = Decoder.decode_document(document, pk)

    Enum.map(fields, fn
      {:field, name, field} ->
        preprocess.(field, Map.get(document, Atom.to_string(name)), document[:context])
      {:value, value, field} ->
        preprocess.(field, Decoder.decode_value(value, pk), document[:context])
      field ->
        preprocess.(field, document, document[:context])
    end)
  end

  @doc false
  def log(repo, :ok, queue_time, query_time, fun, args) do
    log(repo, {:ok, nil}, queue_time, query_time, fun, args)
  end
  def log(repo, return, queue_time, query_time, fun, args) do
    entry =
      %Ecto.LogEntry{query: &format_log(&1, fun, args), params: [],
                     result: return, query_time: query_time, queue_time: queue_time}
    repo.log(entry)
  end

  defp format_log(_entry, :run_command, [command, _opts]) do
    ["COMMAND " | inspect(command)]
  end
  defp format_log(_entry, :fetch_type, [bucket, id, _opts]) do
    ["FETCH_TYPE", format_part("bucket", bucket), format_part("id", id)]
  end
  defp format_log(_entry, :update_type, [bucket, id, _opts]) do
    ["UPDATE_TYPE", format_part("bucket", bucket), format_part("id", id)]
  end
  defp format_log(_entry, :search, [index, filter, _opts]) do
    ["SEARCH", format_part("index", index), format_part("filter", filter)]
  end
  defp format_log(_entry, :delete, [coll, filter, _opts]) do
    ["DELETE", format_part("coll", coll), format_part("filter", filter),
     format_part("many", false)]
  end

  defp format_part(name, value) do
    [" ", name, "=" | inspect(value)]
  end
end
