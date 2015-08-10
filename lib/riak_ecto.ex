defmodule Riak.Ecto do
  require Logger

  @moduledoc """
  Adapter module for Riak, using a map bucket_type to store models.
  It uses `riakc` for communicating with the database and manages
  a connection pool using `poolboy`.
  ## Features
  * WIP
  """

  @behaviour Ecto.Adapter

  alias Riak.Ecto.NormalizedQuery
  alias Riak.Ecto.NormalizedQuery.ReadQuery
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
  def load(:float, data) when is_binary(data),
    do: Ecto.Type.load(:float, String.to_float(data), &load/2)
  def load(:integer, data) when is_binary(data),
    do: Ecto.Type.load(:integer, String.to_integer(data), &load/2)
  def load(type, data),
    do: Ecto.Type.load(type, data, &load/2)

  @doc false
  def dump(:binary_id, data),
    do: Ecto.Type.dump(:string, data, &dump/2)
  def dump(:float, data) when is_float(data),
    do: Ecto.Type.dump(:string, String.Chars.Float.to_string(data), &dump/2)
  def dump(:integer, data) when is_integer(data),
    do: Ecto.Type.dump(:string, String.Chars.Integer.to_string(data), &dump/2)
  def dump(Ecto.Date, %Ecto.DateTime{} = data),
    do: Ecto.Type.dump(Ecto.DateTime, Ecto.DateTime.to_iso8601(data), &dump/2)
  def dump(Ecto.Date, %Ecto.Date{} = data),
    do: Ecto.Type.dump(Ecto.Date, Ecto.Date.to_iso8601(data), &dump/2)
  def dump(type, data),
    do: Ecto.Type.dump(type, data, &dump/2)

  @doc false
  def embed_id(_), do: Flaky.alpha

  @doc false
  def prepare(function, query) do
    {:nocache, {function, query}}
  end

  @doc false
  def execute(repo, _meta, {function, query}, params, preprocess, opts) do
    case apply(NormalizedQuery, function, [query, params]) do
      %ReadQuery{} = read ->
        {rows, count} =
          Connection.all(repo.__riak_pool__, read, opts)
        |> Enum.map_reduce(0, &{process_document(&1, read, preprocess), &2 + 1})
        {count, rows}
      %WriteQuery{} = write ->
        result = apply(Connection, function, [repo.__riak_pool__, write, opts])
        {result, nil}
    end
  end

  @doc false
  def insert(_repo, source, _params, {key, :id, _}, _returning, _opts) do
    raise ArgumentError, "Riak adapter does not support :id field type in models. " <>
      "The #{inspect key} field in #{inspect source} is tagged as such."
  end

  def insert(_repo, source, _params, _autogen, [_] = returning, _opts) do
    raise ArgumentError,
    "Riak adapter does not support :read_after_writes in models. " <>
      "The following fields in #{inspect source} are tagged as such: #{inspect returning}"
  end

  def insert(repo, source, params, nil, [], opts) do
    normalized = NormalizedQuery.insert(source, params, nil)

    {:ok, _} = Connection.insert(repo.__riak_pool__, normalized, opts)
    {:ok, []}
  end

  def insert(repo, source, params, {pk, :binary_id, nil}, [], opts) do
    normalized = NormalizedQuery.insert(source, params, pk)

    {:ok, %{inserted_id: value}} =
      Connection.insert(repo.__riak_pool__, normalized, opts)
    {:ok, [{pk, value}]}
  end

  def insert(repo, source, params, {pk, :binary_id, _value}, [], opts) do
    normalized = NormalizedQuery.insert(source, params, pk)

    {:ok, _} = Connection.insert(repo.__riak_pool__, normalized, opts)
    {:ok, []}
  end

  @doc false
  def update(_repo, source, _fields, _filter, {key, :id, _}, _returning, _opts) do
    raise ArgumentError, "Riak adapter does not support :id field type in models. " <>
      "The #{inspect key} field in #{inspect source} is tagged as such."
  end

  def update(_repo, source, _fields, _filter, _autogen, [_|_] = returning, _opts) do
    raise ArgumentError,
    "Riak adapter does not support :read_after_writes in models. " <>
      "The following fields in #{inspect source} are tagged as such: #{inspect returning}"
  end

  def update(repo, source, fields, filter, {pk, :binary_id, _value}, [], opts) do
    normalized = NormalizedQuery.update(source, fields, filter, pk)

    Connection.update(repo.__riak_pool__, normalized, opts)
  end

  @doc false
  def delete(_repo, source, _filter, {key, :id, _}, _opts) do
    raise ArgumentError, "Riak adapter does not support :id field type in models. " <>
      "The #{inspect key} field in #{inspect source} is tagged as such."
  end

  def delete(repo, source, filter, {pk, :binary_id, _value}, opts) do
    normalized = NormalizedQuery.delete(source, filter, pk)

    Connection.delete(repo.__riak_pool__, normalized, opts)
  end

  defp process_document(document, %{fields: fields, pk: pk}, preprocess) do
    document = Decoder.decode_document(document, pk)
    Enum.map(fields, fn
      {:field, name, field} ->
        preprocess.(field, Map.get(document, Atom.to_string(name)))
      {:value, value, field} ->
        preprocess.(field, Decoder.decode_value(value, pk))
      field ->
        preprocess.(field, document)
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
  defp format_log(_entry, :search, [index, query, _opts]) do
    ["SEARCH", format_part("index", index), format_part("query", query)]
  end
  defp format_log(_entry, :insert_one, [coll, doc, _opts]) do
    ["INSERT", format_part("coll", coll), format_part("document", doc)]
  end
  defp format_log(_entry, :insert_many, [coll, docs, _opts]) do
    ["INSERT", format_part("coll", coll), format_part("documents", docs)]
  end
  defp format_log(_entry, :delete_one, [coll, filter, _opts]) do
    ["DELETE", format_part("coll", coll), format_part("filter", filter),
     format_part("many", false)]
  end
  defp format_log(_entry, :delete_many, [coll, filter, _opts]) do
    ["DELETE", format_part("coll", coll), format_part("filter", filter),
     format_part("many", true)]
  end
  defp format_log(_entry, :replace_one, [coll, filter, doc, _opts]) do
    ["REPLACE", format_part("coll", coll), format_part("filter", filter),
     format_part("document", doc)]
  end
  defp format_log(_entry, :update_one, [coll, filter, update, _opts]) do
    ["UPDATE", format_part("coll", coll), format_part("filter", filter),
     format_part("update", update), format_part("many", false)]
  end
  defp format_log(_entry, :update_many, [coll, filter, update, _opts]) do
    ["UPDATE", format_part("coll", coll), format_part("filter", filter),
     format_part("update", update), format_part("many", true)]
  end
  defp format_log(_entry, :find_cursor, [coll, query, projection, _opts]) do
    ["FIND", format_part("coll", coll), format_part("query", query),
     format_part("projection", projection)]
  end
  defp format_log(_entry, :find_batch, [coll, cursor, _opts]) do
    ["GET_MORE", format_part("coll", coll), format_part("cursor_id", cursor)]
  end
  defp format_log(_entry, :kill_cursors, [cursors, _opts]) do
    ["KILL_CURSORS", format_part("cursor_ids", cursors)]
  end

  defp format_part(name, value) do
    [" ", name, "=" | inspect(value)]
  end

end
