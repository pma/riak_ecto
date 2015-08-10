defmodule Riak.Connection do
  use GenServer
  require Logger

  @spec start_link(Keyword.t) :: GenServer.on_start
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  Stops the connection process.
  """
  @spec stop(pid) :: :ok
  def stop(conn) do
    GenServer.cast(conn, :stop)
  end

  @doc false
  def init(opts) do
    :random.seed(:os.timestamp)
    timeout = opts[:timeout] || 5000
    bucket_type = opts[:bucket_type]

    opts = opts
    |> Keyword.put_new(:hostname, "localhost")
    |> Keyword.update!(:hostname, &to_char_list/1)
    |> Keyword.put_new(:port, 8087)
    |> Keyword.put_new(:backoff, 1000)
    |> Keyword.delete(:timeout)
    |> Keyword.delete(:bucket_type)

    s = %{pid: nil, opts: opts, bucket_type: bucket_type,
          timeout: timeout}

    host = opts[:hostname]
    port = opts[:port]
    {:ok, pid} = :riakc_pb_socket.start_link(host, port, [queue_if_disconnected: false, auto_reconnect: true, keepalive: true])

    s = %{s | pid: pid}

    {:ok, s}
  end

  def disconnect(:close, s) do
    {:stop, :normal, s}
  end

  def fetch_type(pid, bucket, id) do
    GenServer.call(pid, {:fetch_type, bucket, id})
  end

  def update_type(pid, bucket, id, dt) do
    GenServer.call(pid, {:update_type, bucket, id, dt})
  end

  def search(pid, index, query, opts) do
    GenServer.call(pid, {:search, index, query, opts})
  end

  def handle_call({:fetch_type, bucket, id}, _from, s) do
    case :riakc_pb_socket.fetch_type(s.pid, {s.bucket_type, bucket}, id) do
      {:ok, dt}        -> {:reply, {:ok, dt}, s}
      {:error, reason} -> {:reply, {:error, reason}, s}
    end
  end

  def handle_call({:update_type, bucket, id, dt}, _from, s) do
    case :riakc_pb_socket.update_type(s.pid, {s.bucket_type, bucket}, id, dt) do
      :ok       -> {:reply, :ok, s}
      {:ok, id} -> {:reply, {:ok, id}, s}
    end
  end

  def handle_call({:search, index, query, opts}, _from, s) do
    case :riakc_pb_socket.search(s.pid, index, query, opts) do
      {:ok, {:search_results, _docs, _max_score, _num_found} = search_results} ->
        {:reply, {:ok, search_results}, s}
      {:error, reason} ->
        {:reply, {:error, reason}, s}
    end
  end

  @doc false
  def handle_cast(:stop, s) do
    {:disconnect, :close, s}
  end

  def terminate(_, s) do
    :riakc_pb_socket.stop(s.pid)
  end

end
