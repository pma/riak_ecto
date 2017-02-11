defmodule Riak.Connection do
  use GenServer

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
    timeout = opts[:timeout] || 5_000
    heartbeat = opts[:heartbeat] || 10_000

    opts = opts
    |> Keyword.put_new(:hostname, "localhost")
    |> Keyword.update!(:hostname, &to_char_list/1)
    |> Keyword.put_new(:port, 8087)
    |> Keyword.delete(:timeout)

    send(self(), :connect)

    {:ok, %{pid: nil, opts: opts, timeout: timeout, heartbeat: heartbeat}}
  end

  def fetch_type(pid, bucket_type, bucket, id) do
    GenServer.call(pid, {:fetch_type, bucket_type, bucket, id}, :infinity)
  end

  def update_type(pid, bucket_type, bucket, id, dt) do
    GenServer.call(pid, {:update_type, bucket_type, bucket, id, dt}, :infinity)
  end

  def search(pid, index, bucket, query, opts) do
    GenServer.call(pid, {:search, index, bucket, query, opts}, :infinity)
  end

  def delete(pid, bucket_type, bucket, id) do
    GenServer.call(pid, {:delete, bucket_type, bucket, id}, :infinity)
  end

  def handle_call({:fetch_type, bucket_type, bucket, id}, _from, s) do
    case :riakc_pb_socket.fetch_type(s.pid, {bucket_type, bucket}, id) do
      {:ok, dt}        -> {:reply, {:ok, dt}, s}
      {:error, reason} -> {:reply, {:error, reason}, s}
    end
  end

  def handle_call({:update_type, bucket_type, bucket, nil, dt}, _from, s) do
    case :riakc_pb_socket.update_type(s.pid, {bucket_type, bucket}, :undefined, dt) do
      {:ok, id}        -> {:reply, {:ok, id}, s}
      {:error, reason} -> {:reply, {:error, reason}, s}
    end
  end

  def handle_call({:update_type, bucket_type, bucket, id, dt}, _from, s) do
    case :riakc_pb_socket.update_type(s.pid, {bucket_type, bucket}, id, dt) do
      :ok              -> {:reply, :ok, s}
      {:error, reason} -> {:reply, {:error, reason}, s}
    end
  end

  def handle_call({:delete, bucket_type, bucket, id}, _from, s) do
    case :riakc_pb_socket.delete(s.pid, {bucket_type, bucket}, id, []) do
      :ok              -> {:reply, :ok, s}
      {:error, reason} -> {:reply, {:error, reason}, s}
    end
  end

  def handle_call({:search, index, bucket, query, opts}, _from, s) do
    opts = opts
    |> Keyword.put_new(:filter, "")
    |> Keyword.update!(:filter,
      fn "" -> "_yz_rb:#{bucket}"
         v  -> "_yz_rb:#{bucket} AND (#{v})"
      end)
    case :riakc_pb_socket.search(s.pid, index, query, opts) do
      {:ok, {:search_results, _docs, _max_score, _num_found} = search_results} ->
        {:reply, {:ok, search_results}, s}
      {:error, reason} ->
        {:reply, {:error, reason}, s}
    end
  end

  def handle_info(:connect, %{opts: opts} = s) do
    host = opts[:hostname]
    port = opts[:port]
    {:ok, pid} = :riakc_pb_socket.start_link(host, port,
                                             [queue_if_disconnected: false,
                                              auto_reconnect: true,
                                              keepalive: true])

    :timer.send_after(s.heartbeat, self(), :heartbeat)

    {:noreply, %{s | pid: pid}}
  end

  def handle_info(:heartbeat, s) do
    case :riakc_pb_socket.is_connected(s.pid, s.timeout) do
      true ->
        case :riakc_pb_socket.ping(s.pid, s.timeout) do
          :pong ->
            :timer.send_after(s.heartbeat, self(), :heartbeat)
            {:noreply, s}
          {:error, _reason} ->
            :ok = :riakc_pb_socket.stop(s.pid)
            handle_info(:connect, s)
        end
      {false, _} ->
        :timer.send_after(s.heartbeat, self(), :heartbeat)
        {:noreply, s}
    end
  end

  def terminate(_, s) do
    :riakc_pb_socket.stop(s.pid)
  end
end
