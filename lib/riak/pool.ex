defmodule Riak.Pool do
  @moduledoc """
  Defines a pool of Riak connections.

  A pool can be defined as:

      defmodule MyPool do
        use Riak.Pool,
          adapter: Riak.Pool.Poolboy,
          hostname: "localhost"
       end

  Options will be passed to the pool adapter and to `Mongo.Connection`.

  ## Logging

  The pool may define a `log/5` function, that will be called by the
  driver on every call to the database.

  The first argument result can be of form: `:ok`, `{:ok, _}` or `{:error, _}`.
  The second element of the tuples should be considered private, and not used.

  The fourth argument determines the operation, these can be (listed with the
  arguments passed as the fifth argument to the log function):

    * `:run_command`,  `[query, options]`
    * `:fetch_type`,   `[bucket, key]`

  """

  use Behaviour

  @type t :: module

  @doc false
  defmacro __using__(opts) do
    adapter = Keyword.fetch!(opts, :adapter)

    quote do
      # TODO: Customizable timeout
      @timeout   5_000
      @behaviour unquote(__MODULE__)
      @adapter   unquote(adapter)
      @name      __MODULE__
      @sup       __MODULE__.Sup

      def start_link(opts) do
        import Supervisor.Spec, warn: false

        children = [
          worker(@adapter, [@name, opts])
        ]

        opts = [strategy: :one_for_all, name: @sup]
        Supervisor.start_link(children, opts)
      end

      def stop do
        Process.whereis(__MODULE__)
        |> Process.exit(:shutdown)
      end

      def run(fun) do
        @adapter.run(@name, fun)
      end

      def log(return, queue_time, query_time, _fun, _args) do
        return
      end

      defoverridable [log: 5]
    end
  end

  @type time :: integer

  defcallback run((pid -> return)) :: {queue_time :: time, return} when return: var
  defcallback log(return, queue_time, query_time, fun :: atom, args :: list) ::
    return when return: var, queue_time: time, query_time: time

  def run_with_log(pool, log, args, opts, fun) do
    {log?, opts} = Keyword.pop(opts, :log, true)

    if log? do
      {queue_time, {query_time, value}} =
        pool.run(&:timer.tc(fun, [&1]))

      pool.log(value, queue_time, query_time, log, args ++ [opts])
      value
    else
      {_queue_time, value} = pool.run(fun)
      value
    end
  end
end
