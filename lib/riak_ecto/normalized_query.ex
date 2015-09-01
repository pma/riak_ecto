defmodule Riak.Ecto.NormalizedQuery do
  @moduledoc false

  defmodule SearchQuery do
    @moduledoc false

    defstruct coll: nil, pk: nil, params: {}, query: %{},
              model: nil, filter: "", fields: [], order: nil,
              projection: %{}, opts: []
  end

  defmodule FetchQuery do
    @moduledoc false

    defstruct coll: nil, pk: nil, id: nil, fields: [],
              model: nil, projection: %{}, opts: []
  end

  defmodule CountQuery do
    @moduledoc false

    defstruct coll: nil, pk: nil, params: {}, query: %{},
              model: nil, filter: "", fields: [], order: nil,
              projection: %{}, opts: []
  end

  defmodule WriteQuery do
    @moduledoc false

    defstruct coll: nil, query: %{}, command: %{},
              filter: nil,
              model: nil, context: nil, opts: []
  end

  alias Riak.Ecto.Encoder
  alias Ecto.Query.Tagged
  alias Ecto.Query

  defmacrop is_op(op) do
    quote do
      is_atom(unquote(op)) and unquote(op) != :^
    end
  end

  def all(%Query{} = original, params) do
    check_query(original)

    from   = from(original)
    params = List.to_tuple(params)
    {filter, order} = filter_order(original, params, from)

    case projection(original, params, from) do
      {:count, fields} ->
        case filter do
          {:search, filter} ->
            count(original, filter, fields, from)
        end
      {projection, fields} ->
        case filter do
          {:fetch, id} ->
            find_one(original, id, projection, fields, params, from)
          {:search, filter} ->
            find_all(original, "*:*", filter, order, projection, fields, params, from)
        end
    end
  end

  defp find_all(original, query, filter, order, projection, fields, params, {coll, model, pk}) do
    opts = opts(:find_all, original, params, pk)

    %SearchQuery{coll: coll, pk: pk, params: params, query: query, projection: projection,
                 opts: opts, filter: filter, order: order, fields: fields, model: model}
  end

  defp count(_original, filter, fields, {coll, model, pk}) do
    %CountQuery{coll: coll, pk: pk, filter: filter,
                fields: fields, model: model}
  end

  defp find_one(original, id, projection, fields, params, {coll, model, pk}) do
    opts = opts(:find_one, original, params, pk)

    %FetchQuery{coll: coll, pk: pk, projection: projection, id: id, fields: fields,
                opts: opts, model: model}
  end

  def update(%{source: {_prefix, coll}, model: model, context: context}, values, filter, pk) do
    command = command(:update, values, pk)
    query   = query(filter, pk)

    %WriteQuery{coll: coll, query: query, command: command, context: context, model: model}
  end

  def delete({_prefix, coll}, context, filter, pk) do
    query = query(filter, pk)

    %WriteQuery{coll: coll, query: query, context: context}
  end

  def insert(%{context: _context, model: model, source: {_prefix, coll}}, document, pk) do
    command = command(:insert, document, model.__struct__(), pk)

    %WriteQuery{coll: coll, command: command}
  end

  defp from(%Query{from: {coll, model}}) do
    {coll, model, primary_key(model)}
  end

  defp filter_order(original, params, from) do
    #%{query: query, filters: filters} =
    filter = filter(original, params, from)
    order  = order(original, from)
    #query_filters_order(query, filters, order)
    {filter, order}
  end

  defp projection(%Query{select: nil}, _params, _from),
    do: {%{}, []}
  defp projection(%Query{select: %Query.SelectExpr{fields: fields}} = query, params, from),
    do: projection(fields, params, from, query, %{}, [])

  defp projection([], _params, _from, _query, pacc, facc),
    do: {pacc, Enum.reverse(facc)}
  defp projection([{:&, _, [0]} = field | rest], params, {_, model, pk} = from, query, pacc, facc)
  when  model != nil do
    pacc = Enum.into(model.__schema__(:types), pacc, fn {field, ecto_type} ->
      {field(field, pk), riak_type(ecto_type)}
    end)
    facc = [field | facc]

    projection(rest, params, from, query, pacc, facc)
  end
  defp projection([{:&, _, [0]} = field | rest], params, {_, nil, _} = from, query, _pacc, facc) do
    # Model is nil, we want empty projection, but still extract fields
    {_, facc} = projection(rest, params, from, query, %{}, [field | facc])
    {%{}, facc}
  end
  defp projection([{:count, _, _} = field], _params, _from, _query, pacc, _facc) when pacc == %{} do
    {:count, [{:field, :value, field}]}
  end
#  defp projection([{op, _, [name]} = field], _params, from, query, pacc, _facc) when pacc == %{} and op in [:count] do
#    {_, _, pk} = from
#    name  = field(name, pk, query, "select clause")
#    field = {:field, :value, field}
#    {:aggregate, [["$group": [_id: nil, value: [{"$#{op}", "$#{name}"}]]]], [field]}
#  end
  defp projection([{op, _, _} | _rest], _params, _from, query, _pacc, _facc) when is_op(op) do
    error(query, "select clause")
  end
  # We skip all values and then add them when constructing return result
  defp projection([%Tagged{value: {:^, _, [idx]}} = field | rest], params, from, query, pacc, facc) do
    {_, _, pk} = from
    value = params |> elem(idx) |> value(params, pk, query, "select clause")
    facc = [{:value, value, field} | facc]

    projection(rest, params, from, query, pacc, facc)
  end
  defp projection([field | rest], params, from, query, pacc, facc) do
    {_, _, pk} = from
    value = value(field, params, pk, query, "select clause")
    facc = [{:value, value, field} | facc]

    projection(rest, params, from, query, pacc, facc)
  end

  defp opts(:find_all, query, params, pk),
    do: [rows: rows(query, params, pk), start: start(query, params, pk)]

  defp opts(:find_one, _query, _params, _pk),
    do: []

  defp start(%Query{offset: offset} = query, params, pk), do: offset_limit(offset, query, params, pk)
  defp rows(%Query{limit: limit} = query, params, pk), do: offset_limit(limit, query, params, pk)

  defp filter(%Query{wheres: [%Query.QueryExpr{expr: {:==, _, [{{:., _, [{:&, _, [0]}, pk]}, _, []},
                                                               right]}}]} = query, params, {_coll, _model, pk}) do
    {:fetch, value(right, params, pk, query, "where clause")}
  end

  defp filter(%Query{wheres: wheres} = query, params, {_coll, model, pk}) do
    search =
      wheres
      |> Enum.map(fn %Query.QueryExpr{expr: expr} ->
        pair(expr, params, model, pk, query, "where clause")
      end)
      |> Enum.intersperse([" AND "])
      |> IO.iodata_to_binary

    {:search, search}
  end

  defp query(filter, pk) do
    filter |> value(pk, "where clause") |> map_unless_empty
  end

  defp order(%Query{order_bys: order_bys} = query, {_coll, model, pk}) do
    order_bys
    |> Enum.flat_map(fn %Query.QueryExpr{expr: expr} ->
      Enum.map(expr, &order_by_expr(&1, model, pk, query))
    end)
    |> Enum.intersperse([","])
    |> IO.iodata_to_binary
  end

  defp command(:insert, document, struct, pk) do
    document
    |> Enum.reject(fn {key, value} -> both_nil(value, Map.get(struct, key)) end)
    |> value(pk, "insert command") |> map_unless_empty
  end

  defp command(:update, values, pk) do
    [set: values |> value(pk, "update command") |> map_unless_empty]
  end

  defp both_nil(nil, nil), do: true
  defp both_nil( %Ecto.Query.Tagged{tag: nil, value: nil}, nil), do: true
  defp both_nil([], []), do: true
  defp both_nil(false, _), do: true
  defp both_nil(_, _), do: false

  defp offset_limit(nil, _, _, _),
    do: nil
  defp offset_limit(%Query.QueryExpr{expr: int}, _query, _params, _pk) when is_integer(int),
    do: int
  defp offset_limit(%Query.QueryExpr{expr: int}, _query, _params, _pk) when is_integer(int),
    do: int
  defp offset_limit(%Query.QueryExpr{expr: expr},  query, params, pk) do
    value(expr, params, pk, query, "offset/limit clause") |> String.to_integer
  end

  defp primary_key(nil),
    do: nil
  defp primary_key(model) do
    case model.__schema__(:primary_key) do
      []   -> nil
      [pk] -> pk
      keys ->
        raise ArgumentError, "Riak adapter does not support multiple primary keys " <>
          "and #{inspect keys} were defined in #{inspect model}."
    end
  end

  defp order_by_expr({:asc,  expr}, model, pk, query),
    do: [ field(expr, model, pk, query, "order clause"), " asc" ]
  defp order_by_expr({:desc, expr}, model, pk, query),
    do: [ field(expr, model, pk, query, "order clause"), " desc" ]

  defp check_query(query) do
    check(query.distinct, nil, query, "Riak adapter does not support distinct clauses")
    check(query.lock,     nil, query, "Riak adapter does not support locking")
    check(query.joins,     [], query, "Riak adapter does not support join clauses")
    check(query.group_bys, [], query, "Riak adapter does not support group_by clauses")
    check(query.havings,   [], query, "Riak adapter does not support having clauses")
  end

  defp check(expr, expr, _, _),
    do: nil
  defp check(_, _, query, message),
    do: raise(Ecto.QueryError, query: query, message: message)

  defp value(expr, pk, place) do
    case Encoder.encode(expr, pk) do
      {:ok, value} -> value
      :error       -> error(place)
    end
  end

  defp value(expr, params, pk, query, place) do
    case Encoder.encode(expr, params, pk) do
      {:ok, value} -> value
      :error       -> error(query, place)
    end
  end

  defp escaped_value(expr, params, pk, query, place),
    do: value(expr, params, pk, query, place) |> to_string |> escape_value

  defp field(pk, pk), do: :id
  defp field(key, _), do: key

  defp field(pk, _, pk), do: "_yz_rk"
  defp field(key, type, _), do: [Atom.to_string(key), '_', Atom.to_string(type)]

  defp field({{:., _, [{:&, _, [0]}, field]}, _, []}, model, pk, _query, _place) do
    type = model.__schema__(:type, field) |> riak_type
    field(field, type, pk)
  end

  defp field(_expr, _model, _pk, query, place),
    do: error(query, place)

  defp riak_type(:string),    do: :register
  defp riak_type(:integer),   do: :register
  defp riak_type(:float),     do: :register
  defp riak_type(:binary_id), do: :register
  defp riak_type(:id),        do: :register

  defp riak_type(:boolean),   do: :flag

  defp riak_type(_),          do: :register

  defp map_unless_empty([]),   do: %{}
  defp map_unless_empty(list), do: list

  {:ok, pattern} = :re.compile(~S"[:;~^\"!*+\-&\?()\][}{\\\|\s#]", [:unicode])
  @escape_pattern pattern

  defp escape_value(string) do
    :re.replace(string, @escape_pattern, "\\\\&", [:global, {:return, :binary}])
  end

  bool_ops = [and: "AND", or: "OR"]

  @bool_ops Keyword.keys(bool_ops)

  Enum.map(bool_ops, fn {op, riak_top} ->
    defp bool_op(unquote(op)), do: unquote(riak_top)
  end)

  defp mapped_pair_or_value({op, _, _} = tuple, params, model, pk, query, place) when is_op(op) do
    [pair(tuple, params, model, pk, query, place)]
  end
  defp mapped_pair_or_value(value, params, _model, pk, query, place) do
    escaped_value(value, params, pk, query, place)
  end

  defp pair({:==, _, [left, right]}, params, model, pk, query, place) do
    [field(left, model, pk, query, place), ':', to_string(value(right, params, pk, query, place))]
  end

  defp pair({op, _, [left, right]}, params, model, pk, query, place) when op in @bool_ops do
    left  = mapped_pair_or_value(left, params, model, pk, query, place)
    right = mapped_pair_or_value(right, params, model, pk, query, place)
    ["(", left, " ", bool_op(op), " ", right, ")"]
  end

  defp pair({:>=, _, [left, right]}, params, model, pk, query, place) do
    ["(",
     field(left, model, pk, query, place), ":", "[",
     escaped_value(right, params, pk, query, place), " TO *]", ")"]
  end

  defp pair({:>, _, [left, right]}, params, model, pk, query, place) do
    ["(", field(left, model, pk, query, place), ":", "{",
     escaped_value(right, params, pk, query, place), " TO *]", ")"]
  end

  defp pair({:<, _, [left, right]}, params, model, pk, query, place) do
    ["(", field(left, model, pk, query, place), ":", "[* TO ",
     escaped_value(right, params, pk, query, place), "}", ")"]
  end

  defp pair({:<=, _, [left, right]}, params, model, pk, query, place) do
    ["(", field(left, model, pk, query, place), ":", "[* TO ",
     escaped_value(right, params, pk, query, place), "]", ")"]
  end

  defp pair({:!=, _, [left, right]}, params, model, pk, query, place) do
    ["(", "-", "(", field(left, model, pk, query, place), ":",
     escaped_value(right, params, pk, query, place), ")", ")"]
  end

  #defp pair({:not, _, [expr]}, params, model, pk, query, place) do
  #  ["(", "*:* NOT (", pair(expr, params, pk, query, place), "))"]
  #end

  # embedded fragment
  defp pair({:fragment, _, args}, params, _model, pk, query, place) when is_list(args) do
    Enum.map(args, fn arg ->
      case arg do
        {:raw, raw}   -> raw
        {:expr, expr} -> escape_value(to_string(value(expr, params, pk, query, place)))
      end
    end)
  end

  defp pair(_expr, _params, _model, _pk, query, place) do
    error(query, place)
  end

  defp error(query, place) do
    raise Ecto.QueryError, query: query,
      message: "1) Invalid expression for Riak adapter in #{place}"
  end
  defp error(place) do
    raise ArgumentError, "2) Invalid expression for Riak adapter in #{place}"
  end

end
