defmodule Riak.Ecto.NormalizedQuery do
  @moduledoc false

  defmodule SearchQuery do
    @moduledoc false

    defstruct prefix: nil, source: nil, pk: nil, params: nil,
              struct: nil,
              filter_query: nil, normal_query: nil,
              projection: nil, fields: nil, order: nil,
              opts: nil
  end

  defmodule FetchQuery do
    @moduledoc false

    defstruct prefix: nil, source: nil, pk: nil, id: nil,
              struct: nil,
              projection: nil, fields: [], opts: []
  end

  alias Riak.Ecto.Encoder
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

    case projection(original) do
      {projection, fields} ->
        case filter(original, params, from) do
          {:fetch, id} ->
            find_one(original, id, projection, fields, params, from)
          {:search, filter} ->
            order = order(original, from)
            find_all(original, "*:*", filter, order, projection, fields, params, from)
        end
    end

  end

  defp find_all(original, query, filter, order, projection, fields, params, {source, struct, pk}) do
    opts = opts(:find_all, original, params, pk)

    %SearchQuery{prefix: original.prefix, source: source, pk: pk, params: params,
                 struct: struct,
                 normal_query: query, filter_query: filter, projection: projection,
                 fields: fields, order: order, opts: opts}
  end

  defp find_one(original, id, projection, fields, params, {source, struct, pk}) do
    opts = opts(:find_one, original, params, pk)

    %FetchQuery{prefix: original.prefix, source: source,
                struct: struct,
                pk: pk, projection: projection, fields: fields, id: id, opts: opts}
  end

  defp from(%Query{from: {coll, model}}) do
    {coll, model, primary_key(model)}
  end

  defp projection(%Query{select: %Query.SelectExpr{fields: fields}} = query) do
    projection(fields, query, fields, [])
  end

  defp projection([], _query, projection, facc) do
    {projection, facc |> Enum.reverse |> List.flatten}
  end

  defp projection([{:&, _, [_ix, fields, _]} | rest], query, projection, facc) do
    projection(rest, query, projection, [fields | facc])
  end

  defp projection([{{:., _, [{:&, _, [_]}, field]}, _, []} | rest], query, projection, facc) do
    projection(rest, query, projection, [field | facc])
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

  defp filter(%Query{wheres: [%Query.QueryExpr{expr: {:in, _, [{{:., _, [{:&, _, [0|_]}, pk]}, _, []},
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

  defp order(%Query{order_bys: order_bys} = query, {_coll, model, pk}) do
    order_bys
    |> Enum.flat_map(fn %Query.QueryExpr{expr: expr} ->
      Enum.map(expr, &order_by_expr(&1, model, pk, query))
    end)
    |> Enum.intersperse([","])
    |> IO.iodata_to_binary
  end

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

  defp value(expr, params, pk, query, place) do
    case Encoder.encode(expr, params, pk) do
      {:ok, value} -> value
      :error       -> error(query, place)
    end
  end

  defp escaped_value(expr, params, pk, query, place),
    do: value(expr, params, pk, query, place) |> to_string |> escape_value

  defp field(_expr, _model, _pk, query, place),
    do: error(query, place)

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
    ["(", "*:* NOT ", "(", field(left, model, pk, query, place), ":",
     escaped_value(right, params, pk, query, place), ")", ")"]
  end

  defp pair({:not, _, [expr]}, params, model, pk, query, place) do
    ["(", "*:* NOT (", pair(expr, params, model, pk, query, place), "))"]
  end

  defp pair({:in, _, [left, right]}, params, model, pk, query, place) do
    [field(left, model, pk, query, place), ':', to_string(value(right, params, pk, query, place))]
  end

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

end
