defmodule Simple.App do
  use Application

  def start(_type, _args) do
    import Supervisor.Spec

    tree = [worker(Simple.Repo, [])]

    opts = [name: Simple.Sup, strategy: :one_for_one]
    Supervisor.start_link(tree, opts)
  end
end

defmodule Simple.Repo do
  use Ecto.Repo, otp_app: :simple
end

defmodule Weather do
  use Ecto.Model

  @primary_key {:id, :binary_id, autogenerate: true}

  schema "weather" do
    field :city, :string
    field :temp_lo, :integer
    field :temp_hi, :integer
    field :expiry_date, Ecto.Date
    field :active, :boolean
    field :prcp, :float, default: 0.0
    field :map, :map

    field :counter, Riak.Ecto.Counter
    field :set, Riak.Ecto.Set

    field :list, {:array, :integer}

    embeds_one :item, Item, on_replace: :delete
    embeds_many :items, Item

    timestamps
  end
end

defmodule Item do
  use Ecto.Model

  embedded_schema do
    field :name
    field :order, :integer
  end
end


defmodule Simple do
  import Ecto.Query

  def sample_query do
    query = from w in Weather,
         select: w
    Simple.Repo.all(query)
  end
end
