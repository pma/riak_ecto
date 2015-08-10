ExUnit.start
Logger.configure(level: :info)

defmodule Simple.Case do
  use ExUnit.CaseTemplate

  setup do
    :ok
  end
end
