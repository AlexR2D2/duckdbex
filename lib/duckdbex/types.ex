defmodule Duckdbex.Interval do
  @moduledoc """
  A DuckDB `INTERVAL`.

  The three components are kept separate because DuckDB stores them that way;
  months cannot be converted to days or microseconds without a reference date.
  """

  @enforce_keys [:months, :days, :micros]
  defstruct [:months, :days, :micros]

  @type t :: %__MODULE__{
          months: integer(),
          days: integer(),
          micros: integer()
        }
end

defmodule Duckdbex.TimeTz do
  @moduledoc """
  A DuckDB `TIMETZ`: a wall-clock time plus a UTC offset in seconds.

  Elixir has no zoned time type, so the offset is carried next to a `Time`.
  """

  @enforce_keys [:time, :offset]
  defstruct [:time, :offset]

  @type t :: %__MODULE__{
          time: Time.t(),
          offset: integer()
        }
end

defmodule Duckdbex.TimestampNs do
  @moduledoc """
  A DuckDB `TIMESTAMP_NS`.

  The `nanosecond` field holds the sub-microsecond remainder (`0..999`), so
  nanosecond precision survives the trip through a microsecond-precision
  `NaiveDateTime`.
  """

  @enforce_keys [:datetime, :nanosecond]
  defstruct [:datetime, :nanosecond]

  @type t :: %__MODULE__{
          datetime: NaiveDateTime.t(),
          nanosecond: 0..999
        }
end

defmodule Duckdbex.TimestampTzNs do
  @moduledoc """
  A DuckDB `TIMESTAMPTZ_NS`, in UTC.

  The `nanosecond` field holds the sub-microsecond remainder (`0..999`).
  """

  @enforce_keys [:datetime, :nanosecond]
  defstruct [:datetime, :nanosecond]

  @type t :: %__MODULE__{
          datetime: DateTime.t(),
          nanosecond: 0..999
        }
end

defmodule Duckdbex.Hugeint do
  @moduledoc """
  A DuckDB `HUGEINT` / `UHUGEINT`, split into its two 64-bit halves.

  Erlang integers are arbitrary precision, but the NIF API cannot build a
  bignum, so the halves are returned as-is. Use `to_integer/1` to convert.
  """

  @enforce_keys [:upper, :lower]
  defstruct [:upper, :lower]

  @type t :: %__MODULE__{
          upper: integer(),
          lower: non_neg_integer()
        }

  @doc """
  Converts a `Duckdbex.Hugeint` to an Elixir integer.

      iex> Duckdbex.Hugeint.to_integer(%Duckdbex.Hugeint{upper: 0, lower: 1})
      1
  """
  @spec to_integer(t()) :: integer()
  def to_integer(%__MODULE__{upper: upper, lower: lower}) do
    Bitwise.bsl(upper, 64) + lower
  end

  @doc """
  Converts an Elixir integer to a `Duckdbex.Hugeint`.

      iex> Duckdbex.Hugeint.from_integer(1)
      %Duckdbex.Hugeint{upper: 0, lower: 1}
  """
  @spec from_integer(integer()) :: t()
  def from_integer(value) when is_integer(value) do
    lower = Bitwise.band(value, 0xFFFFFFFFFFFFFFFF)
    upper = Bitwise.bsr(value - lower, 64)
    %__MODULE__{upper: upper, lower: lower}
  end
end

defmodule Duckdbex.Decimal do
  @moduledoc """
  A DuckDB `DECIMAL`: an unscaled integer, the declared width and the scale.

  The value is `unscaled / 10^scale`. `unscaled` is a plain integer when it fits
  in 64 bits and a `Duckdbex.Hugeint` for wider decimals.

  Use `to_decimal/1` when the optional `decimal` package is available.
  """

  @enforce_keys [:unscaled, :width, :scale]
  defstruct [:unscaled, :width, :scale]

  @type unscaled :: integer() | Duckdbex.Hugeint.t()

  @type t :: %__MODULE__{
          unscaled: unscaled(),
          width: pos_integer(),
          scale: non_neg_integer()
        }

  @doc """
  Converts to a `Decimal` when the `decimal` package is loaded.

  Returns `{:error, :decimal_not_available}` otherwise.
  """
  @spec to_decimal(t()) :: {:ok, Decimal.t()} | {:error, :decimal_not_available}
  def to_decimal(%__MODULE__{unscaled: unscaled, scale: scale}) do
    if Code.ensure_loaded?(Decimal) do
      value =
        case unscaled do
          %Duckdbex.Hugeint{} -> Duckdbex.Hugeint.to_integer(unscaled)
          integer when is_integer(integer) -> integer
        end

      {:ok, apply(Decimal, :new, [1, value, -scale])}
    else
      {:error, :decimal_not_available}
    end
  end
end

defmodule Duckdbex.Union do
  @moduledoc """
  A DuckDB `UNION`: the tag of the active member and its value.
  """

  @enforce_keys [:tag, :value]
  defstruct [:tag, :value]

  @type t :: %__MODULE__{
          tag: String.t(),
          value: term()
        }
end
