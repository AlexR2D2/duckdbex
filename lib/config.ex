defmodule Duckdbex.Config do
  @moduledoc """
  DuckDB database instance configuration.
  """

  defstruct [
    # Access mode of the database
    access_mode: :automatic,

    # Checkpoint when WAL reaches this size (default: 16MB)
    checkpoint_wal_size: 16777216,

    # Whether or not to use Direct IO, bypassing operating system buffers
    use_direct_io: false,

    # Whether extensions should be loaded on start-up
    load_extensions: true,

    # The maximum memory used by the database system (in bytes). Default: 80% of System available memory
    maximum_memory: nil,

    # The maximum amount of native CPU threads used by the database system. Default: all available.
    maximum_threads: nil,

    # Whether or not to create and use a temporary directory to store intermediates that do not fit in memory
    use_temporary_directory: true,

    # Directory to store temporary structures that do not fit in memory. Default, current
    temporary_directory: nil,

    # The collation type of the database
    collation: nil,

    # The order type used when none is specified
    default_order_type: :asc,

    # Null ordering used when none is specified (default: NULLS LAST)
    default_null_order: :nulls_last,

    # Enable COPY and related commands
    enable_external_access: true,

    # Whether or not object cache is used
    object_cache_enable: false,

    # Whether or not the global http metadata cache is used
    http_metadata_cache_enable: false,

    # Force checkpoint when CHECKPOINT is called or on shutdown, even if no changes have been made
    force_checkpoint: false,

    # Run a checkpoint on successful shutdown and delete the WAL, to leave only a single database file behind
    checkpoint_on_shutdown: true,

    # Force a specific compression method to be used when checkpointing (if available)
    force_compression: :auto,

    # Force a specific bitpacking mode to be used when using the bitpacking compression method
    force_bitpacking_mode: :auto,

    # Whether or not preserving insertion order should be preserved
    preserve_insertion_order: true,

    # Directory to store extension binaries in
    extension_directory: nil,

    # Whether unsigned extensions should be loaded
    allow_unsigned_extensions: false,

    # Start transactions immediately in all attached databases - instead of lazily when a database is referenced
    immediate_transaction_mode: false,

    # The memory allocator used by the database system
    #  :duckdb - native DuckDB allocator,
    #  :erlang - erlang 'void *enif_alloc(size_t size)' allocator
    memory_allocator: :duckdb
  ]

  @typedoc """
  DuckDB database instance configuration type.

  `:access_mode`: Access mode of the database. Maybe `:automatic`, `:read_only` or `:read_write`. Default: `:automatic`.

  `:checkpoint_wal_size`: Checkpoint when WAL reaches this size. Default: `16777216` (16MB).

  `:use_direct_io`: Whether or not to use Direct IO, bypassing operating system buffers. Default: `false`.

  `:load_extensions`: Whether extensions should be loaded on start-up. Default: `true`.

  `:maximum_memory`: The maximum memory used by the database system (in bytes). Default: `nil` (80% of System available memory)

  `:maximum_threads`: The maximum amount of native CPU threads used by the database system. Default: `nil` (all available).

  `:use_temporary_directory`: Whether or not to create and use a temporary directory to store intermediates that do not fit in memory. Default: `true`

  `:temporary_directory`: Directory to store temporary structures that do not fit in memory. Default: `nil` (current)

  `:collation`: The collation type of the database. Default: `nil`

  `:default_order_type`: The order type used when none is specified. Maybe `:asc`, `:desc`. Deafult: `:asc`.

  `:default_null_order`: Null ordering used when none is specified. Maybe `:nulls_first`, `:nulls_last`. Default: `:nulls_last`.

  `:enable_external_access`: Enable COPY and related commands. Default: `true`.

  `:object_cache_enable`: Whether or not object cache is used. Default: `false`.

  `:http_metadata_cache_enable`: Whether or not the global http metadata cache is used. Default: `false`.

  `:force_checkpoint`: Force checkpoint when CHECKPOINT is called or on shutdown, even if no changes have been made. Default: `false`.

  `:checkpoint_on_shutdown`: Run a checkpoint on successful shutdown and delete the WAL, to leave only a single database file behind. Default: `true`.

  `:force_compression`: Force a specific compression method to be used when checkpointing (if available). Maybe `:auto`, `:uncompressed`, `:constant`, `:rle`, `:dictionary`, `:pfor_delta`, `:bitpacking`, `:fsst`, `:chimp`, `:patas`. Default: `:auto`.

  `:force_bitpacking_mode`: Force a specific bitpacking mode to be used when using the bitpacking compression method. Maybe `:auto`, `:constant`, `:constant_delta`, `:delta_for`, `:for`. Default: `:auto`.

  `:preserve_insertion_order`: Whether or not preserving insertion order should be preserved. Default: `true`.

  `:extension_directory`: Directory to store extension binaries in. Default: `nil`.

  `:allow_unsigned_extensions`: Whether unsigned extensions should be loaded. Default: `false`.

  `:immediate_transaction_mode`: Start transactions immediately in all attached databases - instead of lazily when a database is referenced. Default: `false`.

  `:memory_allocator`: The memory allocator used by the database system. Maybe `:duckdb` - native DuckDB allocator or `:erlang` - erlang 'void *enif_alloc(size_t size)' allocator. Default: `:duckdb`.
  """
  @type t :: %__MODULE__{
    access_mode: :automatic | :read_only | :read_write,
    checkpoint_wal_size: pos_integer(),
    use_direct_io: boolean(),
    load_extensions: boolean(),
    maximum_memory: pos_integer(),
    maximum_threads: pos_integer(),
    use_temporary_directory: boolean(),
    temporary_directory: binary() | nil,
    collation: binary() | nil,
    default_order_type: :asc | :desc,
    default_null_order: :nulls_last | :nulls_first,
    enable_external_access: boolean(),
    object_cache_enable: boolean(),
    http_metadata_cache_enable: boolean(),
    force_checkpoint: boolean(),
    checkpoint_on_shutdown: boolean(),
    force_compression: :auto | :uncompressed | :constant | :rle |
                       :dictionary | :pfor_delta | :bitpacking | :fsst | :chimp | :patas,
    force_bitpacking_mode: :auto | :constant | :constant_delta | :delta_for | :for,
    preserve_insertion_order: boolean(),
    extension_directory: binary() | nil,
    allow_unsigned_extensions: boolean(),
    immediate_transaction_mode: boolean(),
    memory_allocator: :duckdb | :erlang
  }
end
