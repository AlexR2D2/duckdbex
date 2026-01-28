defmodule Duckdbex.Config do
  @moduledoc """
  DuckDB database instance configuration.
  """

  @type optimizer_type() ::
          :invalid
          | :expression_rewriter
          | :filter_pullup
          | :filter_pushdown
          | :cte_filter_pusher
          | :regex_range
          | :in_clause
          | :join_order
          | :deliminator
          | :unnest_rewriter
          | :unused_columns
          | :statistics_propagation
          | :common_subexpressions
          | :common_aggregate
          | :column_lifetime
          | :build_side_probe_side
          | :limit_pushdown
          | :top_n
          | :compressed_materialization
          | :duplicate_groups
          | :reorder_filter
          | :join_filter_pushdown
          | :extension
          | :materialized_cte

  @type compressionType() ::
          :compression_auto
          | :compression_uncompressed
          # internal only
          | :compression_constant
          | :compression_rle
          | :compression_dictionary
          | :compression_prof_delta
          | :compression_bitpacking
          | :compression_fsst
          | :compression_chimp
          | :compression_patas
          | :compression_alp
          | :compression_alprd
          | :compression_zstd
          | :compression_roaring
          #  internal only
          | :compression_empty
          # This has to stay the last entry of the type!
          | :compression_count

  defstruct [
    # Database file path. May be empty for in-memory mode
    database_path: nil,

    # Database type. If empty, automatically extracted from `database_path`, where a `type:path` syntax is expected
    database_type: nil,

    # Access mode of the database
    access_mode: :automatic,

    # Checkpoint when WAL reaches this size (default: 16MB) In bytes.
    checkpoint_wal_size: nil,

    # Whether or not to use Direct IO, bypassing operating system buffers
    use_direct_io: false,

    # Whether extensions should be loaded on start-up
    load_extensions: true,

    # Whether known extensions are allowed to be automatically loaded when a query depends on them
    autoload_known_extensions: true,

    # Whether known extensions are allowed to be automatically installed when a query depends on them
    autoinstall_known_extensions: true,

    # Override for the default extension repository
    custom_extension_repo: nil,

    # Override for the default autoload extension repository
    autoinstall_extension_repo: nil,

    # The maximum memory used by the database system (in bytes). Default: 80% of System available memory
    maximum_memory: nil,

    # The maximum size of the `temp_directory` folder when set (in bytes). Default: 90% of available disk space.
    maximum_swap_space: nil,

    # The maximum amount of native CPU threads used by the database system. Default: all available.
    maximum_threads: nil,

    # The number of external threads that work on DuckDB tasks.
    # Must be smaller or equal to maximum_threads.
    external_threads: 1,

    # Whether or not to create and use a temporary directory to store intermediates that do not fit in memory
    use_temporary_directory: true,

    # Directory to store temporary structures that do not fit in memory. Default, current
    temporary_directory: System.get_env("DUCKDBEX_TEMPORARY_DIRECTORY"),

    # Whether or not to invoke filesystem trim on free blocks after checkpoint. This will reclaim
    # space for sparse files, on platforms that support it.
    trim_free_blocks: false,

    # Record timestamps of buffer manager unpin() events. Usable by custom eviction policies.
    buffer_manager_track_eviction_timestamps: false,

    # Whether or not to allow printing unredacted secrets
    allow_unredacted_secrets: false,

    # Disables invalidating the database instance when encountering a fatal error.
    disable_database_invalidation: false,

    # Enable COPY and related commands
    enable_external_access: true,

    # Whether or not the global http metadata cache is used
    http_metadata_cache_enable: false,

    # HTTP Proxy config as `hostname:port`
    http_proxy: nil,

    # HTTP Proxy username for basic auth
    http_proxy_username: nil,

    # HTTP Proxy password for basic auth
    http_proxy_password: nil,

    # Force checkpoint when CHECKPOINT is called or on shutdown, even if no changes have been made
    force_checkpoint: false,

    # Run a checkpoint on successful shutdown and delete the WAL, to leave only a single database file behind
    checkpoint_on_shutdown: true,

    # Serialize the metadata on checkpoint with compatibility for a given DuckDB version.
    serialization_compatibility: nil,

    # Initialize the database with the standard set of DuckDB functions
    # You should probably not touch this unless you know what you are doing
    initialize_default_database: true,

    # The set(elixir list) of disabled optimizers (default empty)
    disabled_optimizers: nil,

    # The average string length required to use ZSTD compression. Default: 4096
    zstd_min_string_length: nil,

    # Force a specific compression method to be used when checkpointing (if available)
    force_compression: nil,

    # The set of disabled compression methods (default empty)
    disabled_compression_methods: nil,

    # Force a specific bitpacking mode to be used when using the bitpacking compression method
    force_bitpacking_mode: :auto,

    # Directory to store extension binaries in
    extension_directory: System.get_env("DUCKDBEX_EXTENSION_DIRECTORY"),

    # Whether unsigned extensions should be loaded
    allow_unsigned_extensions: false,

    # Whether community extensions should be loaded
    allow_community_extensions: true,

    # The set of user-provided options
    user_options: nil,

    # The set of unrecognized (other) options
    unrecognized_options: nil,

    # Whether or not the configuration settings can be altered
    lock_configuration: false,

    # The peak allocation threshold at which to flush the allocator after completing a task (1 << 27, ~128MB). In bytes.
    allocator_flush_threshold: nil,

    # If bulk deallocation larger than this occurs, flush outstanding allocations (1 << 30, ~1GB)
    allocator_bulk_deallocation_flush_threshold: nil,

    # Whether the allocator background thread is enabled
    allocator_background_threads: false,

    # DuckDB API surface
    duckdb_api: nil,

    # Metadata from DuckDB callers
    custom_user_agent: nil,

    # Encrypt the temp files
    temp_file_encryption: false,

    # TODO : DEPRECATED?
    # Use old implicit casting style (i.e. allow everything to be implicitly casted to VARCHAR)
    # old_implicit_casting = false;

    # The default block allocation size for new duckdb database files (new as-in, they do not yet exist). In bytes.
    default_block_alloc_size: nil,

    # The default block header size for new duckdb database files. In bytes.
    default_block_header_size: nil,

    # Whether or not to abort if a serialization exception is thrown during WAL playback (when reading truncated WAL)
    abort_on_wal_failure: false,

    # Paths that are explicitly allowed, even if enable_external_access is false
    allowed_paths: nil,

    # Directories that are explicitly allowed, even if enable_external_access is false
    allowed_directories: nil,

    # TODO: The log configuration
    # LogConfig log_config = LogConfig();

    # TODO: Whether to enable external file caching using CachingFileSystem
    # enable_external_file_cache: true,

    # TODO: Partially process tasks before rescheduling - allows for more scheduler fairness between separate queries
    # scheduler_process_partial: false,

    # TODO: Whether to pin threads to cores (linux only, default AUTOMATIC: on when there are more than 64 cores)
    # ThreadPinMode pin_threads = ThreadPinMode::AUTO;

    # The memory allocator used by the database system
    #  :duckdb - native DuckDB allocator,
    #  :erlang - erlang `void *enif_alloc(size_t size)` allocator
    memory_allocator: :duckdb
  ]

  @typedoc """
  DuckDB database instance configuration type.

  `:database_path`: Database file path. May be empty for in-memory mode. Default: `nil`.

  `:database_type`: Database type. If empty, automatically extracted from `database_path`, where a `type:path` syntax is expected. Default: `nil`.

  `:access_mode`: Access mode of the database. Maybe `:automatic`, `:read_only` or `:read_write`. Default: `:automatic`.

  `:checkpoint_wal_size`: Checkpoint when WAL reaches this size. Default: `nil` (16777216 ~16MB). In bytes.

  `:use_direct_io`: Whether or not to use Direct IO, bypassing operating system buffers. Default: `false`.

  `:load_extensions`: Whether extensions should be loaded on start-up. Default: `true`.

  `:autoload_known_extensions`: Whether known extensions are allowed to be automatically loaded when a query depends on them. Default: `true`.

  `:autoinstall_known_extensions`: Whether known extensions are allowed to be automatically installed when a query depends on them. , Default: `true`.

  `:custom_extension_repo`: Override for the default extension repository. Default: `nil`.

  `:autoinstall_extension_repo`: Override for the default autoload extension repository. Default: `nil`.

  `:maximum_memory`: The maximum memory used by the database system (in bytes). Default: `nil` (80% of System available memory)

  `:maximum_swap_space`: The maximum size of the `temp_directory` folder when set (in bytes). Default: `nil` (90% of available disk space).

  `:maximum_threads`: The maximum amount of native CPU threads used by the database system. Default: `nil` (all available).

  `:external_threads`: The number of external threads that work on DuckDB tasks. Must be smaller or equal to maximum_threads. Default: `1`.

  `:use_temporary_directory`: Whether or not to create and use a temporary directory to store intermediates that do not fit in memory. Default: `true`

  `:temporary_directory`: Directory to store temporary structures that do not fit in memory. Default: `nil` (current)

  `:trim_free_blocks`: Whether or not to invoke filesystem trim on free blocks after checkpoint. This will reclaim space for sparse files, on platforms that support it. Default: `false`

  `:buffer_manager_track_eviction_timestamps`: Record timestamps of buffer manager unpin() events. Usable by custom eviction policies. Default `false`.

  `:allow_unredacted_secrets`: Whether or not to allow printing unredacted secrets. Default: `false`.

  `:disable_database_invalidation`: Disables invalidating the database instance when encountering a fatal error. Default: `false`.

  `:enable_external_access`: Enable COPY and related commands. Default: `true`.

  `:http_metadata_cache_enable`: Whether or not the global http metadata cache is used. Default: `false`.

  `:http_proxy`: HTTP Proxy config as `hostname:port`. Default: `nil`.

  `:http_proxy_username`: HTTP Proxy username for basic auth. Default: `nil`.

  `:http_proxy_password`: HTTP Proxy password for basic auth. Default: `nil`.

  `:force_checkpoint`: Force checkpoint when CHECKPOINT is called or on shutdown, even if no changes have been made. Default: `false`.

  `:checkpoint_on_shutdown`: Run a checkpoint on successful shutdown and delete the WAL, to leave only a single database file behind. Default: `true`.

  `:serialization_compatibility`: Serialize the metadata on checkpoint with compatibility for a given DuckDB version. Default: `nil` (latest version of DuckDB).

  `:initialize_default_database`: Initialize the database with the standard set of DuckDB functions. You should probably not touch this unless you know what you are doing. Default: `true`.

  `:disabled_optimizers`: The set(elixir list) of disabled optimizers. Default: `nil`.

  `:zstd_min_string_length`: The average string length required to use ZSTD compression. Default: 4096.

  `:force_compression`: Force a specific compression method to be used when checkpointing (if available). Maybe `:auto`, `:uncompressed`, `:constant`, `:rle`, `:dictionary`, `:pfor_delta`, `:bitpacking`, `:fsst`, `:chimp`, `:patas`. Default: `:auto`.

  `:disabled_compression_methods`: The set of disabled compression methods. Default `nil`.

  `:force_bitpacking_mode`: Force a specific bitpacking mode to be used when using the bitpacking compression method. Maybe `:auto`, `:constant`, `:constant_delta`, `:delta_for`, `:for`. Default: `:auto`.

  `:extension_directory`: Directory to store extension binaries in. Default: `nil`.

  `:allow_unsigned_extensions`: Whether unsigned extensions should be loaded. Default: `false`.

  `:allow_community_extensions`: Whether community extensions should be loaded. Default: `true`.

  `:user_options`: Default: The set of user-provided options. `nil`.

  `:unrecognized_options`: The set of unrecognized (other) options. Default: `nil`.

  `:lock_configuration`: Whether or not the configuration settings can be altered. Default: `false`.

  `:allocator_flush_threshold`: The peak allocation threshold at which to flush the allocator after completing a task (1 << 27, ~128MB). In bytes. Default: `nil` (134217728 bytes);

  `:allocator_bulk_deallocation_flush_threshold`: If bulk deallocation larger than this occurs, flush outstanding allocations (1 << 30, ~1GB). Default: `nil` (536870912 in bytes)

  `:allocator_background_threads`:  Whether the allocator background thread is enabled. Default: `false`.

  `:duckdb_api`: DuckDB API surface. Default: `nil`.

  `:custom_user_agent`: Metadata from DuckDB callers. Default: `nil`.

  `:temp_file_encryption`: Encrypt the temp files. Default: `false`.

  `:default_block_alloc_size`: The default block allocation size for new duckdb database files (new as-in, they do not yet exist). In bytes. Default: `nil` (DUCKDB_BLOCK_ALLOC_SIZE = 262144 ULL)

  `:default_block_header_size`: The default block header size for new duckdb database files.. In bytes. Default: `nil` (DUCKDB_BLOCK_HEADER_STORAGE_SIZE = 8ULL ULL)

  `:abort_on_wal_failure`: Whether or not to abort if a serialization exception is thrown during WAL playback (when reading truncated WAL). Default: `false`.

  `:allowed_paths`: Paths that are explicitly allowed, even if enable_external_access is false. Default: `nil`.

  `:allowed_directories`: Directories that are explicitly allowed, even if enable_external_access is false. Default: `nil`.

  `:memory_allocator`: The memory allocator used by the database system. Maybe `:duckdb` - native DuckDB allocator or `:erlang` - erlang `void *enif_alloc(size_t size)` allocator. Default: `:duckdb`.
  """
  @type t :: %__MODULE__{
          database_path: binary() | nil,
          database_type: binary() | nil,
          access_mode: :automatic | :read_only | :read_write,
          checkpoint_wal_size: pos_integer() | nil,
          use_direct_io: boolean(),
          load_extensions: boolean(),
          autoload_known_extensions: boolean(),
          autoinstall_known_extensions: boolean(),
          custom_extension_repo: binary() | nil,
          autoinstall_extension_repo: binary() | nil,
          maximum_memory: pos_integer() | nil,
          maximum_swap_space: pos_integer() | nil,
          maximum_threads: pos_integer() | nil,
          external_threads: pos_integer(),
          use_temporary_directory: boolean(),
          temporary_directory: binary() | nil,
          trim_free_blocks: boolean(),
          buffer_manager_track_eviction_timestamps: boolean(),
          allow_unredacted_secrets: boolean(),
          disable_database_invalidation: boolean(),
          enable_external_access: boolean(),
          http_metadata_cache_enable: boolean(),
          http_proxy: binary() | nil,
          http_proxy_username: binary() | nil,
          http_proxy_password: binary() | nil,
          force_checkpoint: boolean(),
          checkpoint_on_shutdown: boolean(),
          serialization_compatibility: binary() | nil,
          initialize_default_database: boolean(),
          disabled_optimizers: list(Duckdbex.Config.optimizer_type()) | [] | nil,
          zstd_min_string_length: pos_integer() | nil,
          force_compression: Duckdbex.Config.compressionType() | nil,
          disabled_compression_methods: list(Duckdbex.Config.compressionType()) | [] | nil,
          force_bitpacking_mode: :auto | :constant | :constant_delta | :delta_for | :for,
          extension_directory: binary() | nil,
          allow_unsigned_extensions: boolean(),
          allow_community_extensions: boolean(),
          user_options: list({binary(), any()}) | nil,
          unrecognized_options: list({binary(), any()}) | nil,
          lock_configuration: boolean(),
          allocator_flush_threshold: pos_integer() | nil,
          allocator_bulk_deallocation_flush_threshold: pos_integer() | nil,
          allocator_background_threads: boolean(),
          duckdb_api: binary() | nil,
          custom_user_agent: binary() | nil,
          temp_file_encryption: boolean(),
          default_block_alloc_size: pos_integer() | nil,
          default_block_header_size: pos_integer() | nil,
          abort_on_wal_failure: boolean(),
          allowed_paths: list(binary()) | nil,
          allowed_directories: list(binary()) | nil,
          memory_allocator: :duckdb | :erlang
        }
end
