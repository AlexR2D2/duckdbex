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

  defstruct [
    # Access mode of the database
    access_mode: :automatic,

    # Checkpoint when WAL reaches this size (default: 16MB) In bytes.
    checkpoint_wal_size: nil,

    # Whether or not to use Direct IO, bypassing operating system buffers
    use_direct_io: false,

    # Whether extensions should be loaded on start-up
    load_extensions: true,

    # Whether known extensions are allowed to be automatically loaded when a query depends on them
    autoload_known_extensions: false,

    # Whether known extensions are allowed to be automatically installed when a query depends on them
    autoinstall_known_extensions: false,

    # Override for the default extension repository
    custom_extension_repo: nil,

    # Override for the default autoload extension repository
    autoinstall_extension_repo: nil,

    # The maximum memory used by the database system (in bytes). Default: 80% of System available memory
    maximum_memory: nil,

    # The maximum size of the 'temp_directory' folder when set (in bytes). Default: 90% of available disk space.
    maximum_swap_space: nil,

    # The maximum amount of native CPU threads used by the database system. Default: all available.
    maximum_threads: nil,

    # The number of external threads that work on DuckDB tasks.
    # Must be smaller or equal to maximum_threads.
    external_threads: 1,

    # Whether or not to create and use a temporary directory to store intermediates that do not fit in memory
    use_temporary_directory: true,

    # Directory to store temporary structures that do not fit in memory. Default, current
    temporary_directory: nil,

    # Whether or not to invoke filesystem trim on free blocks after checkpoint. This will reclaim
    # space for sparse files, on platforms that support it.
    trim_free_blocks: false,

    # Record timestamps of buffer manager unpin() events. Usable by custom eviction policies.
    buffer_manager_track_eviction_timestamps: false,

    # Whether or not to allow printing unredacted secrets
    allow_unredacted_secrets: false,

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

    # HTTP Proxy config as 'hostname:port'
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

    # The set(elixir list) of disabled optimizers (default empty)
    disabled_optimizers: nil,

    # Force a specific compression method to be used when checkpointing (if available)
    force_compression: :auto,

    # Force a specific bitpacking mode to be used when using the bitpacking compression method
    force_bitpacking_mode: :auto,

    # Whether or not preserving insertion order should be preserved
    preserve_insertion_order: true,

    # Whether Arrow Arrays use Large or Regular buffers
    arrow_offset_size: :regular,

    # Whether LISTs should produce Arrow ListViews
    arrow_use_list_view: false,

    # Whenever a DuckDB type does not have a clear native or canonical extension match in Arrow, export the types
    # with a duckdb.type_name extension name
    arrow_arrow_lossless_conversion: false,

    # Whether when producing arrow objects we produce string_views or regular strings
    produce_arrow_string_views: false,

    # Directory to store extension binaries in
    extension_directory: nil,

    # Whether unsigned extensions should be loaded
    allow_unsigned_extensions: false,

    # Whether community extensions should be loaded
    allow_community_extensions: true,

    # Whether extensions with missing metadata should be loaded
    allow_extensions_metadata_mismatch: false,

    # Enable emitting FSST Vectors
    enable_fsst_vectors: false,

    # Enable VIEWs to create dependencies
    enable_view_dependencies: false,

    # Enable macros to create dependencies
    enable_macro_dependencies: false,

    # Start transactions immediately in all attached databases - instead of lazily when a database is referenced
    immediate_transaction_mode: false,

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

    # TODO : DEPRECATED?
    # Use old implicit casting style (i.e. allow everything to be implicitly casted to VARCHAR)
    # old_implicit_casting = false;

    # The default block allocation size for new duckdb database files (new as-in, they do not yet exist). In bytes.
    default_block_alloc_size: nil,

    # Whether or not to abort if a serialization exception is thrown during WAL playback (when reading truncated WAL)
    abort_on_wal_failure: false,

    # The index_scan_percentage sets a threshold for index scans.
    # If fewer than MAX(index_scan_max_count, index_scan_percentage * total_row_count)
    # rows match, we perform an index scan instead of a table scan.
    index_scan_percentage: nil,

    # The index_scan_max_count sets a threshold for index scans.
    # If fewer than MAX(index_scan_max_count, index_scan_percentage * total_row_count)
    # rows match, we perform an index scan instead of a table scan.
    index_scan_max_count: nil,

    # The maximum number of schemas we will look through for "did you mean..." style errors in the catalog
    catalog_error_max_schemas: nil,

    # The maximum amount of vacuum tasks to schedule during a checkpoint
    max_vacuum_tasks: nil,

    # The memory allocator used by the database system
    #  :duckdb - native DuckDB allocator,
    #  :erlang - erlang 'void *enif_alloc(size_t size)' allocator
    memory_allocator: :duckdb
  ]

  @typedoc """
  DuckDB database instance configuration type.

  `:access_mode`: Access mode of the database. Maybe `:automatic`, `:read_only` or `:read_write`. Default: `:automatic`.

  `:checkpoint_wal_size`: Checkpoint when WAL reaches this size. Default: `nil` (16777216 ~16MB). In bytes.

  `:use_direct_io`: Whether or not to use Direct IO, bypassing operating system buffers. Default: `false`.

  `:load_extensions`: Whether extensions should be loaded on start-up. Default: `true`.

  `:autoload_known_extensions`: Whether known extensions are allowed to be automatically loaded when a query depends on them. Default: `false`.

  `:autoinstall_known_extensions`: Whether known extensions are allowed to be automatically installed when a query depends on them. , Default: `false`.

  `:custom_extension_repo`: Override for the default extension repository. Default: 'nil'.

  `:autoinstall_extension_repo`: Override for the default autoload extension repository. Default: 'nil'.

  `:maximum_memory`: The maximum memory used by the database system (in bytes). Default: `nil` (80% of System available memory)

  `:maximum_swap_space`: The maximum size of the 'temp_directory' folder when set (in bytes). Default: nil (90% of available disk space).

  `:maximum_threads`: The maximum amount of native CPU threads used by the database system. Default: `nil` (all available).

  `:external_threads`: The number of external threads that work on DuckDB tasks. Must be smaller or equal to maximum_threads. Default: `1`.

  `:use_temporary_directory`: Whether or not to create and use a temporary directory to store intermediates that do not fit in memory. Default: `true`

  `:temporary_directory`: Directory to store temporary structures that do not fit in memory. Default: `nil` (current)

  `:trim_free_blocks`: Whether or not to invoke filesystem trim on free blocks after checkpoint. This will reclaim space for sparse files, on platforms that support it. Default: `false`

  `:buffer_manager_track_eviction_timestamps`: Record timestamps of buffer manager unpin() events. Usable by custom eviction policies. Default `false`.

  `:allow_unredacted_secrets`: Whether or not to allow printing unredacted secrets. Default: `false`.

  `:collation`: The collation type of the database. Default: `nil`

  `:default_order_type`: The order type used when none is specified. Maybe `:asc`, `:desc`. Deafult: `:asc`.

  `:default_null_order`: Null ordering used when none is specified. Maybe `:nulls_first`, `:nulls_last`. Default: `:nulls_last`.

  `:enable_external_access`: Enable COPY and related commands. Default: `true`.

  `:object_cache_enable`: Whether or not object cache is used. Default: `false`.

  `:http_metadata_cache_enable`: Whether or not the global http metadata cache is used. Default: `false`.

  `:http_proxy`: HTTP Proxy config as 'hostname:port'. Default: `nil`.

  `:http_proxy_username`: HTTP Proxy username for basic auth. Default: `nil`.

  `:http_proxy_password`: HTTP Proxy password for basic auth. Default: `nil`.

  `:force_checkpoint`: Force checkpoint when CHECKPOINT is called or on shutdown, even if no changes have been made. Default: `false`.

  `:checkpoint_on_shutdown`: Run a checkpoint on successful shutdown and delete the WAL, to leave only a single database file behind. Default: `true`.

  `:serialization_compatibility`: Serialize the metadata on checkpoint with compatibility for a given DuckDB version. Default: `nil` (latest version of DuckDB).

  `:disabled_optimizers`: The set(elixir list) of disabled optimizers. Default: `nil`.

  `:force_compression`: Force a specific compression method to be used when checkpointing (if available). Maybe `:auto`, `:uncompressed`, `:constant`, `:rle`, `:dictionary`, `:pfor_delta`, `:bitpacking`, `:fsst`, `:chimp`, `:patas`. Default: `:auto`.

  `:force_bitpacking_mode`: Force a specific bitpacking mode to be used when using the bitpacking compression method. Maybe `:auto`, `:constant`, `:constant_delta`, `:delta_for`, `:for`. Default: `:auto`.

  `:preserve_insertion_order`: Whether or not preserving insertion order should be preserved. Default: `true`.

  `:arrow_offset_size`: Whether Arrow Arrays use Large or Regular buffers. Default `:regular`.

  `:arrow_use_list_view`: Whether LISTs should produce Arrow ListViews. Default: `false`.

  `:arrow_arrow_lossless_conversion`: Whenever a DuckDB type does not have a clear native or canonical extension match in Arrow, export the types with a duckdb.type_name extension name. Default: `false`.

  `:produce_arrow_string_views`: Whether when producing arrow objects we produce string_views or regular strings. Default: `false`.

  `:extension_directory`: Directory to store extension binaries in. Default: `nil`.

  `:allow_unsigned_extensions`: Whether unsigned extensions should be loaded. Default: `false`.

  `:allow_community_extensions`: Whether community extensions should be loaded. Default: `true`.

  `:allow_extensions_metadata_mismatch`: Whether extensions with missing metadata should be loaded. Default: `false`.

  `:enable_fsst_vectors`: Enable emitting FSST Vectors. Default: `false`.

  `:enable_view_dependencies`: Enable VIEWs to create dependencies. Default: `false`.

  `:enable_macro_dependencies`: Enable macros to create dependencies. Default: `false`.

  `:user_options`: Default: The set of user-provided options. `nil`.

  `:unrecognized_options`: The set of unrecognized (other) options. Default: `nil`.

  `:immediate_transaction_mode`: Start transactions immediately in all attached databases - instead of lazily when a database is referenced. Default: `false`.

  `:lock_configuration`: Whether or not the configuration settings can be altered. Default: `false`.

  `:allocator_flush_threshold`: The peak allocation threshold at which to flush the allocator after completing a task (1 << 27, ~128MB). In bytes. Default: `nil` (134217728 bytes);

  `:allocator_bulk_deallocation_flush_threshold`: If bulk deallocation larger than this occurs, flush outstanding allocations (1 << 30, ~1GB). Default: `nil` (536870912 in bytes)

  `:allocator_background_threads`:  Whether the allocator background thread is enabled. Default: `false`.

  `:duckdb_api`: DuckDB API surface. Default: `nil`.

  `:custom_user_agent`: Metadata from DuckDB callers. Default: `nil`.

  `:default_block_alloc_size`: The default block allocation size for new duckdb database files (new as-in, they do not yet exist). In bytes. Default: `nil` (DUCKDB_BLOCK_ALLOC_SIZE = 262144 ULL)

  `:abort_on_wal_failure`: Whether or not to abort if a serialization exception is thrown during WAL playback (when reading truncated WAL). Default: `false`.

  `:index_scan_percentage`: The index_scan_percentage sets a threshold for index scans. If fewer than MAX(index_scan_max_count, index_scan_percentage * total_row_count) rows match, we perform an index scan instead of a table scan. Default: `nil` (0.001).

  `:index_scan_max_count`: The index_scan_max_count sets a threshold for index scans. If fewer than MAX(index_scan_max_count, index_scan_percentage * total_row_count) rows match, we perform an index scan instead of a table scan. Default: `nil` (STANDARD_VECTOR_SIZE = 2048).

  `:catalog_error_max_schemas`: The maximum number of schemas we will look through for "did you mean..." style errors in the catalog. Default: `nil` (100).

  `:max_vacuum_tasks`: The maximum amount of vacuum tasks to schedule during a checkpoint. Default: `nil` (100).

  `:memory_allocator`: The memory allocator used by the database system. Maybe `:duckdb` - native DuckDB allocator or `:erlang` - erlang 'void *enif_alloc(size_t size)' allocator. Default: `:duckdb`.
  """
  @type t :: %__MODULE__{
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
          external_threads: pos_integer() | nil,
          use_temporary_directory: boolean(),
          temporary_directory: binary() | nil,
          trim_free_blocks: boolean(),
          buffer_manager_track_eviction_timestamps: boolean(),
          allow_unredacted_secrets: boolean(),
          collation: binary() | nil,
          default_order_type: :asc | :desc,
          default_null_order: :nulls_last | :nulls_first,
          enable_external_access: boolean(),
          object_cache_enable: boolean(),
          http_metadata_cache_enable: boolean(),
          http_proxy: binary() | nil,
          http_proxy_username: binary() | nil,
          http_proxy_password: binary() | nil,
          force_checkpoint: boolean(),
          checkpoint_on_shutdown: boolean(),
          serialization_compatibility: binary() | nil,
          disabled_optimizers: list(Duckdbex.Config.optimizer_type()) | [] | nil,
          force_compression:
            :auto
            | :uncompressed
            | :constant
            | :rle
            | :dictionary
            | :pfor_delta
            | :bitpacking
            | :fsst
            | :chimp
            | :patas,
          force_bitpacking_mode: :auto | :constant | :constant_delta | :delta_for | :for,
          preserve_insertion_order: boolean(),
          arrow_offset_size: :regular | :large,
          arrow_use_list_view: boolean(),
          arrow_arrow_lossless_conversion: boolean(),
          produce_arrow_string_views: boolean(),
          extension_directory: binary() | nil,
          allow_unsigned_extensions: boolean(),
          allow_community_extensions: boolean(),
          allow_extensions_metadata_mismatch: boolean(),
          enable_fsst_vectors: boolean(),
          enable_view_dependencies: boolean(),
          enable_macro_dependencies: boolean(),
          user_options: list({binary(), any()}),
          unrecognized_options: list({binary(), any()}),
          immediate_transaction_mode: boolean(),
          lock_configuration: boolean(),
          allocator_flush_threshold: pos_integer() | nil,
          allocator_bulk_deallocation_flush_threshold: pos_integer() | nil,
          allocator_background_threads: boolean(),
          duckdb_api: binary() | nil,
          custom_user_agent: binary() | nil,
          default_block_alloc_size: pos_integer() | nil,
          abort_on_wal_failure: boolean(),
          index_scan_percentage: number() | nil,
          index_scan_max_count: pos_integer() | nil,
          catalog_error_max_schemas: pos_integer() | nil,
          max_vacuum_tasks: pos_integer() | nil,
          memory_allocator: :duckdb | :erlang
        }
end
