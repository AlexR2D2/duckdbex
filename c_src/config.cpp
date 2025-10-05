#include "allocator.h"
#include "config.h"
#include "map_iterator.h"
#include "term.h"
#include "term_to_value.h"
#include "duckdb.hpp"
#include <iostream>

namespace {
  bool get_user_options(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::case_insensitive_map_t<duckdb::Value>& options) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    if (!enif_is_list(env, term))
      return false;

    unsigned list_length = 0;
    if (!enif_get_list_length(env, term, &list_length))
      return false;

    ERL_NIF_TERM list = term;
    for (size_t i = 0; i < list_length; i++) {
      ERL_NIF_TERM head, tail;
      if (!enif_get_list_cell(env, list, &head, &tail))
        return false;

      int arity = 0;
      const ERL_NIF_TERM* option_parts;
      if (!enif_get_tuple(env, head, &arity, &option_parts) || arity != 2)
        return false;

      ErlNifBinary bin;
      if (!enif_inspect_binary(env, option_parts[0], &bin))
        return false;

      std::string option_name((const char*)bin.data, bin.size);

      duckdb::Value options_value;

      if (nif::is_atom(env, term, "true") || nif::is_atom(env, term, "false")) {
        if (nif::term_to_boolean(env, option_parts[1], options_value)) {
          options[option_name] = options_value;
          list = tail;
          continue;
        }
      }

      if (enif_is_binary(env, option_parts[1])) {
        if (nif::term_to_string(env, option_parts[1], options_value)) {
          options[option_name] = options_value;
          list = tail;
          continue;
        }
      }

      if (enif_is_number(env, option_parts[1])) {
        if (nif::term_to_bigint(env, option_parts[1], options_value) ||
            nif::term_to_ubigint(env, option_parts[1], options_value) ||
            nif::term_to_double(env, option_parts[1], options_value)) {
          options[option_name] = options_value;
          list = tail;
          continue;
        }
      }

      return false;
    }

    return true;
  }
}

namespace {

  bool set_boolean(ErlNifEnv* env, ERL_NIF_TERM term, bool& boolean) {
    if (nif::is_atom(env, term, "true")) {
      boolean = true;
      return true;
    }

    if (nif::is_atom(env, term, "false")) {
      boolean = false;
      return true;
    }

    return false;
  }

  bool set_database_path(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    ErlNifBinary bin;
    if (!enif_inspect_binary(env, term, &bin))
      return false;

    sink.options.database_path = std::string((const char*)bin.data, bin.size);

    return true;
  }

  bool set_database_type(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    ErlNifBinary bin;
    if (!enif_inspect_binary(env, term, &bin))
      return false;

    sink.options.database_type = std::string((const char*)bin.data, bin.size);

    return true;
  }

  bool set_access_mode(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "automatic")) {
      sink.options.access_mode = duckdb::AccessMode::AUTOMATIC;
      return true;
    }

    if (nif::is_atom(env, term, "read_only")) {
      sink.options.access_mode = duckdb::AccessMode::READ_ONLY;
      return true;
    }

    if (nif::is_atom(env, term, "read_write")) {
      sink.options.access_mode = duckdb::AccessMode::READ_WRITE;
      return true;
    }

    return false;
  }

  bool set_checkpoint_wal_size(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    ErlNifUInt64 checkpoint_wal_size;
    if (!enif_get_uint64(env, term, &checkpoint_wal_size))
      return false;

    sink.options.checkpoint_wal_size = checkpoint_wal_size;

    return true;
  }

  bool set_use_direct_io(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    return set_boolean(env, term, sink.options.use_direct_io);
  }

  bool set_load_extensions(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    return set_boolean(env, term, sink.options.load_extensions);
  }

  bool set_autoload_known_extensions(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    return set_boolean(env, term, sink.options.autoload_known_extensions);
  }

  bool set_autoinstall_known_extensions(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    return set_boolean(env, term, sink.options.autoinstall_known_extensions);
  }

  bool set_custom_extension_repo(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    ErlNifBinary bin;
    if (!enif_inspect_binary(env, term, &bin))
      return false;

    sink.options.custom_extension_repo = std::string((const char*)bin.data, bin.size);

    return true;
  }

  bool set_autoinstall_extension_repo(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    ErlNifBinary bin;
    if (!enif_inspect_binary(env, term, &bin))
      return false;

    sink.options.autoinstall_extension_repo = std::string((const char*)bin.data, bin.size);

    return true;
  }

  bool set_maximum_memory(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    ErlNifUInt64 maximum_memory;
    if (!enif_get_uint64(env, term, &maximum_memory))
      return false;

    sink.options.maximum_memory = maximum_memory;

    return true;
  }

  bool set_maximum_swap_space(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    ErlNifUInt64 maximum_swap_space;
    if (!enif_get_uint64(env, term, &maximum_swap_space))
      return false;

    sink.options.maximum_swap_space = maximum_swap_space;

    return true;
  }

  bool set_maximum_threads(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    ErlNifUInt64 maximum_threads;
    if (!enif_get_uint64(env, term, &maximum_threads))
      return false;

    sink.options.maximum_threads = maximum_threads;

    return true;
  }

  bool set_external_threads(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    ErlNifUInt64 external_threads;
    if (!enif_get_uint64(env, term, &external_threads))
      return false;

    sink.options.external_threads = external_threads;

    return true;
  }

  bool set_use_temporary_directory(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    return set_boolean(env, term, sink.options.use_temporary_directory);
  }

  bool set_temporary_directory(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    ErlNifBinary bin;
    if (!enif_inspect_binary(env, term, &bin))
      return false;

    sink.options.temporary_directory = std::string((const char*)bin.data, bin.size);

    return true;
  }

  bool set_trim_free_blocks(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    return set_boolean(env, term, sink.options.trim_free_blocks);
  }

  bool set_buffer_manager_track_eviction_timestamps(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    return set_boolean(env, term, sink.options.buffer_manager_track_eviction_timestamps);
  }

  bool set_allow_unredacted_secrets(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    return set_boolean(env, term, sink.options.allow_unredacted_secrets);
  }

  bool set_disable_database_invalidation(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    return set_boolean(env, term, sink.options.disable_database_invalidation);
  }

  bool set_enable_external_access(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    return set_boolean(env, term, sink.options.enable_external_access);
  }

  bool set_http_metadata_cache_enable(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    return set_boolean(env, term, sink.options.http_metadata_cache_enable);
  }

  bool set_http_proxy(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    ErlNifBinary bin;
    if (!enif_inspect_binary(env, term, &bin))
      return false;

    sink.options.http_proxy = std::string((const char*)bin.data, bin.size);

    return true;
  }

  bool set_http_proxy_username(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    ErlNifBinary bin;
    if (!enif_inspect_binary(env, term, &bin))
      return false;

    sink.options.http_proxy_username = std::string((const char*)bin.data, bin.size);

    return true;
  }

  bool set_http_proxy_password(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    ErlNifBinary bin;
    if (!enif_inspect_binary(env, term, &bin))
      return false;

    sink.options.http_proxy_password = std::string((const char*)bin.data, bin.size);

    return true;
  }

  bool set_force_checkpoint(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    return set_boolean(env, term, sink.options.force_checkpoint);
  }

  bool set_checkpoint_on_shutdown(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    return set_boolean(env, term, sink.options.checkpoint_on_shutdown);
  }

  bool set_serialization_compatibility(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    ErlNifBinary bin;
    if (!enif_inspect_binary(env, term, &bin))
      return false;

    sink.options.serialization_compatibility =
      duckdb::SerializationCompatibility::FromString(std::string((const char*)bin.data, bin.size));

    return true;
  }

  bool set_initialize_default_database(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    return set_boolean(env, term, sink.options.initialize_default_database);
  }

  bool set_disabled_optimizers(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    if (!enif_is_list(env, term))
      return false;

    unsigned list_length = 0;
    if (!enif_get_list_length(env, term, &list_length))
      return false;

    std::set<duckdb::OptimizerType> optimizers;

    ERL_NIF_TERM list = term;
    for (size_t i = 0; i < list_length; i++) {
      ERL_NIF_TERM head, tail;
      if (!enif_get_list_cell(env, list, &head, &tail))
        return false;

      if (nif::is_atom(env, head, "invalid"))
        optimizers.insert(duckdb::OptimizerType::INVALID);
      if (nif::is_atom(env, head, "expression_rewriter"))
        optimizers.insert(duckdb::OptimizerType::EXPRESSION_REWRITER);
      if (nif::is_atom(env, head, "filter_pullup"))
        optimizers.insert(duckdb::OptimizerType::FILTER_PULLUP);
      if (nif::is_atom(env, head, "filter_pushdown"))
        optimizers.insert(duckdb::OptimizerType::FILTER_PUSHDOWN);
      if (nif::is_atom(env, head, "cte_filter_pusher"))
        optimizers.insert(duckdb::OptimizerType::CTE_FILTER_PUSHER);
      if (nif::is_atom(env, head, "regex_range"))
        optimizers.insert(duckdb::OptimizerType::REGEX_RANGE);
      if (nif::is_atom(env, head, "in_clause"))
        optimizers.insert(duckdb::OptimizerType::IN_CLAUSE);
      if (nif::is_atom(env, head, "join_order"))
        optimizers.insert(duckdb::OptimizerType::JOIN_ORDER);
      if (nif::is_atom(env, head, "deliminator"))
        optimizers.insert(duckdb::OptimizerType::DELIMINATOR);
      if (nif::is_atom(env, head, "unnest_rewriter"))
        optimizers.insert(duckdb::OptimizerType::UNNEST_REWRITER);
      if (nif::is_atom(env, head, "unused_columns"))
        optimizers.insert(duckdb::OptimizerType::UNUSED_COLUMNS);
      if (nif::is_atom(env, head, "statistics_propagation"))
        optimizers.insert(duckdb::OptimizerType::STATISTICS_PROPAGATION);
      if (nif::is_atom(env, head, "common_subexpressions"))
        optimizers.insert(duckdb::OptimizerType::COMMON_SUBEXPRESSIONS);
      if (nif::is_atom(env, head, "common_aggregate"))
        optimizers.insert(duckdb::OptimizerType::COMMON_AGGREGATE);
      if (nif::is_atom(env, head, "column_lifetime"))
        optimizers.insert(duckdb::OptimizerType::COLUMN_LIFETIME);
      if (nif::is_atom(env, head, "build_side_probe_side"))
        optimizers.insert(duckdb::OptimizerType::BUILD_SIDE_PROBE_SIDE);
      if (nif::is_atom(env, head, "limit_pushdown"))
        optimizers.insert(duckdb::OptimizerType::LIMIT_PUSHDOWN);
      if (nif::is_atom(env, head, "top_n"))
        optimizers.insert(duckdb::OptimizerType::TOP_N);
      if (nif::is_atom(env, head, "compressed_materialization"))
        optimizers.insert(duckdb::OptimizerType::COMPRESSED_MATERIALIZATION);
      if (nif::is_atom(env, head, "duplicate_groups"))
        optimizers.insert(duckdb::OptimizerType::DUPLICATE_GROUPS);
      if (nif::is_atom(env, head, "reorder_filter"))
        optimizers.insert(duckdb::OptimizerType::REORDER_FILTER);
      if (nif::is_atom(env, head, "join_filter_pushdown"))
        optimizers.insert(duckdb::OptimizerType::JOIN_FILTER_PUSHDOWN);
      if (nif::is_atom(env, head, "extension"))
        optimizers.insert(duckdb::OptimizerType::EXTENSION);
      if (nif::is_atom(env, head, "materialized_cte"))
        optimizers.insert(duckdb::OptimizerType::MATERIALIZED_CTE);

      list = tail;
    }

    sink.options.disabled_optimizers = optimizers;

    return true;
  }

  bool set_zstd_min_string_length(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    ErlNifUInt64 zstd_min_string_length;
    if (!enif_get_uint64(env, term, &zstd_min_string_length))
      return false;

    sink.options.zstd_min_string_length = zstd_min_string_length;

    return true;
  }

  bool set_force_compression(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    if (nif::is_atom(env, term, "compression_auto")) {
      sink.options.force_compression = duckdb::CompressionType::COMPRESSION_AUTO;
      return true;
    }

    if (nif::is_atom(env, term, "compression_uncompressed")) {
      sink.options.force_compression = duckdb::CompressionType::COMPRESSION_UNCOMPRESSED;
      return true;
    }

    if (nif::is_atom(env, term, "compression_constant")) {
      sink.options.force_compression = duckdb::CompressionType::COMPRESSION_CONSTANT;
      return true;
    }

    if (nif::is_atom(env, term, "compression_rle")) {
      sink.options.force_compression = duckdb::CompressionType::COMPRESSION_RLE;
      return true;
    }

    if (nif::is_atom(env, term, "compression_dictionary")) {
      sink.options.force_compression = duckdb::CompressionType::COMPRESSION_DICTIONARY;
      return true;
    }

    if (nif::is_atom(env, term, "compression_prof_delta")) {
      sink.options.force_compression = duckdb::CompressionType::COMPRESSION_PFOR_DELTA;
      return true;
    }

    if (nif::is_atom(env, term, "compression_bitpacking")) {
      sink.options.force_compression = duckdb::CompressionType::COMPRESSION_BITPACKING;
      return true;
    }

    if (nif::is_atom(env, term, "compression_fsst")) {
      sink.options.force_compression = duckdb::CompressionType::COMPRESSION_FSST;
      return true;
    }

    if (nif::is_atom(env, term, "compression_chimp")) {
      sink.options.force_compression = duckdb::CompressionType::COMPRESSION_CHIMP;
      return true;
    }

    if (nif::is_atom(env, term, "compression_patas")) {
      sink.options.force_compression = duckdb::CompressionType::COMPRESSION_PATAS;
      return true;
    }

    if (nif::is_atom(env, term, "compression_alp")) {
      sink.options.force_compression = duckdb::CompressionType::COMPRESSION_ALP;
      return true;
    }

    if (nif::is_atom(env, term, "compression_alprd")) {
      sink.options.force_compression = duckdb::CompressionType::COMPRESSION_ALPRD;
      return true;
    }

    if (nif::is_atom(env, term, "compression_zstd")) {
      sink.options.force_compression = duckdb::CompressionType::COMPRESSION_ZSTD;
      return true;
    }

    if (nif::is_atom(env, term, "compression_roaring")) {
      sink.options.force_compression = duckdb::CompressionType::COMPRESSION_ROARING;
      return true;
    }

    if (nif::is_atom(env, term, "compression_empty")) {
      sink.options.force_compression = duckdb::CompressionType::COMPRESSION_EMPTY;
      return true;
    }

    if (nif::is_atom(env, term, "compression_count")) {
      sink.options.force_compression = duckdb::CompressionType::COMPRESSION_COUNT;
      return true;
    }

    return false;
  }

  bool set_disabled_compression_methods(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    if (!enif_is_list(env, term))
      return false;

    unsigned list_length = 0;
    if (!enif_get_list_length(env, term, &list_length))
      return false;

    std::set<duckdb::CompressionType> compressions;

    ERL_NIF_TERM list = term;
    for (size_t i = 0; i < list_length; i++) {
      ERL_NIF_TERM head, tail;
      if (!enif_get_list_cell(env, list, &head, &tail))
        return false;

      if (nif::is_atom(env, head, "compression_auto"))
        compressions.insert(duckdb::CompressionType::COMPRESSION_AUTO);
      if (nif::is_atom(env, head, "compression_uncompressed"))
        compressions.insert(duckdb::CompressionType::COMPRESSION_UNCOMPRESSED);
      if (nif::is_atom(env, head, "compression_constant"))
        compressions.insert(duckdb::CompressionType::COMPRESSION_CONSTANT);
      if (nif::is_atom(env, head, "compression_rle"))
        compressions.insert(duckdb::CompressionType::COMPRESSION_RLE);
      if (nif::is_atom(env, head, "compression_dictionary"))
        compressions.insert(duckdb::CompressionType::COMPRESSION_DICTIONARY);
      if (nif::is_atom(env, head, "compression_prof_delta"))
        compressions.insert(duckdb::CompressionType::COMPRESSION_PFOR_DELTA);
      if (nif::is_atom(env, head, "compression_bitpacking"))
        compressions.insert(duckdb::CompressionType::COMPRESSION_BITPACKING);
      if (nif::is_atom(env, head, "compression_fsst"))
        compressions.insert(duckdb::CompressionType::COMPRESSION_FSST);
      if (nif::is_atom(env, head, "compression_chimp"))
        compressions.insert(duckdb::CompressionType::COMPRESSION_CHIMP);
      if (nif::is_atom(env, head, "compression_patas"))
        compressions.insert(duckdb::CompressionType::COMPRESSION_PATAS);
      if (nif::is_atom(env, head, "compression_alp"))
        compressions.insert(duckdb::CompressionType::COMPRESSION_ALP);
      if (nif::is_atom(env, head, "compression_alprd"))
        compressions.insert(duckdb::CompressionType::COMPRESSION_ALPRD);
      if (nif::is_atom(env, head, "compression_zstd"))
        compressions.insert(duckdb::CompressionType::COMPRESSION_ZSTD);
      if (nif::is_atom(env, head, "compression_roaring"))
        compressions.insert(duckdb::CompressionType::COMPRESSION_ROARING);
      if (nif::is_atom(env, head, "compression_empty"))
        compressions.insert(duckdb::CompressionType::COMPRESSION_EMPTY);
      if (nif::is_atom(env, head, "compression_count"))
        compressions.insert(duckdb::CompressionType::COMPRESSION_COUNT);

      list = tail;
    }

    sink.options.disabled_compression_methods = compressions;

    return true;
  }

  bool set_force_bitpacking_mode(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "auto")) {
      sink.options.force_bitpacking_mode = duckdb::BitpackingMode::AUTO;
      return true;
    }

    if (nif::is_atom(env, term, "constant")) {
      sink.options.force_bitpacking_mode = duckdb::BitpackingMode::CONSTANT;
      return true;
    }

    if (nif::is_atom(env, term, "constant_delta")) {
      sink.options.force_bitpacking_mode = duckdb::BitpackingMode::CONSTANT_DELTA;
      return true;
    }

    if (nif::is_atom(env, term, "delta_for")) {
      sink.options.force_bitpacking_mode = duckdb::BitpackingMode::DELTA_FOR;
      return true;
    }

    if (nif::is_atom(env, term, "for")) {
      sink.options.force_bitpacking_mode = duckdb::BitpackingMode::FOR;
      return true;
    }

    return false;
  }

  bool set_extension_directory(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    ErlNifBinary bin;
    if (!enif_inspect_binary(env, term, &bin))
      return false;

    sink.options.extension_directory = std::string((const char*)bin.data, bin.size);

    return true;
  }

  bool set_allow_unsigned_extensions(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    return set_boolean(env, term, sink.options.allow_unsigned_extensions);
  }

  bool set_allow_community_extensions(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    return set_boolean(env, term, sink.options.allow_community_extensions);
  }

  bool set_user_options(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    duckdb::case_insensitive_map_t<duckdb::Value> options;

    if (!get_user_options(env, term, options))
      return false;

    sink.options.user_options = options;

    return true;
  }

  bool set_unrecognized_options(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    duckdb::case_insensitive_map_t<duckdb::Value> options;

    if (!get_user_options(env, term, options))
      return false;

    sink.options.unrecognized_options = options;

    return true;
  }

  bool set_lock_configuration(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    return set_boolean(env, term, sink.options.lock_configuration);
  }

  bool set_allocator_flush_threshold(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    ErlNifUInt64 allocator_flush_threshold;
    if (!enif_get_uint64(env, term, &allocator_flush_threshold))
      return false;

    sink.options.allocator_flush_threshold = allocator_flush_threshold;

    return true;
  }

  bool set_allocator_bulk_deallocation_flush_threshold(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    ErlNifUInt64 allocator_bulk_deallocation_flush_threshold;
    if (!enif_get_uint64(env, term, &allocator_bulk_deallocation_flush_threshold))
      return false;

    sink.options.allocator_bulk_deallocation_flush_threshold = allocator_bulk_deallocation_flush_threshold;

    return true;
  }

  bool set_allocator_background_threads(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    return set_boolean(env, term, sink.options.allocator_background_threads);
  }

  bool set_duckdb_api(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    ErlNifBinary bin;
    if (!enif_inspect_binary(env, term, &bin))
      return false;

    sink.options.duckdb_api = std::string((const char*)bin.data, bin.size);

    return true;
  }

  bool set_custom_user_agent(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    ErlNifBinary bin;
    if (!enif_inspect_binary(env, term, &bin))
      return false;

    sink.options.custom_user_agent = std::string((const char*)bin.data, bin.size);

    return true;
  }

  bool set_temp_file_encryption(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    return set_boolean(env, term, sink.options.temp_file_encryption);
  }

  bool set_default_block_alloc_size(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    ErlNifUInt64 default_block_alloc_size;
    if (!enif_get_uint64(env, term, &default_block_alloc_size))
      return false;

    sink.options.default_block_alloc_size = default_block_alloc_size;

    return true;
  }

  bool set_default_block_header_size(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    ErlNifUInt64 default_block_header_size;
    if (!enif_get_uint64(env, term, &default_block_header_size))
      return false;

    sink.options.default_block_header_size = default_block_header_size;

    return true;
  }

  bool set_abort_on_wal_failure(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    return set_boolean(env, term, sink.options.abort_on_wal_failure);
  }

  bool set_memory_allocator(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "duckdb"))
      return true;

    if (nif::is_atom(env, term, "erlang")) {
      sink.allocator = duckdb::make_uniq<duckdb::Allocator>(nif::eddb_allocate, nif::eddb_free, nif::eddb_reallocate, nullptr);
      sink.default_allocator = duckdb::make_shared_ptr<duckdb::Allocator>(nif::eddb_allocate, nif::eddb_free, nif::eddb_reallocate, nullptr);
      return true;
    }

    return false;
  }

  bool set_allowed_paths(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    if (!enif_is_list(env, term))
      return false;

    unsigned list_length = 0;
    if (!enif_get_list_length(env, term, &list_length))
      return false;

    std::unordered_set<std::string> allowed_paths;

    ERL_NIF_TERM list = term;
    for (size_t i = 0; i < list_length; i++) {
      ERL_NIF_TERM head, tail;
      if (!enif_get_list_cell(env, list, &head, &tail))
        return false;

      ErlNifBinary bin;
      if (!enif_inspect_binary(env, head, &bin))
        return false;

      allowed_paths.insert(std::string((const char*)bin.data, bin.size));

      list = tail;
    }

    sink.options.allowed_paths = allowed_paths;

    return true;
  }

  bool set_allowed_directories(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    if (!enif_is_list(env, term))
      return false;

    unsigned list_length = 0;
    if (!enif_get_list_length(env, term, &list_length))
      return false;

    std::set<std::string> allowed_directories;

    ERL_NIF_TERM list = term;
    for (size_t i = 0; i < list_length; i++) {
      ERL_NIF_TERM head, tail;
      if (!enif_get_list_cell(env, list, &head, &tail))
        return false;

      ErlNifBinary bin;
      if (!enif_inspect_binary(env, head, &bin))
        return false;

      allowed_directories.insert(std::string((const char*)bin.data, bin.size));

      list = tail;
    }

    sink.options.allowed_directories = allowed_directories;

    return true;
  }

  bool set_option(ErlNifEnv* env, ERL_NIF_TERM name, ERL_NIF_TERM value, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, name, "database_path"))
      return set_database_path(env, value, sink);

    if (nif::is_atom(env, name, "database_type"))
      return set_database_type(env, value, sink);

    if (nif::is_atom(env, name, "access_mode"))
      return set_access_mode(env, value, sink);

    if (nif::is_atom(env, name, "checkpoint_wal_size"))
      return set_checkpoint_wal_size(env, value, sink);

    if (nif::is_atom(env, name, "use_direct_io"))
      return set_use_direct_io(env, value, sink);

    if (nif::is_atom(env, name, "load_extensions"))
      return set_load_extensions(env, value, sink);

    if (nif::is_atom(env, name, "autoload_known_extensions"))
      return set_autoload_known_extensions(env, value, sink);

    if (nif::is_atom(env, name, "autoinstall_known_extensions"))
      return set_autoinstall_known_extensions(env, value, sink);

    if (nif::is_atom(env, name, "custom_extension_repo"))
      return set_custom_extension_repo(env, value, sink);

    if (nif::is_atom(env, name, "autoinstall_extension_repo"))
      return set_autoinstall_extension_repo(env, value, sink);

    if (nif::is_atom(env, name, "maximum_memory"))
      return set_maximum_memory(env, value, sink);

    if (nif::is_atom(env, name, "maximum_swap_space"))
      return set_maximum_swap_space(env, value, sink);

    if (nif::is_atom(env, name, "maximum_threads"))
      return set_maximum_threads(env, value, sink);

    if (nif::is_atom(env, name, "external_threads"))
      return set_external_threads(env, value, sink);

    if (nif::is_atom(env, name, "use_temporary_directory"))
      return set_use_temporary_directory(env, value, sink);

    if (nif::is_atom(env, name, "temporary_directory"))
      return set_temporary_directory(env, value, sink);

    if (nif::is_atom(env, name, "trim_free_blocks"))
      return set_trim_free_blocks(env, value, sink);

    if (nif::is_atom(env, name, "buffer_manager_track_eviction_timestamps"))
      return set_buffer_manager_track_eviction_timestamps(env, value, sink);

    if (nif::is_atom(env, name, "allow_unredacted_secrets"))
      return set_allow_unredacted_secrets(env, value, sink);

    if (nif::is_atom(env, name, "disable_database_invalidation"))
      return set_disable_database_invalidation(env, value, sink);

    if (nif::is_atom(env, name, "enable_external_access"))
      return set_enable_external_access(env, value, sink);

    if (nif::is_atom(env, name, "http_metadata_cache_enable"))
      return set_http_metadata_cache_enable(env, value, sink);

    if (nif::is_atom(env, name, "http_proxy"))
      return set_http_proxy(env, value, sink);

    if (nif::is_atom(env, name, "http_proxy_username"))
      return set_http_proxy_username(env, value, sink);

    if (nif::is_atom(env, name, "http_proxy_password"))
      return set_http_proxy_password(env, value, sink);

    if (nif::is_atom(env, name, "force_checkpoint"))
      return set_force_checkpoint(env, value, sink);

    if (nif::is_atom(env, name, "checkpoint_on_shutdown"))
      return set_checkpoint_on_shutdown(env, value, sink);

    if (nif::is_atom(env, name, "serialization_compatibility"))
      return set_serialization_compatibility(env, value, sink);

    if (nif::is_atom(env, name, "initialize_default_database"))
      return set_initialize_default_database(env, value, sink);

    if (nif::is_atom(env, name, "disabled_optimizers"))
      return set_disabled_optimizers(env, value, sink);

    if (nif::is_atom(env, name, "zstd_min_string_length"))
      return set_zstd_min_string_length(env, value, sink);

    if (nif::is_atom(env, name, "force_compression"))
      return set_force_compression(env, value, sink);

    if (nif::is_atom(env, name, "disabled_compression_methods"))
      return set_disabled_compression_methods(env, value, sink);

    if (nif::is_atom(env, name, "force_bitpacking_mode"))
      return set_force_bitpacking_mode(env, value, sink);

    if (nif::is_atom(env, name, "extension_directory"))
      return set_extension_directory(env, value, sink);

    if (nif::is_atom(env, name, "allow_unsigned_extensions"))
      return set_allow_unsigned_extensions(env, value, sink);

    if (nif::is_atom(env, name, "allow_community_extensions"))
      return set_allow_community_extensions(env, value, sink);

    if (nif::is_atom(env, name, "user_options"))
      return set_user_options(env, value, sink);

    if (nif::is_atom(env, name, "unrecognized_options"))
      return set_unrecognized_options(env, value, sink);

    if (nif::is_atom(env, name, "lock_configuration"))
      return set_lock_configuration(env, value, sink);

    if (nif::is_atom(env, name, "allocator_flush_threshold"))
      return set_allocator_flush_threshold(env, value, sink);

    if (nif::is_atom(env, name, "allocator_bulk_deallocation_flush_threshold"))
      return set_allocator_bulk_deallocation_flush_threshold(env, value, sink);

    if (nif::is_atom(env, name, "allocator_background_threads"))
      return set_allocator_background_threads(env, value, sink);

    if (nif::is_atom(env, name, "duckdb_api"))
      return set_duckdb_api(env, value, sink);

    if (nif::is_atom(env, name, "custom_user_agent"))
      return set_custom_user_agent(env, value, sink);

    if (nif::is_atom(env, name, "temp_file_encryption"))
      return set_temp_file_encryption(env, value, sink);

    if (nif::is_atom(env, name, "default_block_alloc_size"))
      return set_default_block_alloc_size(env, value, sink);

    if (nif::is_atom(env, name, "default_block_header_size"))
      return set_default_block_header_size(env, value, sink);

    if (nif::is_atom(env, name, "abort_on_wal_failure"))
      return set_abort_on_wal_failure(env, value, sink);

    if (nif::is_atom(env, name, "allowed_paths"))
      return set_allowed_paths(env, value, sink);

    if (nif::is_atom(env, name, "allowed_directories"))
      return set_allowed_directories(env, value, sink);

    if (nif::is_atom(env, name, "memory_allocator"))
      return set_memory_allocator(env, value, sink);

    if (nif::is_atom(env, name, "__struct__"))
      return true;

    std::string unexpected_option;
    if (nif::atom_to_string(env, name, unexpected_option))
      std::cerr << "Error: unexpected DuckDB DBConfig option: " + unexpected_option << std::endl;

    return false;
  }
}

bool nif::get_config(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
  if (!enif_is_map(env, term))
    return false;

  ErlangMapIterator map_iterator(env, term);
  while (map_iterator.valid && map_iterator.next()) {
    if (!set_option(env, map_iterator.key, map_iterator.value, sink))
      return false;
  }

  return true;
}
