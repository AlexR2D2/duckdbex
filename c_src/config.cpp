#include "allocator.h"
#include "config.h"
#include "map_iterator.h"
#include "term.h"
#include "duckdb.hpp"

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

  bool set_maximum_memory(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    ErlNifUInt64 maximum_memory;
    if (!enif_get_uint64(env, term, &maximum_memory))
      return false;

    sink.options.maximum_memory = maximum_memory;

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

  bool set_collation(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nil"))
      return true;

    ErlNifBinary bin;
    if (!enif_inspect_binary(env, term, &bin))
      return false;

    sink.options.collation = std::string((const char*)bin.data, bin.size);

    return true;
  }

  bool set_default_order_type(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "asc")) {
      sink.options.default_order_type = duckdb::OrderType::ASCENDING;
      return true;
    }

    if (nif::is_atom(env, term, "desc")) {
      sink.options.default_order_type = duckdb::OrderType::DESCENDING;
      return true;
    }

    return false;
  }

  bool set_default_null_order(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "nulls_first")) {
      sink.options.default_null_order = duckdb::DefaultOrderByNullType::NULLS_FIRST;
      return true;
    }

    if (nif::is_atom(env, term, "nulls_last")) {
      sink.options.default_null_order = duckdb::DefaultOrderByNullType::NULLS_LAST;
      return true;
    }

    return false;
  }

  bool set_enable_external_access(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    return set_boolean(env, term, sink.options.enable_external_access);
  }

  bool set_object_cache_enable(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    return set_boolean(env, term, sink.options.object_cache_enable);
  }

  bool set_http_metadata_cache_enable(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    return set_boolean(env, term, sink.options.http_metadata_cache_enable);
  }

  bool set_force_checkpoint(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    return set_boolean(env, term, sink.options.force_checkpoint);
  }

  bool set_checkpoint_on_shutdown(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    return set_boolean(env, term, sink.options.checkpoint_on_shutdown);
  }

  bool set_force_compression(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "auto")) {
      sink.options.force_compression = duckdb::CompressionType::COMPRESSION_AUTO;
      return true;
    }

    if (nif::is_atom(env, term, "uncompressed")) {
      sink.options.force_compression = duckdb::CompressionType::COMPRESSION_UNCOMPRESSED;
      return true;
    }

    if (nif::is_atom(env, term, "constant")) {
      sink.options.force_compression = duckdb::CompressionType::COMPRESSION_CONSTANT;
      return true;
    }

    if (nif::is_atom(env, term, "rle")) {
      sink.options.force_compression = duckdb::CompressionType::COMPRESSION_RLE;
      return true;
    }

    if (nif::is_atom(env, term, "dictionary")) {
      sink.options.force_compression = duckdb::CompressionType::COMPRESSION_DICTIONARY;
      return true;
    }

    if (nif::is_atom(env, term, "pfor_delta")) {
      sink.options.force_compression = duckdb::CompressionType::COMPRESSION_PFOR_DELTA;
      return true;
    }

    if (nif::is_atom(env, term, "bitpacking")) {
      sink.options.force_compression = duckdb::CompressionType::COMPRESSION_BITPACKING;
      return true;
    }

    if (nif::is_atom(env, term, "fsst")) {
      sink.options.force_compression = duckdb::CompressionType::COMPRESSION_FSST;
      return true;
    }

    if (nif::is_atom(env, term, "chimp")) {
      sink.options.force_compression = duckdb::CompressionType::COMPRESSION_CHIMP;
      return true;
    }

    if (nif::is_atom(env, term, "patas")) {
      sink.options.force_compression = duckdb::CompressionType::COMPRESSION_PATAS;
      return true;
    }

    if (nif::is_atom(env, term, "patas")) {
      sink.options.force_compression = duckdb::CompressionType::COMPRESSION_PATAS;
      return true;
    }

    return false;
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

  bool set_preserve_insertion_order(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    return set_boolean(env, term, sink.options.preserve_insertion_order);
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

  bool set_immediate_transaction_mode(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    return set_boolean(env, term, sink.options.immediate_transaction_mode);
  }

  bool set_memory_allocator(ErlNifEnv* env, ERL_NIF_TERM term, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, term, "duckdb"))
      return true;

    if (nif::is_atom(env, term, "erlang")) {
      sink.allocator = duckdb::make_uniq<duckdb::Allocator>(nif::eddb_allocate, nif::eddb_free, nif::eddb_reallocate, nullptr);
      sink.default_allocator = duckdb::make_shared<duckdb::Allocator>(nif::eddb_allocate, nif::eddb_free, nif::eddb_reallocate, nullptr);
      return true;
    }

    return false;
  }

  bool set_option(ErlNifEnv* env, ERL_NIF_TERM name, ERL_NIF_TERM value, duckdb::DBConfig& sink) {
    if (nif::is_atom(env, name, "access_mode"))
      return set_access_mode(env, value, sink);

    if (nif::is_atom(env, name, "checkpoint_wal_size"))
      return set_checkpoint_wal_size(env, value, sink);

    if (nif::is_atom(env, name, "use_direct_io"))
      return set_use_direct_io(env, value, sink);

    if (nif::is_atom(env, name, "load_extensions"))
      return set_load_extensions(env, value, sink);

    if (nif::is_atom(env, name, "maximum_memory"))
      return set_maximum_memory(env, value, sink);

    if (nif::is_atom(env, name, "maximum_threads"))
      return set_maximum_threads(env, value, sink);

    if (nif::is_atom(env, name, "use_temporary_directory"))
      return set_use_temporary_directory(env, value, sink);

    if (nif::is_atom(env, name, "temporary_directory"))
      return set_temporary_directory(env, value, sink);

    if (nif::is_atom(env, name, "collation"))
      return set_collation(env, value, sink);

    if (nif::is_atom(env, name, "default_order_type"))
      return set_default_order_type(env, value, sink);

    if (nif::is_atom(env, name, "default_null_order"))
      return set_default_null_order(env, value, sink);

    if (nif::is_atom(env, name, "enable_external_access"))
      return set_enable_external_access(env, value, sink);

    if (nif::is_atom(env, name, "object_cache_enable"))
      return set_object_cache_enable(env, value, sink);

    if (nif::is_atom(env, name, "http_metadata_cache_enable"))
      return set_http_metadata_cache_enable(env, value, sink);

    if (nif::is_atom(env, name, "force_checkpoint"))
      return set_force_checkpoint(env, value, sink);

    if (nif::is_atom(env, name, "checkpoint_on_shutdown"))
      return set_checkpoint_on_shutdown(env, value, sink);

    if (nif::is_atom(env, name, "force_compression"))
      return set_force_compression(env, value, sink);

    if (nif::is_atom(env, name, "force_bitpacking_mode"))
      return set_force_bitpacking_mode(env, value, sink);

    if (nif::is_atom(env, name, "preserve_insertion_order"))
      return set_preserve_insertion_order(env, value, sink);

    if (nif::is_atom(env, name, "extension_directory"))
      return set_extension_directory(env, value, sink);

    if (nif::is_atom(env, name, "allow_unsigned_extensions"))
      return set_allow_unsigned_extensions(env, value, sink);

    if (nif::is_atom(env, name, "immediate_transaction_mode"))
      return set_immediate_transaction_mode(env, value, sink);

    if (nif::is_atom(env, name, "memory_allocator"))
      return set_memory_allocator(env, value, sink);

    if (nif::is_atom(env, name, "__struct__"))
      return true;

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
