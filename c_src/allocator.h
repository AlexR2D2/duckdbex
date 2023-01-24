#pragma once
#include <erl_nif.h>
#include "duckdb.hpp"

namespace nif {
  duckdb::data_ptr_t eddb_allocate(duckdb::PrivateAllocatorData *private_data, duckdb::idx_t n) {
    if(n > std::size_t(-1) / sizeof(duckdb::data_t))
      throw std::bad_alloc();

    if(auto p = static_cast<duckdb::data_ptr_t>(enif_alloc(n * sizeof(duckdb::data_t))))
      return p;

    throw std::bad_alloc();
  }

  void eddb_free(duckdb::PrivateAllocatorData *private_data, duckdb::data_ptr_t p, duckdb::idx_t n) {
    enif_free(p);
  }

  duckdb::data_ptr_t eddb_reallocate(duckdb::PrivateAllocatorData *private_data, duckdb::data_ptr_t p, duckdb::idx_t old_size, duckdb::idx_t n) {
    if(auto rp = static_cast<duckdb::data_ptr_t>(enif_realloc(p, n * sizeof(duckdb::data_t))))
      return rp;

    throw std::bad_alloc();
  }
}
