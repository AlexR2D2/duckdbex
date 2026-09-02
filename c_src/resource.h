#pragma once
#include "duckdb.hpp"
#include <erl_nif.h>

/*
 * Erlang resources
 */

static ErlNifResourceType* database_nif_type = nullptr;
static ErlNifResourceType* config_nif_type = nullptr;
static ErlNifResourceType* connection_nif_type = nullptr;
static ErlNifResourceType* query_result_nif_type = nullptr;
static ErlNifResourceType* prepared_statement_nif_type = nullptr;
static ErlNifResourceType* appender_nif_type = nullptr;

/*
 * Erlang resource holds DuckDB object
 */

template<class T>
struct erlang_resource {
  std::unique_ptr<T> data;
  void* parent_resource = nullptr;

  erlang_resource(std::unique_ptr<T> d)
      : data(std::move(d)) {}
};

template<class T>
static void resource_destructor(ErlNifEnv*, void* arg) {
  auto* resource = static_cast<erlang_resource<T>*>(arg);
  if (resource->parent_resource) {
    enif_release_resource(resource->parent_resource);
  }
  resource->~erlang_resource<T>();
}

/*
 * Erlang resource builder
 */

template<class Data>
class ErlangResourceBuilder {
  public:
    typedef erlang_resource<Data> Resource;

    ErlangResourceBuilder(ErlNifResourceType* resource_type, duckdb::unique_ptr<Data> data)
      : resource(static_cast<Resource*>(enif_alloc_resource(resource_type, sizeof(Resource)))) {
      if (!resource) {
        throw std::bad_alloc();
      }

      new (resource) Resource(std::move(data));
    }

    template <typename... Args>
    ErlangResourceBuilder(ErlNifResourceType* resource_type, Args&&... args)
      : resource(static_cast<Resource*>(enif_alloc_resource(resource_type, sizeof(Resource)))) {
      if (!resource) {
        throw std::bad_alloc();
      }

      new (resource) Resource(duckdb::make_uniq<Data>(std::forward<Args>(args)...));
    }

    ~ErlangResourceBuilder() {
      if (resource) {
        enif_release_resource(resource);
        resource = nullptr;
      }
    }

    Resource* get() {
      return resource;
    }

    ERL_NIF_TERM make_and_release_resource(ErlNifEnv* env) {
      if (resource) {
        ERL_NIF_TERM term = enif_make_resource(env, resource);
        enif_release_resource(resource);
        resource = nullptr;
        return term;
      } else {
        throw std::runtime_error("resource is empty");
      }
    }
  private:
    Resource* resource;
};

/*
 * Erlang resource getters
 */

template <class T>
erlang_resource<T>* get_resource(ErlNifEnv* env, ERL_NIF_TERM term);

template <>
inline erlang_resource<duckdb::DuckDB>* get_resource(ErlNifEnv* env, ERL_NIF_TERM term) {
  erlang_resource<duckdb::DuckDB>* resource = nullptr;
  if(enif_get_resource(env, term, database_nif_type, (void**)&resource) && resource->data)
    return resource;
  return nullptr;
}

template <>
inline erlang_resource<duckdb::DBConfig>* get_resource(ErlNifEnv* env, ERL_NIF_TERM term) {
  erlang_resource<duckdb::DBConfig>* resource = nullptr;
  if(enif_get_resource(env, term, config_nif_type, (void**)&resource) && resource->data)
    return resource;
  return nullptr;
}

template <>
inline erlang_resource<duckdb::Connection>* get_resource(ErlNifEnv* env, ERL_NIF_TERM term) {
  erlang_resource<duckdb::Connection>* resource = nullptr;
  if(enif_get_resource(env, term, connection_nif_type, (void**)&resource) && resource->data)
    return resource;
  return nullptr;
}

template <>
inline erlang_resource<duckdb::QueryResult>* get_resource(ErlNifEnv* env, ERL_NIF_TERM term) {
  erlang_resource<duckdb::QueryResult>* resource = nullptr;
  if(enif_get_resource(env, term, query_result_nif_type, (void**)&resource) && resource->data)
    return resource;
  return nullptr;
}

template <>
inline erlang_resource<duckdb::PreparedStatement>* get_resource(ErlNifEnv* env, ERL_NIF_TERM term) {
  erlang_resource<duckdb::PreparedStatement>* resource = nullptr;
  if(enif_get_resource(env, term, prepared_statement_nif_type, (void**)&resource) && resource->data)
    return resource;
  return nullptr;
}

template <>
inline erlang_resource<duckdb::Appender>* get_resource(ErlNifEnv* env, ERL_NIF_TERM term) {
  erlang_resource<duckdb::Appender>* resource = nullptr;
  if(enif_get_resource(env, term, appender_nif_type, (void**)&resource) && resource->data)
    return resource;
  return nullptr;
}

template <class T>
erlang_resource<T>* get_resource(ErlNifEnv* env, ERL_NIF_TERM term, ErlNifResourceType* resource_type) {
  erlang_resource<T>* resource = nullptr;
  if(enif_get_resource(env, term, resource_type, (void**)&resource) && resource->data)
    return resource;

  return nullptr;
}
