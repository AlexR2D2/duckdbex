#pragma once
#include "duckdb.hpp"
#include <erl_nif.h>

template<class T>
struct erlang_resource {
  std::unique_ptr<T> data;
};

template<class Data>
class ErlangResourceBuilder {
  public:
    typedef erlang_resource<Data> Resource;

    ErlangResourceBuilder(ErlNifResourceType* resource_type, std::unique_ptr<Data> data)
      : resource(static_cast<Resource*>(enif_alloc_resource(resource_type, sizeof(Resource)))) {
      if (resource) {
        memset(resource, 0, sizeof(Resource));
        resource->data = std::move(data);
      } else {
        throw std::runtime_error("out of memory");
      }
    }

    template <typename... Args>
    ErlangResourceBuilder(ErlNifResourceType* resource_type, Args&&... args)
      : resource(static_cast<Resource*>(enif_alloc_resource(resource_type, sizeof(Resource)))) {
      if (resource) {
        memset(resource, 0, sizeof(Resource));
        resource->data = duckdb::make_unique<Data>(std::forward<Args>(args)...);
      } else {
        throw std::runtime_error("out of memory");
      }
    }

    ~ErlangResourceBuilder() {
      if (resource)
        enif_release_resource(resource);
    }

    Resource* get() {
      return resource;
    }

    ERL_NIF_TERM make_and_release_resource(ErlNifEnv* env) {
      ERL_NIF_TERM term = enif_make_resource(env, resource);
      enif_release_resource(resource);
      resource = nullptr;
      return term;
    }
  private:
    Resource* resource;
};
