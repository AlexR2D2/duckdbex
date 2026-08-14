SRC_DIR = c_src
DUCKDB_DIR = $(SRC_DIR)/duckdb

CXXFLAGS = -O3 -std=c++11

# Statically link the DuckDB build-baseline extensions (see
# extension/extension_config.cmake upstream): core_functions and parquet.
CXXFLAGS += -DDUCKDB_EXTENSION_CORE_FUNCTIONS_LINKED=1
CXXFLAGS += -DDUCKDB_EXTENSION_PARQUET_LINKED=1

# Include roots emitted by DuckDB's scripts/package_build.py and recorded
# in c_src/duckdb/.include_dirs at generation time (see bin/regen_duckdb.sh).
DUCKDB_INCLUDE_DIRS = $(shell cat $(DUCKDB_DIR)/.include_dirs)
CXXFLAGS += $(foreach dir, $(DUCKDB_INCLUDE_DIRS), -I"$(DUCKDB_DIR)/$(dir)")
CXXFLAGS += -I"$(ERTS_INCLUDE_DIR)"
CXXFLAGS += -DNDEBUG=1

KERNEL_NAME := $(shell uname -s)

PRIV_DIR = $(MIX_APP_PATH)/priv
LIB_NAME = $(PRIV_DIR)/duckdb_nif.so

ifneq ($(CROSSCOMPILE),)
	CXXFLAGS += -fPIC -fvisibility=hidden
	LDFLAGS += -fPIC -shared
else
	ifeq ($(KERNEL_NAME), Linux)
		CXXFLAGS += -fPIC -fvisibility=hidden
		LDFLAGS += -fPIC -shared
	endif
	ifeq ($(KERNEL_NAME), Darwin)
		CXXFLAGS += -fPIC
		LDFLAGS += -dynamiclib -undefined dynamic_lookup
	endif
	ifeq ($(KERNEL_NAME), $(filter $(KERNEL_NAME),OpenBSD FreeBSD NetBSD))
		CXXFLAGS += -fPIC
		LDFLAGS += -fPIC -shared
	endif
endif

# Compile the file list emitted by DuckDB's scripts/package_build.py
# (unity builds + directly referenced sources), plus the NIF files.
# See c_src/duckdb/.sources for the generated list.
GENERATED_SRC = $(shell cat $(DUCKDB_DIR)/.sources)
NIF_SRC = $(SRC_DIR)/nif.cpp $(SRC_DIR)/config.cpp $(SRC_DIR)/term.cpp $(SRC_DIR)/term_to_value.cpp $(SRC_DIR)/value_to_term.cpp
SRC = $(addprefix $(DUCKDB_DIR)/, $(GENERATED_SRC)) $(NIF_SRC)

OBJ = $(patsubst %.cpp, %.o, $(patsubst %.cc, %.o, $(subst $(SRC_DIR), $(PRIV_DIR), $(SRC))))

.PRECIOUS: $(PRIV_DIR)/. $(PRIV_DIR)%/.

$(PRIV_DIR):
	mkdir -p $@

$(PRIV_DIR)/.:
	mkdir -p $@

$(PRIV_DIR)%/.:
	mkdir -p $@

.SECONDEXPANSION:

$(PRIV_DIR)/%.o: $(SRC_DIR)/%.cpp | $$(@D)/.
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(PRIV_DIR)/%.o: $(SRC_DIR)/%.cc | $$(@D)/.
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(LIB_NAME): $(OBJ)
	$(CXX) $(LDFLAGS) $^ -o $@

all: $(PRIV_DIR) $(SRC) $(LIB_NAME)

# Guard against a broken/partial vendor tree: every file in .sources must
# exist on disk (run this in CI to catch rsync/LFS accidents).
verify-sources:
	@missing=""; while IFS= read -r f; do \
	  [ -f "$(DUCKDB_DIR)/$$f" ] || missing="$$missing $$f"; \
	done < "$(DUCKDB_DIR)/.sources"; \
	if [ -n "$$missing" ]; then \
	  echo "ERROR: missing vendored sources:$$missing"; exit 1; \
	fi; \
	echo "OK: all vendored sources present"

clean:
	$(RM) -rf run $(OBJ)
	$(RM) -f $(LIB_NAME)

.PHONY: all clean verify-sources
