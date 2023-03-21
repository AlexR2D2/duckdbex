SRC_DIR = c_src

CXXFLAGS = -O3 -std=c++11

HEADERS_DIRS = $(sort $(dir $(shell find $(SRC_DIR)/* | grep .h)))

CXXFLAGS += -I"$(ERTS_INCLUDE_DIR)"
CXXFLAGS += $(foreach header, $(HEADERS_DIRS), -I"$(header)")
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

SRC = $(shell find $(SRC_DIR) -name "*.cpp")

OBJ=$(subst $(SRC_DIR), $(PRIV_DIR), $(SRC:.cpp=.o))

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

$(LIB_NAME): $(OBJ)
	$(CXX) $(LDFLAGS) $^ -o $@

all: $(PRIV_DIR) $(SRC) $(LIB_NAME)

clean:
	$(RM) -rf run $(OBJ)
	$(RM) -f $(LIB_NAME)

.PHONY: all clean
