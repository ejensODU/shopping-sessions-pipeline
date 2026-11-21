CXX ?= g++
PYTHON ?= python3
ARROW_DIR ?= $(shell $(PYTHON) -c 'import importlib.util; print(importlib.util.find_spec("pyarrow").submodule_search_locations[0])' 2>/dev/null)
ARCH_FLAGS ?= -march=x86-64-v3
ARROW_LIBS := libparquet.so.2600 libarrow.so.2600

.PHONY: local
local: build/shopping-sessions

build/shopping-sessions: src/main.cpp $(wildcard src/*.hpp) Makefile
	@test -f "$(ARROW_DIR)/include/parquet/file_writer.h" || { echo "Install pyarrow==26.0.1 or set ARROW_DIR to its package directory." >&2; exit 1; }
	mkdir -p build/lib
	$(CXX) -O3 -std=c++20 $(ARCH_FLAGS) -DNDEBUG -pthread $(CPPFLAGS) $(CXXFLAGS) \
	  -isystem "$(ARROW_DIR)/include" src/main.cpp -o $@ $(LDFLAGS) \
	  -L"$(ARROW_DIR)" -Wl,-rpath-link,"$(ARROW_DIR)" -Wl,-rpath,'$$ORIGIN/lib' \
	  $(addprefix -l:,$(ARROW_LIBS))
	@for library in $(ARROW_LIBS); do ln -sf "$(abspath $(ARROW_DIR))/$$library" "build/lib/$$library"; done
