CXX ?= g++
CXXFLAGS ?= -O3 -flto=auto -std=c++20 -march=x86-64-v3 -pthread -Wall -Wextra
LDFLAGS ?= -pthread
IMAGE ?= shopping-sessions:latest
PLATFORM ?= linux/amd64

.PHONY: all static clean build

all: build/shopping-sessions

build/shopping-sessions: src/main.cpp src/arena.hpp src/catalog_people.hpp src/io.hpp src/keys.hpp src/parquet_writer.hpp src/parse.hpp
	@mkdir -p build
	$(CXX) $(CXXFLAGS) src/main.cpp $(LDFLAGS) -o $@

static: src/main.cpp src/arena.hpp src/catalog_people.hpp src/io.hpp src/keys.hpp src/parquet_writer.hpp src/parse.hpp
	@mkdir -p build
	$(CXX) $(CXXFLAGS) -static src/main.cpp $(LDFLAGS) -o build/shopping-sessions
	strip build/shopping-sessions

build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

clean:
	rm -rf build
