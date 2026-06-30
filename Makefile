CXX      := g++
CXXFLAGS := -std=gnu++17 -O2 -Wall -Wextra

# macOS: clang may need -stdlib=libc++ and has no -lrt
UNAME := $(shell uname)
ifeq ($(UNAME),Linux)
    LDFLAGS  := -lpthread -lrt -lm
else
    LDFLAGS  := -lpthread -lm
    CXX      := clang++
    CXXFLAGS += -stdlib=libc++
endif

# ── core/ (base server: select + in-memory hash map) ─────────────────────────

BASE_SRCS := core/server.cpp core/hashtable.cpp core/avl.cpp \
             core/zset.cpp core/heap.cpp core/thread_pool.cpp

kvs-base: $(BASE_SRCS) core/common.h core/hashtable.h core/avl.h \
          core/zset.h core/list.h core/heap.h core/thread_pool.h
	$(CXX) $(CXXFLAGS) -o $@ $(BASE_SRCS) $(LDFLAGS)

# ── engine/ (full server: kqueue/epoll + WAL + mmap + seqlock RCU) ───────────

ENGINE_SRCS := engine/server.cpp \
               engine/event_loop.cpp \
               engine/wal.cpp \
               engine/mmap_store.cpp \
               engine/rcu.cpp \
               core/hashtable.cpp \
               core/avl.cpp \
               core/zset.cpp \
               core/heap.cpp \
               core/thread_pool.cpp

ENGINE_HDRS := engine/event_loop.h engine/wal.h engine/mmap_store.h engine/rcu.h \
               core/common.h core/hashtable.h core/avl.h \
               core/zset.h core/list.h core/heap.h core/thread_pool.h

kvs: $(ENGINE_SRCS) $(ENGINE_HDRS)
	$(CXX) $(CXXFLAGS) -Icore -o $@ $(ENGINE_SRCS) $(LDFLAGS)

# ── GPU support (opt-in: make bench GPU=1) ────────────────────────────────────

GPU       ?= 0
GPU_FLAGS :=
GPU_LIBS  :=
ifeq ($(GPU),1)
    GPU_FLAGS := -DHAVE_GPU
    GPU_LIBS  := -lnvidia-ml
endif

# ── bench ─────────────────────────────────────────────────────────────────────

BENCH_SRCS := bench/bench.cpp bench/gpu_profiler.cpp

bench/bench: $(BENCH_SRCS) bench/gpu_profiler.h
	$(CXX) $(CXXFLAGS) $(GPU_FLAGS) -o $@ $(BENCH_SRCS) $(LDFLAGS) $(GPU_LIBS)

bench: bench/bench

# ── tests ─────────────────────────────────────────────────────────────────────

TEST_BIN := tests/test_hashtable tests/test_zset tests/test_wal

tests/test_hashtable: tests/test_hashtable.cpp tests/test_util.h \
                      core/hashtable.cpp core/hashtable.h core/common.h
	$(CXX) $(CXXFLAGS) -o $@ tests/test_hashtable.cpp core/hashtable.cpp $(LDFLAGS)

tests/test_zset: tests/test_zset.cpp tests/test_util.h \
                 core/zset.cpp core/avl.cpp core/hashtable.cpp \
                 core/zset.h core/avl.h core/hashtable.h
	$(CXX) $(CXXFLAGS) -o $@ tests/test_zset.cpp core/zset.cpp core/avl.cpp core/hashtable.cpp $(LDFLAGS)

tests/test_wal: tests/test_wal.cpp tests/test_util.h engine/wal.cpp engine/wal.h
	$(CXX) $(CXXFLAGS) -o $@ tests/test_wal.cpp engine/wal.cpp $(LDFLAGS)

# Unit tests (data structures + WAL) followed by the end-to-end integration
# suite (drives the real server over TCP, including SIGKILL crash recovery).
test: $(TEST_BIN) kvs
	@set -e; for t in $(TEST_BIN); do echo "── $$t ──"; ./$$t; done
	@echo "── tests/test_integration.py ──"; python3 tests/test_integration.py

# ── convenience ───────────────────────────────────────────────────────────────

all: kvs-base kvs bench/bench

.PHONY: all clean run-base run run-bench bench test

run-base: kvs-base
	./kvs-base

run: kvs
	./kvs

run-bench: bench/bench
	./bench/bench 127.0.0.1 1234 100000 16

clean:
	rm -f kvs-base kvs bench/bench redis.dat redis.wal $(TEST_BIN)
