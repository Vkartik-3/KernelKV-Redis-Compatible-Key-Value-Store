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

# ── 14/ (original server) ─────────────────────────────────────────────────────

V14_SRCS := 14/14_server.cpp 14/hashtable.cpp 14/avl.cpp \
             14/zset.cpp 14/heap.cpp 14/thread_pool.cpp

server14: $(V14_SRCS) 14/common.h 14/hashtable.h 14/avl.h \
           14/zset.h 14/list.h 14/heap.h 14/thread_pool.h
	$(CXX) $(CXXFLAGS) -o $@ $(V14_SRCS) $(LDFLAGS)

# ── 15/ (new server: kqueue/epoll + WAL + mmap + seqlock RCU) ────────────────

V15_SRCS := 15/15_server.cpp \
             15/event_loop.cpp \
             15/wal.cpp \
             15/mmap_store.cpp \
             15/rcu.cpp \
             14/hashtable.cpp \
             14/avl.cpp \
             14/zset.cpp \
             14/heap.cpp \
             14/thread_pool.cpp

V15_HDRS := 15/event_loop.h 15/wal.h 15/mmap_store.h 15/rcu.h \
             14/common.h 14/hashtable.h 14/avl.h \
             14/zset.h 14/list.h 14/heap.h 14/thread_pool.h

server15: $(V15_SRCS) $(V15_HDRS)
	$(CXX) $(CXXFLAGS) -I14 -o $@ $(V15_SRCS) $(LDFLAGS)

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

# ── convenience ───────────────────────────────────────────────────────────────

all: server14 server15 bench/bench

.PHONY: all clean run14 run15 run-bench

run14: server14
	./server14

run15: server15
	./server15

run-bench: bench/bench
	./bench/bench 127.0.0.1 1234 100000 16

clean:
	rm -f server14 server15 bench/bench redis.dat redis.wal
