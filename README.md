# KernelKV — Redis-Compatible Key-Value Store

A from-scratch Redis-like key-value store built incrementally across two major server generations. The goal is to demonstrate every systems-programming primitive that makes a real database fast and durable: edge-triggered I/O, write-ahead logging, memory-mapped persistence, seqlock RCU, multithreaded reads, and GPU-correlated benchmarking.

---

## Why This Project Exists

Most key-value store implementations stop at a single-threaded `poll()` loop with no persistence. This project builds that foundation first (v14), then asks: *what would it take to make it production-grade?*

v14 is a complete, working key-value server. v15 is what happens when you engineer it seriously — adding every primitive a real database needs, one at a time, with no external dependencies:

| Limitation in v14 | Solution engineered in v15 |
|---|---|
| `poll()` scans all fds every tick — O(n) | kqueue/epoll edge-triggered — O(events) |
| Data lost on crash | Write-Ahead Log (WAL) + mmap snapshot |
| No persistence across restarts | `mmap(MAP_SHARED)` + `ftruncate()` |
| Reads block writes | Seqlock RCU — concurrent readers, single writer |
| Single-threaded GET | 4-thread pool for GET dispatch |
| No way to measure perf | Benchmark suite with p50/p99/p999 + GPU correlation |

---

## Repository Layout

```
KernelKV/
├── core/                       # base server — poll-based, in-memory
│   ├── server.cpp              # main server: poll() event loop
│   ├── hashtable.h / .cpp      # progressive-rehash hash map
│   ├── avl.h / .cpp            # AVL tree (used by ZSet)
│   ├── zset.h / .cpp           # sorted set: AVL + HMap dual index
│   ├── heap.h / .cpp           # min-heap (TTL expiry)
│   ├── thread_pool.h / .cpp    # 4-thread pool (entry_del only)
│   ├── list.h                  # intrusive doubly-linked list
│   └── common.h                # container_of, FNV hash (str_hash)
│
├── engine/                     # full server — all features enabled
│   ├── server.cpp              # main server: ET event loop + WAL + mmap + MT-GET
│   ├── event_loop.h / .cpp     # kqueue (macOS) / epoll (Linux) abstraction
│   ├── wal.h / .cpp            # Write-Ahead Log: CRC-32 + fdatasync
│   ├── mmap_store.h / .cpp     # mmap + ftruncate append-only snapshot
│   └── rcu.h / .cpp            # seqlock RCU for concurrent reads
│
├── stages/                     # incremental build-up (TCP → hash map → ZSet → TTL)
│   ├── 03/                     # basic TCP echo server
│   ├── 04/                     # request/response framing
│   ├── 06/ – 09/               # protocol parsing, multi-client, hash map
│   ├── 10/                     # AVL tree
│   ├── 11/ – 12/               # sorted set + linked list queries
│   └── 13/                     # heap + TTL expiry
│
├── bench/
│   ├── bench.cpp               # benchmark: p50/p99/p999 + GPU correlation
│   ├── gpu_profiler.h          # GPU profiler interface
│   └── gpu_profiler.cpp        # NVML sampling + latency correlation CSV
│
├── Makefile                    # builds kvs-base, kvs, bench
└── README.md                   # this file
```

---

## Architecture

### v14 — First-generation server (poll-based)

```
Client 1 ──┐
Client 2 ──┤──► poll([fd1,fd2,...,fdN]) ──► single-threaded handler
Client N ──┘        O(N) every tick
                         │
                    HMap + AVL + ZSet
                    (in-memory only, lost on crash)
```

### v15 — Production-grade server

```
                     ┌─────────────────────────────────────────────┐
Clients ─────────────► kqueue / epoll (edge-triggered, O(events))  │
                     │                                             │
                     │  Event Loop (single thread)                 │
                     │  ┌──────────┐  ┌──────────┐  ┌──────────┐ │
                     │  │ handle_  │  │ handle_  │  │ drain_   │ │
                     │  │ accept() │  │ read()   │  │ results()│ │
                     │  └──────────┘  └──────────┘  └──┬───────┘ │
                     │       │             │            │         │
                     │  SET/DEL/ZADD  GET dispatch   GET results  │
                     │       │         via pipe           │        │
                     │       ▼             ▼              ▼        │
                     │  ┌─────────┐  ┌──────────────────────┐    │
                     │  │SeqLock  │  │   Thread Pool (×4)   │    │
                     │  │write_   │  │  worker_get_func()   │    │
                     │  │lock /   │  │  • rcu_read_begin    │    │
                     │  │unlock   │  │  • hm_lookup         │    │
                     │  └────┬────┘  │  • rcu_read_retry    │    │
                     │       │       └──────────────────────┘    │
                     │       ▼                                    │
                     │  ┌─────────────────────┐                  │
                     │  │  HMap + AVL + ZSet  │  ← in-memory DB  │
                     │  └─────────────────────┘                  │
                     │       │                                    │
                     │       ▼                                    │
                     │  ┌───────────┐   ┌──────────────────────┐ │
                     │  │  WAL      │   │  mmap snapshot       │ │
                     │  │  .wal     │   │  redis.dat           │ │
                     │  │  CRC-32   │   │  ftruncate + msync   │ │
                     │  │ fdatasync │   └──────────────────────┘ │
                     │  └───────────┘                            │
                     └─────────────────────────────────────────────┘
```

### WAL record format

```
┌────────────────┬────────────────┬───────┬──────────────────────────────────┐
│  payload_len   │   crc32        │  op   │  nargs │ len │ arg │ len │ arg … │
│    (4 bytes)   │  (4 bytes)     │ (1 B) │ (4 B)  │ (4B)│     │     │       │
└────────────────┴────────────────┴───────┴──────────────────────────────────┘
```

### mmap snapshot record format

```
┌───────────┬──────────┬────────┬──────────────┬────────────┐
│ key_len   │   key    │  type  │   val_len    │    val     │
│  (4 B)    │ (N bytes)│  (1 B) │   (4 bytes)  │  (M bytes) │
└───────────┴──────────┴────────┴──────────────┴────────────┘
type: 0 = tombstone (DEL), 1 = string value
```

### Seqlock RCU protocol

```
Writer (event loop thread):        Reader (thread pool worker):
  rcu_write_lock(&g.rcu)             uint64_t seq;
    seq.fetch_add(1, release)        do {
    // seq is now ODD (write active)   seq = rcu_read_begin(&g.rcu);
    mutate HMap                        //   spin while seq is odd
    seq.fetch_add(1, release)          hm_lookup(...)
    // seq is now EVEN (write done)    copy Entry::str
  rcu_write_unlock(&g.rcu)          } while (rcu_read_retry(&g.rcu, seq));
                                     //   retry if seq changed during read
```

### Multithreaded GET dispatch & wakeup pipe

```
Event loop                  Thread pool workers         Wakeup pipe
    │                              │                         │
    ├─ EV_READ (conn)              │                         │
    │    try_one_request()         │                         │
    │    GET detected              │                         │
    │    dispatch_get() ──────────►│ worker_get_func()       │
    │    el_mod(EV_ERR)            │  rcu_read_begin         │
    │    conn suspended            │  hm_lookup              │
    │                              │  rcu_read_retry         │
    │                              │  push GetResult         │
    │                              │  write(pipe[1]) ───────►│
    │                              │                         │
    ├─ EV_READ (pipe[0]) ◄─────────│─────────────────────────┘
    │    drain_results()           │
    │    try_one_request() ───────►│ (next GET)
    │    handle_write()            │
    │    conn re-enabled           │
```

### MVCC — multi-version concurrency control & transactions

String keys are stored as **version chains** rather than single values, giving real **snapshot-isolation transactions**. Each key owns a linked list of versions, newest first:

```
Entry "k"
└── vhead → Version{commit_ts=7, "v3"} → Version{commit_ts=4, "v2"} → Version{commit_ts=2, "v1"} → ∅
```

- **Global commit clock** (`g.clock`): a monotonically increasing timestamp; every committed write gets one.
- **Snapshot reads**: a transaction fixes `read_ts = g.clock` at `BEGIN`; every read returns the newest version with `commit_ts <= read_ts`. Writes committed by other connections *after* that point are invisible — a stable, consistent snapshot. Autocommit reads use `read_ts = g.clock` (latest committed).
- **Buffered writes + read-your-own-writes**: a transaction's `SET`/`DEL` go into a per-connection write set; the transaction reads its own pending writes, but no other connection sees them until commit.
- **Atomic commit at one timestamp**: `COMMIT` assigns a single `commit_ts = ++g.clock` and installs every buffered write as a new version, so they all become visible together. The records are WAL-appended and group-committed in the same batch — the durability contract composes with [group commit](#optimization-2--wal-group-commit-the-write-path-unlock).
- **Write-write conflict detection (first-committer-wins)**: at `COMMIT`, if any key in the write set has a head version newer than the transaction's `read_ts`, the transaction aborts. This prevents lost updates — true snapshot isolation, not just atomic batching.
- **Garbage collection**: a `std::multiset` tracks the read timestamps of in-flight transactions; on each write the chain is pruned of versions older than the newest one still visible to the oldest live snapshot, bounding chain length.
- **`DEL` is a tombstone version**, so a concurrent snapshot can still see the pre-delete value; `keys` skips tombstoned entries.
- **`KEYS` is snapshot-consistent**: inside a transaction it reflects the keys visible at `read_ts` with the transaction's own buffered writes overlaid (new keys added, deleted keys hidden), and does *not* show keys committed after the snapshot.
- **All command types are transactional**: `ZADD`/`ZREM`/`PEXPIRE` issued inside a transaction are buffered (they return `QUEUED`) and replayed atomically at `COMMIT` in the same WAL group-commit batch, so a mixed transaction is all-or-nothing.

```text
A: SET k v0
A: BEGIN              # read_ts = 5
A: GET k  → v0
B: SET k v1           # commits at ts 6
A: GET k  → v0        # A still sees its snapshot, not v1
A: COMMIT
A: GET k  → v1        # now sees latest

A: BEGIN; B: BEGIN    # both snapshot the same state
A: SET c A; B: SET c B
A: COMMIT  → OK
B: COMMIT  → ERR write-write conflict; transaction aborted
```

The fast read path stays fast: with no transactions a key has a single version whose `commit_ts <= g.clock`, so the inline-GET path returns the head in O(1) — the common case is unchanged.

**Per-type guarantees (honest scope):**

| | Atomicity at COMMIT | Snapshot-isolated reads | Read-your-own-writes | Write-write conflict detection |
|---|---|---|---|---|
| Strings (`GET`/`SET`/`DEL`) | ✅ | ✅ | ✅ | ✅ |
| Sorted sets / TTL (`ZADD`/`ZREM`/`PEXPIRE`) | ✅ (buffered, replayed at commit) | ❌ reads run live | ❌ | ❌ |

The isolation level is **snapshot isolation**, not serializable — it does not protect against write-skew (no read-set validation / SSI). True snapshot-isolated reads for sorted sets would require versioning the AVL structure (a separate effort). Snapshots do not survive a restart (replay rebuilds the latest committed state). Verified by `test_mvcc` and `test_mvcc_durability` in the integration suite (snapshot isolation, read-your-writes, rollback, write-write conflict, transactional `ZADD`/`DEL`, and snapshot-consistent `KEYS`).

---

## Technologies & Libraries

| Component | Technology | Notes |
|---|---|---|
| Language | C++17 (`-std=gnu++17`) | gnu++ for `typeof` in `container_of` |
| Compiler | `clang++` (macOS) / `g++` (Linux) | auto-detected by Makefile |
| I/O multiplexing | **kqueue** (macOS) / **epoll** (Linux) | edge-triggered (`EV_CLEAR` / `EPOLLET`) |
| Threading | **pthreads** | 4-thread pool + mutex + condvar |
| Persistence | **mmap(MAP_SHARED)** + **ftruncate()** | 4 MB growth quanta, `msync(MS_ASYNC)` on write |
| Durability | **fdatasync()** (Linux) / **fsync()** (macOS) | group commit: one fsync per pipeline batch, before responses are acked |
| Integrity | **CRC-32** (ISO 3309 / Ethernet poly `0xEDB88320`) | rolled by hand, no external library |
| Hash function | **FNV-1** (`str_hash` in common.h) | 64-bit, from-scratch |
| GPU monitoring | **NVML** (`libnvidia-ml`) | optional, graceful fallback when absent |
| Data structures | All **from scratch**: HMap, AVL, ZSet, heap | no STL containers for core DB |
| STL used | `std::vector`, `std::deque`, `std::string`, `std::atomic` | only in server/bench glue code |
| Build | GNU **Make** | single Makefile, no cmake/autoconf |

---

## All Commands

### Build

```bash
# Build everything (kvs-base, kvs, bench — CPU-only GPU profiler)
make all

# Build individual targets
make kvs-base
make kvs
make bench

# Build bench with NVML GPU support (requires nvidia-ml on Linux)
make bench GPU=1

# Clean all binaries and data files
make clean
```

### Run servers

```bash
# Run base server (poll-based, in-memory only)
./kvs-base

# Run full engine (kqueue/epoll + WAL + mmap + seqlock RCU + MT-GET)
./kvs

# v15 startup output:
#   restored N keys from snapshot + WAL
#   listening on :1234  (kqueue/epoll + WAL + mmap + MT-GET)
```

Both servers listen on **port 1234** by default.

### Benchmark

```bash
# Usage: ./bench/bench [host] [port] [ops] [pipeline] [mode]
./bench/bench 127.0.0.1 1234 100000 16

# Defaults: host=127.0.0.1  port=1234  ops=100000  pipeline=16  mode=get

# Quick sanity check (fast)
./bench/bench 127.0.0.1 1234 5000 1

# High-throughput pipeline sweep
./bench/bench 127.0.0.1 1234 100000 64

# Mixed 80/20 GET/SET workload
./bench/bench 127.0.0.1 1234 100000 64 mixed:80

# With GPU profiling enabled (NVIDIA host only)
make bench GPU=1
./bench/bench 127.0.0.1 1234 100000 16

# Multiple concurrent connections (run in parallel)
./bench/bench 127.0.0.1 1234 50000 16 &
./bench/bench 127.0.0.1 1234 50000 16 &
wait
```

### Crash recovery demo

```bash
# Terminal 1: start server
./kvs

# Terminal 2: run benchmark (seeds data via WAL + mmap)
./bench/bench 127.0.0.1 1234 100000 16

# Terminal 1: kill mid-run with Ctrl-C or kill -9 <pid>
kill -9 $(pgrep kvs)

# Terminal 1: restart — observe WAL replay
./kvs
# Output: restored N keys from snapshot + WAL
#         listening on :1234  (kqueue/epoll + WAL + mmap + MT-GET)
```

WAL records are replayed in order with CRC-32 verification. Any partially-written tail record (power-loss mid-write) is truncated automatically.

### Makefile convenience targets

```bash
make run-base     # build + run kvs-base
make run          # build + run kvs
make run-bench    # build + run bench with defaults (127.0.0.1:1234, 100000 ops, pipeline=16)
make test         # build + run the full test suite (unit + integration)
```

---

## Testing

`make test` builds and runs the whole suite; CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs it on every push and PR across Linux (g++) and macOS (clang++). Any failed assertion exits nonzero and fails the build.

| Suite | File | Covers |
|---|---|---|
| Hash table | [`tests/test_hashtable.cpp`](tests/test_hashtable.cpp) | insert / lookup / delete / size; 5 000-key progressive-rehash stress |
| Sorted set | [`tests/test_zset.cpp`](tests/test_zset.cpp) | insert, score update, delete, `seekge` ordering, `znode_offset` arithmetic |
| Write-ahead log | [`tests/test_wal.cpp`](tests/test_wal.cpp) | append/sync group commit, replay, **CRC-mismatch truncation**, **partial-tail recovery**, checkpoint |
| Integration | [`tests/test_integration.py`](tests/test_integration.py) | drives the real `kvs` server over TCP — commands, type errors, `INFO` + group-commit invariant, 200-deep pipelined GET, **MVCC snapshot isolation / read-your-writes / write-write conflict / transactional durability**, and **`SIGKILL` crash recovery (100/100 keys replayed)** |
| Parser fuzzer | [`tests/fuzz_parser.cpp`](tests/fuzz_parser.cpp) | the wire-protocol parser ([`engine/protocol.h`](engine/protocol.h)) under **ASAN + UBSAN** — correctness regressions plus ~50k random/adversarial inputs |

The WAL and crash-recovery tests directly guard the durability contract behind [WAL group commit](#optimization-2--wal-group-commit-the-write-path-unlock): a mutation is acked only after `fdatasync`, and a killed server replays every acked write. The pipelined-GET test guards the [inline-GET fast-path](#optimization-1--inline-get-fast-path-the-throughput-unlock).

```bash
make test
# ── tests/test_hashtable ──   PASSED: 5013 checks, 0 failed
# ── tests/test_zset ──        PASSED: 21 checks, 0 failed
# ── tests/test_wal ──         PASSED: 16 checks, 0 failed
# ── tests/fuzz_parser ──      PASSED: correctness + ~50k fuzz iterations, 0 failed
# ── tests/test_integration ── PASSED: 62 checks, 0 failed
```

### Fuzzing & input hardening

The wire-protocol parser is the server's primary attack surface — it turns raw, untrusted socket bytes into commands — so it's isolated in [`engine/protocol.h`](engine/protocol.h) and fuzzed. [`tests/fuzz_parser.cpp`](tests/fuzz_parser.cpp) builds two ways from one file:

- **Standalone** (default, part of `make test`): a seeded harness feeds correctness cases plus ~50k random and adversarial framings (oversized `nstr`, lying length fields, truncated frames, trailing garbage) to `parse_req` under **AddressSanitizer + UndefinedBehaviorSanitizer**. Portable across clang and g++, so CI runs it on Linux and macOS. *Verified to fail*: planting a deliberate off-by-bounds in `read_str` makes ASan abort immediately.
- **libFuzzer** (`make fuzz-libfuzzer`, where the toolchain ships it): coverage-guided campaign via `LLVMFuzzerTestOneInput`.

This exercise also **hardened the parser**: the bounds checks now compare a length against the *remaining* byte count (`n > (size_t)(end - cur)`) instead of `cur + n > end`, eliminating pointer-arithmetic overflow on adversarial length fields.

---

## Observability

The server exposes runtime metrics via an `INFO` command (Redis-style newline-delimited `key:value` text), so throughput, keyspace size, and the write path can be monitored without a debugger or restart. Counters live on the single event-loop thread, so they're race-free and add no locking to the hot path.

```text
$ redis-style INFO →
uptime_seconds:42
connections_received:8
connections_active:3
commands_processed:1048576
reads:838861
writes:209715
keyspace_keys:50000
wal_records:209715
wal_syncs:16104        ← group commit: ~13× fewer fsyncs than writes
wal_bytes:5242880
```

The `wal_records` vs `wal_syncs` pair makes [WAL group commit](#optimization-2--wal-group-commit-the-write-path-unlock) directly observable: under pipelined writes the fsync count stays far below the record count. The integration suite asserts this invariant (`test_info_group_commit`).

---

## Benchmark Results

All results on Apple Silicon MacBook (macOS 25.0, clang++ with `-O2`). Server and bench on the **same machine over a single loopback TCP connection** — this is single-connection loopback throughput, not networked or multi-client throughput. No GPU present — GPU profiler runs in CPU-only mode.

**Methodology:** Each figure is the **median of 7 runs** of 100 000 ops; min/max shown to bound run-to-run variance. GET-only throughput (the `qps` field) is measured over the GET pass; mixed throughput is measured over the whole interleaved stream. Latency percentiles are per-batch. Raw run logs are committed under [`bench/results/`](bench/results/) so every number here is reproducible from saved output, not reconstructed.

### Pipeline depth scaling (kvs, 100 000 ops, GET-only, single loopback connection)

| Pipeline | Throughput (median of 7) | min–max | p50 | p99 | p99.9 |
|---|---|---|---|---|---|
| 1 | 70,031 ops/s | 69,240–70,365 | 31 µs | 53 µs | 124 µs |
| 16 | 443,685 ops/s | 390,999–445,911 | 135 µs | 242 µs | 656 µs |
| 64 | 836,183 ops/s | 760,688–837,563 | 367 µs | 537 µs | 3,605 µs |

**Interpretation:** Pipelining is the dominant throughput lever — depth 16 gives ~6.3× over depth 1 by amortising the per-request syscall round-trip; depth 64 adds another ~1.9×. Throughput keeps climbing with depth because reads now execute inline on the event-loop thread (see optimization #1 below), so a pipelined batch drains in a single pass with no per-request dispatch.

### Optimization #1 — inline GET fast-path (the throughput unlock)

The original `kvs` dispatched **every** GET to the thread pool and *suspended the connection* until a worker posted the result (`dispatch_get` → `worker_get_func` → `wakeup_pipe` → `drain_results`). On a pipelined connection that serialises the pipeline to **one read in flight at a time** — the round-trip dominates and deep pipelines buy nothing. `try_one_request` now executes GET **inline under the same seqlock read section** the worker used, so the whole pipeline batch drains in one pass. (`dispatch_get`/`worker_get_func` are retained for the offload path and WAL replay.)

| pipeline=64 GET-only | Before (dispatch) | After (inline) | Speedup |
|---|---|---|---|
| Throughput (median of 7) | 129,997 ops/s | **836,183 ops/s** | **6.4×** |

Correctness is unchanged: the inline read uses the same `rcu_read_begin` / `rcu_read_retry` seqlock protocol against the single-threaded writer. Raw logs: [`bench/results/pipeline64_get_optimized.csv`](bench/results/pipeline64_get_optimized.csv) (before: [`pipeline64_get.csv`](bench/results/pipeline64_get.csv)).

### Optimization #2 — WAL group commit (the write-path unlock)

The original WAL called `fdatasync()` on **every** mutation. Under a mixed workload the per-write fsync dominates. `wal_write` is now split into `wal_append` (buffer the record, no fsync) + `wal_sync` (one `fdatasync` per pipeline batch), called once in `handle_read` **before any response is flushed** — so a write is still acked only after it is durable. Same durability contract, ~13× fewer fsyncs per pipeline=64 batch.

Crash-recovery verified: 50 acked SETs → `kill -9` → restart → **50/50 keys recovered** via WAL replay (CRC-checked, partial-tail truncation intact).

### Mixed 80/20 read/write workload (kvs, 100 000 ops, pipeline=64, single loopback connection)

Interleaved GET/SET on one pipelined connection at an 80% read / 20% write ratio, measured over the **whole mixed stream** (includes WAL append + group-commit fsync on the 20% writes). The op sequence uses a fixed seed (`srand(12345)`) so the ratio and stream are reproducible run-to-run.

| Workload | Before (dispatch + per-write fsync) | After (inline GET + group commit) | Speedup |
|---|---|---|---|
| 80% GET / 20% SET | 117,063 ops/s | **396,336 ops/s** | **3.4×** |

After-state spread: median 396,336, min 385,043, max 403,367 (median of 7); p50 157 µs, p99 ~290 µs. Actual split ~79,900 GET / ~20,100 SET. Run as:

```bash
./bench/bench 127.0.0.1 1234 100000 64 mixed:80
```

Raw logs: [`bench/results/mixed_80_20_optimized.csv`](bench/results/mixed_80_20_optimized.csv) (before: [`mixed_80_20.csv`](bench/results/mixed_80_20.csv)).

### Concurrent connections (kvs, 5 000 ops each, pipeline=16)

| Connections | Throughput each | p50 | p99 |
|---|---|---|---|
| 1 | 514,456 ops/s | 189 µs | 570 µs |
| 2 | 506,637 ops/s (conn1), 494,609 ops/s (conn2) | ~223 µs | ~447 µs |

Two concurrent clients with separate connections each achieve near-identical throughput, confirming the accept-loop fix and per-connection ordering invariants work correctly.

### GPU correlation CSV format

When GPU profiling is active (`make bench GPU=1` on a CUDA host), the bench emits a second CSV to stdout after the summary:

```
timestamp_ms,ops_in_window,gpu_util_pct,mem_used_mb,power_watts,p99_us_in_window
1269437870,2619,47,8192,180.5,28
1269437975,4616,52,8192,185.1,29
1269438077,4698,55,8256,191.0,28
...
```

Each row is a 100 ms window. `ops_in_window` = number of GET batches completed. `p99_us_in_window` = p99 batch latency within that window. Useful for correlating GPU memory pressure with tail latency spikes.

In CPU-only mode (no NVIDIA GPU), the CSV is still emitted with zero GPU columns, allowing the latency correlation to be used independently.

---

## Wire Protocol

Custom binary framing (little-endian field widths, tag-prefixed responses):

```
Request:   [total_body_len : 4 bytes] [nstr : 4 bytes] [len0 : 4 bytes] [str0] ...
Response:  [total_body_len : 4 bytes] [tag : 1 byte]   [payload ...]

Tags: 0=nil  1=err  2=str  3=int  4=dbl  5=arr
```

### Supported commands

| Command | Args | Returns |
|---|---|---|
| `get <key>` | 2 | str or nil |
| `set <key> <val>` | 3 | nil |
| `del <key>` | 2 | int (0/1) |
| `keys` | 1 | arr of str |
| `pexpire <key> <ms>` | 3 | int (0/1) |
| `pttl <key>` | 2 | int (ms, -1=no TTL, -2=missing) |
| `zadd <key> <score> <member>` | 4 | int (0/1 added) |
| `zrem <key> <member>` | 3 | int (0/1) |
| `zscore <key> <member>` | 3 | dbl or nil |
| `zquery <key> <score> <name> <offset> <limit>` | 6 | arr |
| `info` | 1 | str (server metrics) |
| `begin` | 1 | str (`OK`) — start an MVCC transaction |
| `commit` | 1 | str (`OK`) or err on write-write conflict |
| `rollback` | 1 | str (`OK`) — discard buffered writes |

---

## Data Structures (all from scratch)

### HMap — Progressive-rehashing hash table

```
HMap
├── HTab ht1   (active table)
└── HTab ht2   (resize target, populated incrementally)

Each rehash step moves 128 nodes.
Load factor threshold: k_max_load_factor = 8
Resize trigger: size > capacity * 8
```

### ZSet — Sorted set with dual index

```
ZSet
├── AVL tree   (ordered by score, then name — for range queries)
└── HMap       (ordered by name — for O(1) point lookups)

Both indexes point into the same ZNode allocation (zero-copy).
ZNode uses a flexible array member: char name[0].
```

### Heap — TTL expiry

```
std::vector<HeapItem>   min-heap by expiry timestamp
HeapItem { val: uint64_t, ref: size_t* }
ref points back into Entry::heap_idx for O(1) heap updates on SET/DEL.
```

---

## Bugs Found and Fixed

| # | Bug | Symptom | Fix |
|---|---|---|---|
| 1 | `container_of` uses GNU `typeof` | Clang C++17 rejects `typeof` | Changed `-std=c++17` → `-std=gnu++17` |
| 2 | `fdatasync` undefined on macOS | Compile error in wal.cpp | `#define fdatasync(fd) fsync(fd)` on `__APPLE__` |
| 3 | VLA in kqueue branch of event_loop.cpp | Clang VLA extension warning/error | Changed `struct kevent evs[maxevents]` to fixed `const int k_max=1024` |
| 4 | `dispatch_get` didn't reset `want_read=false` | Event loop re-enabled reads while GET in-flight; pipelining broke | Added `conn->want_read = false; conn->want_write = false` in `dispatch_get` |
| 5 | `drain_results` didn't drive request loop | Pipelined GETs stalled in edge-triggered mode (socket buffer empty, no EV_READ re-fired) | Added `while (try_one_request(conn)) {}` inside `drain_results` |
| 6 | `handle_write` reset `want_read=true` after GET dispatch | Concurrent connections could dispatch two GETs simultaneously, breaking ordering | After `handle_write` in `drain_results`, check `get_pending` and restore `want_read=false` |
| 7 | `handle_accept` called `accept()` only once | Second simultaneous connection stuck in backlog forever (classic ET bug) | Wrapped `accept()` in `while(true)` loop, break on `EAGAIN` |

---

## What's Remaining

### 1. Crash recovery demo (5 minutes to run)

The WAL is fully implemented. To demonstrate:

```bash
# Seed data
./kvs &
./bench/bench 127.0.0.1 1234 50000 16

# Kill mid-run
kill -9 $(pgrep kvs)

# Restart and observe replay
./kvs
# Expected: "restored N keys from snapshot + WAL"
```

### 2. Mixed workload benchmark — **implemented**

`bench.cpp` now supports an interleaved GET/SET mode (`run_bench_mixed`) at a configurable read ratio, in addition to the original SET-then-GET pass. See the [Mixed 80/20 read/write workload](#mixed-8020-readwrite-workload-kvs-100-000-ops-pipeline64-single-loopback-connection) results above.

```bash
./bench/bench 127.0.0.1 1234 100000 64 mixed:80   # 80% GET / 20% SET
```

### 3. Large connection-count sweep (100 / 500 / 1000 conns)

Requires a benchmarking harness that spawns N bench processes. Server supports it — the accept-loop fix (bug #7) handles burst arrivals correctly.

```bash
# Current manual approach (works):
for i in $(seq 1 10); do
  ./bench/bench 127.0.0.1 1234 10000 16 &
done
wait
```

### 4. GPU correlation on NVIDIA host

Build with `make bench GPU=1` and run on a Linux machine with an NVIDIA GPU. The profiler attaches to GPU 0 via NVML, samples every 100 ms, and emits:

```
timestamp_ms,ops_in_window,gpu_util_pct,mem_used_mb,power_watts,p99_us_in_window
```

### 5. WAL group commit — **implemented**

`fdatasync()` was previously called on every single write. It is now batched: `wal_append` buffers records and `wal_sync` issues one `fdatasync` per pipeline batch (in `handle_read`, before any response is flushed). This lifted the 80/20 mixed workload from 117k → **396k ops/s** with the same durability contract. See [Optimization #2](#optimization-2--wal-group-commit-the-write-path-unlock).

### 6. Seqlock RCU — version-copy safety

String values now live in immutable [MVCC version chains](#mvcc--multi-version-concurrency-control--transactions): a write *prepends* a new `Version` rather than mutating an existing `std::string` in place, which removes the original reader/writer copy race on the value itself. A residual concern remains around concurrent pruning of old versions if reads are ever moved back onto worker threads; with reads inline on the single event-loop thread today there is no live data race. A production fix for the offload path would be epoch-based reclamation or hazard pointers. Documented as an accepted tradeoff.

---

## Production Roadmap (what would make this production-grade)

This is a from-scratch **single-node storage engine** that implements production *techniques* (WAL, group commit, crash recovery, seqlock reads, MVCC snapshot isolation, fuzzed parser, observability) — not a production database. The gap to a production system, in rough priority order:

### 1. Distribution — replication, failover, sharding
The single biggest gap. Real distributed databases replicate the WAL to followers (leader/follower log shipping), elect a new leader on failure (Raft/Paxos), and shard the keyspace across nodes. A natural first step here is async leader→follower WAL streaming, since the WAL already exists.

### 2. Multi-core — sharded event loops (thread-per-core)
Today the whole server runs on **one event-loop thread**, so it saturates a single core. A thread-per-core model (à la ScyllaDB/Seastar or Redis-per-core) would shard connections/keys across N event loops. This is the largest single-node performance lever left; it interacts with the MVCC/seqlock model, so it's a deliberate architectural change, not an add-on.

### 3. Security & operability
The commodity "make it deployable" work, mostly independent and incremental:
- **Auth** (e.g. `AUTH`/password or token) and **TLS** for the wire protocol.
- **Max-memory + eviction policy** (LRU/LFU/random) so the server is bounded under memory pressure.
- **Connection limits / backpressure / per-request quotas** to survive abusive clients.
- **Config file / CLI flags** (currently port and paths are hard-coded to `:1234`, `redis.dat`, `redis.wal`).
- **Graceful shutdown & signal handling** — flush WAL, checkpoint, close connections on `SIGTERM`.
- **Structured logging** with levels, and **metrics export** (Prometheus/OpenMetrics scrape endpoint building on the existing `INFO` counters).

### 4. Stronger isolation & full MVCC coverage
- **Serializable isolation (SSI)** — current level is [snapshot isolation](#per-type-guarantees-honest-scope), which permits write-skew. SSI needs read-set tracking / predicate validation at commit.
- **Versioned sorted sets** — `ZADD`/`ZREM` are transactional for *atomicity* but not snapshot-isolated for reads, because the AVL structure isn't versioned. True SI for zsets means MVCC per member.

### 5. Version & tombstone garbage collection
Old *versions* are pruned, but a fully-deleted key's tombstone `Entry` lingers in the hash map forever. A background/opportunistic sweep should physically remove entries whose newest version is a tombstone older than the oldest live snapshot, reclaiming that memory.

### 6. Hardening the concurrent-read path
If reads are ever moved back onto worker threads (offload), version-chain pruning needs epoch-based reclamation or hazard pointers to be safe against concurrent readers (see [#6 above](#6-seqlock-rcu--version-copy-safety)).

---

## Project Impact

| Dimension | v14 (first gen) | v15 (production gen) |
|---|---|---|
| Event model | `poll()` — O(N fds) | kqueue/epoll — O(active events) |
| Crash safety | None (all data lost) | WAL + CRC-32 + partial-write recovery |
| Persistence | None | mmap snapshot + WAL replay |
| Concurrent reads | Blocked by writes | Seqlock RCU — readers run in parallel |
| Accept bottleneck | One `accept()` per EV_READ | Drain loop — handles burst arrivals |
| Observability | None | p50/p99/p999 + GPU correlation CSV + `INFO` metrics |
| Transactions | None | **MVCC snapshot isolation** — `BEGIN`/`COMMIT`/`ROLLBACK`, write-write conflict detection |
| Commands | SET / GET / DEL / ZADD / etc. | Same + WAL-logged + snapshot-backed |

**Throughput headroom:** At pipeline=64, `kvs` reaches ~836k GET ops/s (and ~396k ops/s under an 80/20 mixed workload) on a single loopback connection on a laptop — median of 7 runs, see [Benchmark Results](#benchmark-results). These are single-connection loopback figures, not networked throughput; they rose 6.4× / 3.4× after the inline-GET and WAL group-commit optimizations. On a server-class machine with real network and many connections, the kqueue/epoll model eliminates the O(N) poll bottleneck and scales across connections rather than relying on deeper pipelining of one connection.

---

## Build Requirements

| Platform | Compiler | Libraries |
|---|---|---|
| macOS 12+ | clang++ (Xcode CLI tools) | none beyond libc |
| Linux (Ubuntu 20+) | g++ or clang++ | `-lpthread -lrt -lm` |
| GPU profiling | Any of the above | CUDA toolkit + `libnvidia-ml` |

Install on macOS:
```bash
xcode-select --install
```

Install on Ubuntu:
```bash
sudo apt install build-essential
# For GPU profiling:
sudo apt install nvidia-cuda-toolkit
```

---

## Quick Start

```bash
# 1. Build
make all

# 2. Start server
./kvs

# 3. In another terminal — run benchmark
./bench/bench 127.0.0.1 1234 100000 16

# 4. Stop server (Ctrl-C), restart — see WAL replay
./kvs
```

Expected server startup output:
```
restored 1 keys from snapshot + WAL
listening on :1234  (kqueue/epoll + WAL + mmap + MT-GET)
```

Expected bench output:
```
ops,pipeline,total_ms,qps,p50_us,p99_us,p999_us
100000,16,225.4,443685,135,242,656
timestamp_ms,ops_in_window,gpu_util_pct,mem_used_mb,power_watts,p99_us_in_window
...
```
