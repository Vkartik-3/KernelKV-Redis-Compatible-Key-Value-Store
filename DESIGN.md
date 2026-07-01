# KernelKV — Design & Feature Reference

A from-scratch, single-node Redis-compatible storage engine in C++17 that
implements production database internals: edge-triggered I/O, write-ahead
logging with group commit, mmap persistence, seqlock reads, MVCC
snapshot-isolation transactions, a fuzzed wire protocol, and a full operability
layer. This document is the deep reference for each subsystem — what it does,
how it works, and the honest limits.

## Tech stack

**C++17** (`-std=gnu++17`), **Python** (integration tests), **GNU Make**,
**GitHub Actions CI** (Linux g++ + macOS clang++), **AddressSanitizer/UBSan**,
**libFuzzer**, **Prometheus**, **stunnel**.

## Headline numbers (single loopback connection, Apple M3, median of 7 runs)

| Metric | Result |
|---|---|
| GET @ pipeline=64 | **836,183 ops/s** (6.4× via inline-GET fast-path) |
| 80/20 mixed @ pipeline=64 | **396,336 ops/s** (3.4× via WAL group commit) |
| GET @ pipeline=16 | 443,685 ops/s |
| 2 concurrent connections | ~1M ops/s aggregate |
| Test coverage | ~5,100 unit assertions + ~50k fuzz iterations + 74 integration checks |

> **Honest scope:** these are single-connection loopback numbers — a real TCP
> round trip through the kernel, but no physical network latency and not
> multi-client. State this before it's asked.

---

## Two core optimizations

### Inline-GET fast-path — 6.4× (130K → 836K ops/s)
The server dispatched every GET to a thread pool and *suspended the connection*
until a worker replied, serializing pipelined reads to one in-flight at a time.
`try_one_request` now executes GET **inline under the seqlock read section**, so
a pipelined batch drains in one pass. Same correctness model; `dispatch_get`/
`worker_get_func` retained for the offload path.

### WAL group commit — 3.4× on mixed (117K → 396K ops/s)
`wal_write` was split into `wal_append` (buffer, no fsync) + `wal_sync` (one
`fdatasync` per pipeline batch), called in `handle_read` **before responses are
flushed** — so a write is acked only after it's durable. ~13× fewer fsyncs per
batch. Composes with MVCC commit (a transaction's writes fsync together).

---

## 1. Observability

**What:** live metrics via an in-band `INFO` command (Redis `key:value`) and an
out-of-band Prometheus `/metrics` HTTP endpoint (`--metrics-port`).

**How:** plain `uint64_t` counters in `g` and `WAL` — no atomics/locks because
all commands run on the single event-loop thread (race-free, zero hot-path
overhead). `fill_metrics(buf, cap, prom)` renders one table two ways. Counters:
uptime, connections_received/active, commands_processed, reads, writes,
keyspace_keys, memory_used_bytes, maxmemory_bytes, evicted_keys, expired_keys,
gc_reaped_tombstones, wal_records, wal_syncs, wal_bytes.

**Clever bit:** `wal_records` vs `wal_syncs` makes group commit observable
(~13× fewer fsyncs); `test_info_group_commit` asserts `syncs < records`.

**Keywords:** observability, runtime metrics, telemetry, Prometheus/OpenMetrics,
text-exposition format, lock-free counters, zero-overhead instrumentation.

**Limit:** counters are process-local; no exported histograms.

---

## 2. Fuzzing + input hardening

**What:** the wire-protocol parser (primary attack surface) isolated and fuzzed
under sanitizers.

**How:** `read_u32`/`read_str`/`parse_req` extracted into `engine/protocol.h` as
a pure function over bytes. `tests/fuzz_parser.cpp` builds two ways:
- **Standalone** (default, in `make test` + CI): seeded, deterministic; ~50k
  correctness + adversarial + random inputs under **ASan + UBSan**; portable
  across clang and g++.
- **libFuzzer** (`-DUSE_LIBFUZZER`): coverage-guided `LLVMFuzzerTestOneInput`
  (Apple clang ships no libFuzzer, hence the portable standalone default).

**Hardening fix:** bounds checks changed from `cur + n > end` to
`n > (size_t)(end - cur)` — compares length to remaining bytes, eliminating
pointer-arithmetic overflow (UB) on adversarial lengths. Verified by planting an
off-by-bounds bug → ASan aborted → reverted.

**Keywords:** fuzzing, libFuzzer, coverage-guided fuzzing, AddressSanitizer,
UndefinedBehaviorSanitizer, attack surface, untrusted input, memory safety,
pointer overflow / OOB, defensive programming, property-based testing.

**Limit:** fuzzes the parser only; standalone mode is random+adversarial, not
coverage-guided.

---

## 3. MVCC / transactions

**What:** multi-version concurrency control with snapshot-isolation
transactions (`BEGIN`/`COMMIT`/`ROLLBACK`).

**How:**
- **Version chains:** each string key holds `Version *vhead` (newest-first list
  of `Version{commit_ts, deleted, value, next}`); writes prepend (immutable).
- **Commit clock:** monotonic `g.clock` assigns a timestamp per commit.
- **Snapshot reads:** a txn fixes `read_ts = g.clock` at BEGIN; `mvcc_visible`
  returns the newest version with `commit_ts <= read_ts`.
- **Per-connection `Txn`:** `read_ts`, `writes` (string write set), `deferred`
  (buffered zset/TTL commands).
- **Read-your-own-writes**, **atomic commit at one commit_ts** (WAL-grouped),
  **write-write conflict detection** (first-committer-wins → true SI).
- **GC:** `multiset<uint64_t> active_snaps` bounds version-chain length.
- **Tombstone deletes**, **snapshot-consistent KEYS**, all command types
  transactional (zset/TTL buffer as QUEUED, apply at commit).
- **Fast path preserved:** no-txn key = single version, inline-GET returns head
  in O(1).

**Per-type guarantees:**

| | Atomic at COMMIT | Snapshot reads | RYOW | Conflict detection |
|---|---|---|---|---|
| Strings (GET/SET/DEL) | ✅ | ✅ | ✅ | ✅ |
| ZSet / TTL | ✅ (buffered) | ❌ live | ❌ | ❌ |

**Keywords:** MVCC, snapshot isolation, version chains, commit timestamp /
logical clock, write-write conflict detection, first-committer-wins,
read-your-own-writes, optimistic concurrency control, atomic commit, tombstone,
ACID atomicity/isolation.

**Limits:** snapshot isolation (not serializable — write-skew possible); zsets/
TTLs not versioned; snapshots don't survive restart; single-node/single-thread.

**Verified:** `test_mvcc`, `test_mvcc_durability`.

---

## 4. Operability layer

| Feature | How | Keywords |
|---|---|---|
| **Config (CLI + env)** | `struct Config cfg`; `parse_config` reads `KVS_*` env then `--flag value` | 12-factor config, CLI parsing, env vars |
| **Structured logging** | `log_msg` — `va_list`/`vfprintf`, ms timestamps, level-filtered; `LOG_D/I/W/E` | structured logging, log levels |
| **Graceful shutdown** | async-signal-safe handler sets `sig_atomic_t g_stop`; loop `while(!g_stop)`; flush + checkpoint on exit; SIGPIPE ignored | signal handling, SIGTERM/SIGINT, async-signal-safe, EINTR |
| **Connection limits** | `handle_accept` rejects beyond `--maxclients` | backpressure, admission control |
| **AUTH** | `Conn.authenticated`; gate in `try_one_request` before dispatch (covers inline GET); `NOAUTH` until authed | authentication, access control |
| **Max-memory + LRU** | intrusive `DList lru_node` (O(1)); `maybe_evict` from LRU end; MVCC-safe; reads don't touch LRU | LRU eviction, cache policy, memory management |
| **Tombstone GC** | `gc_reapable` (head deleted, `commit_ts <= min_active`); throttled ~1/s, bounded sweep in `process_timers` | garbage collection, tombstone reclamation |
| **Prometheus `/metrics`** | 2nd listener; `serve_metrics` speaks minimal HTTP/1.1 + text-exposition | Prometheus, OpenMetrics, HTTP endpoint |
| **TLS** | **in-process TLS 1.3** (OpenSSL, `make kvs TLS=1`): `conn_recv`/`conn_send` wrap `SSL_read`/`SSL_write`, non-blocking handshake via `tls_advance_handshake`, TLS 1.3 pinned to avoid the WANT_READ/WANT_WRITE inversion; also stunnel sidecar | OpenSSL, TLS 1.3, SSL_read/SSL_write, non-blocking TLS handshake, X.509, encryption in transit |

**Verified:** `test_auth`, `test_graceful_shutdown`, `test_maxmemory_eviction`,
`test_metrics_endpoint`.

---

## Testing & CI

`make test` runs unit tests (hash table, sorted set, WAL), the parser fuzzer
(ASan/UBSan), and the Python integration suite (real server over TCP, SIGKILL
crash recovery, MVCC across two connections, operability). CI runs it on Linux
(g++) and macOS (clang++) on every push and PR.

- Hash table: 5,013 assertions (incl. 5,000-key progressive-rehash stress)
- Sorted set: 21 · WAL: 16 · fuzzer: ~50k iterations · integration: 74 checks

---

## Production roadmap (honest gaps)

Not a production database — a single-node engine implementing production
*techniques*. Remaining, in priority order: **distribution** (replication/
failover/sharding), **multi-core** (thread-per-core; rearchitects the
single-thread invariant everything relies on), **serializable isolation (SSI)**
+ **versioned sorted sets**, and incremental cursor-based GC. Everything under
"Security & operability" — including in-process TLS 1.3 — is implemented.
