// stdlib
#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <math.h>
// system
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <signal.h>
#include <stdarg.h>
// C++
#include <string>
#include <vector>
#include <atomic>
#include <deque>
#include <map>
#include <set>
#include <unordered_set>
// proj — shared core modules
#include "../core/common.h"
#include "../core/hashtable.h"
#include "../core/zset.h"
#include "../core/list.h"
#include "../core/heap.h"
#include "../core/thread_pool.h"
// new modules
#include "event_loop.h"
#include "protocol.h"
#include "wal.h"
#include "mmap_store.h"
#include "rcu.h"


// ── Utilities ─────────────────────────────────────────────────────────────────

static void msg_errno(const char *m) { fprintf(stderr, "[errno:%d] %s\n", errno, m); }
static void die(const char *m)       { fprintf(stderr, "[%d] %s\n", errno, m); abort(); }

static uint64_t get_monotonic_msec() {
    struct timespec tv = {0, 0};
    clock_gettime(CLOCK_MONOTONIC, &tv);
    return (uint64_t)tv.tv_sec * 1000u + (uint64_t)tv.tv_nsec / 1000000u;
}

static void fd_set_nb(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) die("fcntl F_GETFL");
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) die("fcntl F_SETFL");
}

// ── Configuration (CLI flags + env vars) ────────────────────────────────────

static struct Config {
    int         port         = 1234;
    std::string data_dir     = ".";     // directory for redis.dat / redis.wal
    std::string requirepass  = "";      // empty = auth disabled
    uint64_t    maxmemory    = 0;       // bytes; 0 = unlimited
    int         max_clients  = 10000;   // reject connections beyond this
    int         metrics_port = 0;       // Prometheus /metrics; 0 = disabled
    int         log_level    = 1;       // 0=debug 1=info 2=warn 3=error
} cfg;

// ── Structured logging ──────────────────────────────────────────────────────

enum { LOG_DEBUG = 0, LOG_INFO = 1, LOG_WARN = 2, LOG_ERROR = 3 };
static const char *k_log_names[] = {"DEBUG", "INFO", "WARN", "ERROR"};

static void log_msg(int level, const char *fmt, ...) {
    if (level < cfg.log_level) return;
    struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts);
    struct tm tm; localtime_r(&ts.tv_sec, &tm);
    char when[32];
    strftime(when, sizeof(when), "%Y-%m-%d %H:%M:%S", &tm);
    fprintf(stderr, "%s.%03ld [%s] ", when, ts.tv_nsec / 1000000, k_log_names[level]);
    va_list ap; va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fputc('\n', stderr);
}

#define LOG_D(...) log_msg(LOG_DEBUG, __VA_ARGS__)
#define LOG_I(...) log_msg(LOG_INFO,  __VA_ARGS__)
#define LOG_W(...) log_msg(LOG_WARN,  __VA_ARGS__)
#define LOG_E(...) log_msg(LOG_ERROR, __VA_ARGS__)

// Parse `--flag value` pairs (with KVS_* env fallbacks). Unknown flags warn.
static void parse_config(int argc, char **argv) {
    auto env = [](const char *k) -> const char* { return getenv(k); };
    if (const char *v = env("KVS_PORT"))        cfg.port         = atoi(v);
    if (const char *v = env("KVS_DIR"))         cfg.data_dir     = v;
    if (const char *v = env("KVS_REQUIREPASS")) cfg.requirepass  = v;
    if (const char *v = env("KVS_MAXMEMORY"))   cfg.maxmemory    = strtoull(v,0,10);
    if (const char *v = env("KVS_MAXCLIENTS"))  cfg.max_clients  = atoi(v);
    if (const char *v = env("KVS_METRICS_PORT"))cfg.metrics_port = atoi(v);
    if (const char *v = env("KVS_LOGLEVEL"))    cfg.log_level    = atoi(v);
    for (int i = 1; i + 1 < argc; i += 2) {
        std::string k = argv[i], val = argv[i+1];
        if      (k=="--port")         cfg.port         = atoi(val.c_str());
        else if (k=="--dir")          cfg.data_dir     = val;
        else if (k=="--requirepass")  cfg.requirepass  = val;
        else if (k=="--maxmemory")    cfg.maxmemory    = strtoull(val.c_str(),0,10);
        else if (k=="--maxclients")   cfg.max_clients  = atoi(val.c_str());
        else if (k=="--metrics-port") cfg.metrics_port = atoi(val.c_str());
        else if (k=="--loglevel")     cfg.log_level    = atoi(val.c_str());
        else LOG_W("unknown flag %s (ignored)", k.c_str());
    }
}

// Set by the signal handler; the event loop exits its loop when it flips.
static volatile sig_atomic_t g_stop = 0;
static void on_signal(int sig) { (void)sig; g_stop = 1; }

const size_t k_max_msg  = 32 << 20;
// k_max_args + read_u32/read_str/parse_req live in engine/protocol.h so the
// parser can be unit tested and fuzzed in isolation.


// ── Buffer ────────────────────────────────────────────────────────────────────

typedef std::vector<uint8_t> Buffer;

static void buf_append(Buffer &buf, const uint8_t *data, size_t len) {
    buf.insert(buf.end(), data, data + len);
}
static void buf_consume(Buffer &buf, size_t n) {
    buf.erase(buf.begin(), buf.begin() + (ptrdiff_t)n);
}


// ── Serialisation type tags ───────────────────────────────────────────────────

enum { TAG_NIL=0, TAG_ERR=1, TAG_STR=2, TAG_INT=3, TAG_DBL=4, TAG_ARR=5 };
enum { ERR_UNKNOWN=1, ERR_TOO_BIG=2, ERR_BAD_TYP=3, ERR_BAD_ARG=4 };

static void buf_append_u8 (Buffer &b, uint8_t  v) { b.push_back(v); }
static void buf_append_u32(Buffer &b, uint32_t v) { buf_append(b,(const uint8_t*)&v,4); }
static void buf_append_i64(Buffer &b, int64_t  v) { buf_append(b,(const uint8_t*)&v,8); }
static void buf_append_dbl(Buffer &b, double   v) { buf_append(b,(const uint8_t*)&v,8); }

static void out_nil(Buffer &o) { buf_append_u8(o,TAG_NIL); }
static void out_str(Buffer &o, const char *s, size_t n) {
    buf_append_u8(o,TAG_STR); buf_append_u32(o,(uint32_t)n);
    buf_append(o,(const uint8_t*)s,n);
}
static void out_int(Buffer &o, int64_t v)  { buf_append_u8(o,TAG_INT); buf_append_i64(o,v); }
static void out_dbl(Buffer &o, double  v)  { buf_append_u8(o,TAG_DBL); buf_append_dbl(o,v); }
static void out_err(Buffer &o, uint32_t code, const std::string &msg) {
    buf_append_u8(o,TAG_ERR); buf_append_u32(o,code);
    buf_append_u32(o,(uint32_t)msg.size());
    buf_append(o,(const uint8_t*)msg.data(),msg.size());
}
static void out_arr(Buffer &o, uint32_t n) { buf_append_u8(o,TAG_ARR); buf_append_u32(o,n); }
static size_t out_begin_arr(Buffer &o) {
    o.push_back(TAG_ARR); buf_append_u32(o,0);
    return o.size()-4;
}
static void out_end_arr(Buffer &o, size_t ctx, uint32_t n) {
    assert(o[ctx-1]==TAG_ARR); memcpy(&o[ctx],&n,4);
}

// read_u32 / read_str / parse_req are defined in engine/protocol.h.


// ── Dispatch structs (declared early — used by Conn, g, worker functions) ─────

// Passed to the thread pool worker for each dispatched GET.
struct GetTask {
    int         fd;     // which connection fd (for posting the result back)
    std::string key;    // copy of the key — safe to read from worker thread
    uint64_t    hcode;  // precomputed FNV hash
};

// Written by the worker thread, read by the event loop in drain_results().
struct GetResult {
    int    fd;          // which connection fd
    Buffer response;    // fully-framed response: [4-byte len][body]
};


// ── Connection ────────────────────────────────────────────────────────────────

// ── MVCC transaction state (per connection) ─────────────────────────────────
// A transaction reads as of a fixed snapshot (read_ts) and buffers its writes
// until COMMIT, which applies them atomically at a single new commit timestamp
// (after a write-write conflict check — snapshot isolation, first-committer-wins).
struct TxnWrite {
    bool        deleted = false;   // true = DEL, false = SET
    std::string value;
};
struct Txn {
    bool                            active  = false;
    uint64_t                        read_ts = 0;
    std::map<std::string, TxnWrite> writes;     // string keyspace (MVCC)
    // Non-string mutations (zadd/zrem/pexpire) are buffered as raw commands and
    // replayed atomically at COMMIT — they get all-or-nothing atomicity, but
    // not snapshot-isolated reads (sorted sets / TTLs are not versioned).
    std::vector<std::vector<std::string>> deferred;
};

struct Conn {
    int  fd         = -1;
    bool want_read  = false;
    bool want_write = false;
    bool want_close = false;
    Buffer incoming;
    Buffer outgoing;
    uint64_t last_active_ms = 0;
    DList    idle_node;
    bool     authenticated  = true;   // false until AUTH when requirepass set
    Txn      txn;     // MVCC transaction state (inactive unless BEGIN issued)
    // True while a worker thread holds a GetTask for this fd.
    // conn_destroy() defers actual cleanup until the worker posts its result.
    std::atomic<bool> get_pending{false};
};


// ── KV entry ─────────────────────────────────────────────────────────────────

enum { T_INIT=0, T_STR=1, T_ZSET=2 };

// ── MVCC version chain ──────────────────────────────────────────────────────
// Each string key owns a chain of versions, newest first. A read at snapshot
// timestamp R sees the newest version whose commit_ts <= R. SET prepends a new
// version; DEL prepends a tombstone. Versions no longer visible to any active
// snapshot are pruned (see mvcc_prune). ZSets are not versioned (documented).
struct Version {
    uint64_t    commit_ts = 0;
    bool        deleted   = false;
    std::string value;
    Version    *next      = nullptr;
};

struct Entry {
    HNode   node;
    std::string key;
    size_t  heap_idx = (size_t)-1;
    uint32_t type    = 0;
    Version *vhead   = nullptr;   // T_STR: version chain (newest first)
    ZSet    zset;                 // T_ZSET
    DList   lru_node;             // position in g.lru_list (T_STR only)
    bool    in_lru   = false;
    uint64_t mem     = 0;         // approx bytes accounted for this entry
};

struct LookupKey { HNode node; std::string key; };


// ── Global state ──────────────────────────────────────────────────────────────

static struct {
    HMap     db;
    std::vector<Conn *> fd2conn;
    DList    idle_list;
    std::vector<HeapItem> heap;
    TheadPool thread_pool;

    EventLoop  el;
    WAL        wal;
    MMapStore  mstore;
    SeqLock    rcu;     // seqlock: protects HMap for concurrent worker readers

    bool     replaying = false;
    int      listen_fd = -1;
    uint64_t mutations_since_ckpt = 0;

    // ── Observability counters ─────────────────────────────────────────────
    // All commands run on the single event-loop thread, so plain (non-atomic)
    // counters are race-free. Exposed via the INFO command.
    uint64_t stat_start_ms       = 0;   // server start (monotonic ms)
    uint64_t stat_conns_total    = 0;   // connections accepted since start
    uint64_t stat_conns_active   = 0;   // currently open connections
    uint64_t stat_commands_total = 0;   // requests processed (live, not replay)
    uint64_t stat_reads          = 0;   // non-mutating commands
    uint64_t stat_writes         = 0;   // mutating commands

    // ── Multithreaded GET dispatch ─────────────────────────────────────────
    // Worker threads post GetResult* here, then write 1 byte to wakeup_pipe[1].
    // The event loop reads wakeup_pipe[0] and drains result_queue.
    int             wakeup_pipe[2]  = {-1, -1};
    pthread_mutex_t result_mu;                   // protects result_queue
    std::deque<GetResult *> result_queue;

    // ── MVCC ───────────────────────────────────────────────────────────────
    uint64_t                 clock = 0;        // last assigned commit timestamp
    std::multiset<uint64_t>  active_snaps;     // read_ts of in-flight transactions

    // ── Operability ────────────────────────────────────────────────────────
    int      metrics_fd = -1;      // Prometheus listener (-1 = disabled)
    DList    lru_list;             // string entries in LRU order (front = MRU)
    uint64_t mem_used   = 0;       // approx bytes of live string key+value data
    uint64_t stat_evictions = 0;   // keys evicted under maxmemory
    uint64_t stat_expired   = 0;   // keys removed by TTL
    uint64_t stat_gc_reaped = 0;   // tombstone entries reclaimed by GC
} g;

static const uint64_t k_ckpt_ops = 1000;


// ── MVCC version-chain helpers ──────────────────────────────────────────────

// Oldest snapshot anyone could still read with; versions older than the newest
// version visible to it can never be seen again and may be freed.
static uint64_t mvcc_min_active() {
    return g.active_snaps.empty() ? g.clock : *g.active_snaps.begin();
}

// Newest version visible at read_ts (commit_ts <= read_ts), or nullptr.
static Version *mvcc_visible(const Entry *ent, uint64_t read_ts) {
    for (Version *v = ent->vhead; v; v = v->next)
        if (v->commit_ts <= read_ts) return v;
    return nullptr;
}

// Free every version older than the newest one visible to the oldest snapshot.
static void mvcc_prune(Entry *ent) {
    uint64_t floor = mvcc_min_active();
    for (Version *v = ent->vhead; v; v = v->next) {
        if (v->commit_ts <= floor) {
            Version *old = v->next;
            v->next = nullptr;
            while (old) { Version *n = old->next; delete old; old = n; }
            return;
        }
    }
}

// Prepend a new version (commit_ts must be the latest), then prune.
static void mvcc_push(Entry *ent, uint64_t commit_ts, bool deleted,
                      std::string value) {
    Version *v   = new Version();
    v->commit_ts = commit_ts;
    v->deleted   = deleted;
    v->value     = std::move(value);
    v->next      = ent->vhead;
    ent->vhead   = v;
    mvcc_prune(ent);
}

static void mvcc_free_chain(Entry *ent) {
    Version *v = ent->vhead;
    while (v) { Version *n = v->next; delete v; v = n; }
    ent->vhead = nullptr;
}


// ── entry_eq — declared here so worker_get_func can use it ───────────────────

static bool entry_eq(HNode *node, HNode *key) {
    return container_of(node,Entry,node)->key ==
           container_of(key, LookupKey,node)->key;
}


// ── conn_update_el — declared here so drain_results() can use it ─────────────

static void conn_update_el(Conn *conn) {
    uint32_t ev = EV_ERR;
    if (conn->want_read)  ev |= EV_READ;
    if (conn->want_write) ev |= EV_WRITE;
    el_mod(&g.el, conn->fd, ev, (void*)(intptr_t)conn->fd);
}


// ── Worker GET function ───────────────────────────────────────────────────────
//
// This is the first real concurrent reader.  It runs on a thread-pool thread
// while SET/DEL continues single-threaded on the event loop.
//
// Safety model:
//   Writer (event loop): rcu_write_lock → mutate HMap → rcu_write_unlock
//   Reader (this func):  do { seq = rcu_read_begin; read; } while(rcu_read_retry)
//
// We copy Entry::str inside the critical section.  If a writer races with us
// (seq changes), rcu_read_retry returns true and we restart from scratch.
// The copy of a partially-updated std::string is never used because we reset
// the response buffer at the top of every retry iteration.

static void worker_get_func(void *arg) {
    GetTask *task = (GetTask *)arg;

    LookupKey lk;
    lk.key        = task->key;
    lk.node.hcode = task->hcode;

    Buffer resp;
    resp.reserve(64);

    // Seqlock retry loop — this is where concurrent readers actually run.
    uint64_t seq;
    do {
        // Reset to a 4-byte placeholder for the length header on each attempt.
        resp.resize(4);
        memset(resp.data(), 0, 4);

        seq = rcu_read_begin(&g.rcu);

        HNode *node = hm_lookup(&g.db, &lk.node, &entry_eq);
        if (!node) {
            out_nil(resp);
        } else {
            Entry *ent = container_of(node, Entry, node);
            if (ent->type != T_STR) {
                out_err(resp, ERR_BAD_TYP, "not a string value");
            } else {
                // Dead path (live reads are inline now); read latest committed.
                Version *v = mvcc_visible(ent, g.clock);
                if (!v || v->deleted) out_nil(resp);
                else out_str(resp, v->value.data(), v->value.size());
            }
        }
    } while (rcu_read_retry(&g.rcu, seq));

    // Fill in the 4-byte framing header (body length, excluding the header).
    uint32_t body_len = (uint32_t)(resp.size() - 4);
    memcpy(resp.data(), &body_len, 4);

    // Post result to the event loop via the result queue.
    GetResult *result = new GetResult{task->fd, std::move(resp)};
    pthread_mutex_lock(&g.result_mu);
    g.result_queue.push_back(result);
    pthread_mutex_unlock(&g.result_mu);

    // Wake the event loop.  A single byte is enough — the pipe is level-triggered
    // and the event loop drains the entire result_queue on each wakeup.
    uint8_t byte = 1;
    (void)write(g.wakeup_pipe[1], &byte, 1);

    delete task;
}


// ── dispatch_get — suspend conn, queue GET to thread pool ────────────────────

[[maybe_unused]] static void dispatch_get(Conn *conn, const std::string &key) {
    conn->get_pending.store(true, std::memory_order_release);

    // Suspend the connection while the GET is in-flight so that conn_update_el
    // at the bottom of the event loop iteration doesn't re-add EV_READ before
    // the worker has a chance to post its result.
    conn->want_read  = false;
    conn->want_write = false;
    el_mod(&g.el, conn->fd, EV_ERR, (void*)(intptr_t)conn->fd);

    GetTask *task  = new GetTask();
    task->fd       = conn->fd;
    task->key      = key;
    task->hcode    = str_hash((uint8_t*)key.data(), key.size());
    thread_pool_queue(&g.thread_pool, &worker_get_func, task);
}


// Forward declarations needed by drain_results (defined later in the file).
static bool try_one_request(Conn *conn);
static void handle_write(Conn *conn);
static void conn_destroy(Conn *conn);

// ── drain_results — called by event loop when wakeup_pipe fires ──────────────

static void drain_results() {
    // Non-blocking drain of the wakeup pipe (may have multiple bytes from
    // multiple workers finishing at the same time).
    uint8_t tmp[64];
    while (read(g.wakeup_pipe[0], tmp, sizeof(tmp)) > 0) {}

    // Swap the result queue under lock to minimise contention.
    std::deque<GetResult *> local;
    pthread_mutex_lock(&g.result_mu);
    local.swap(g.result_queue);
    pthread_mutex_unlock(&g.result_mu);

    for (GetResult *r : local) {
        // Guard: connection might have been destroyed while GET was in-flight.
        if (r->fd < 0 || r->fd >= (int)g.fd2conn.size() || !g.fd2conn[r->fd]) {
            delete r;
            continue;
        }
        Conn *conn = g.fd2conn[r->fd];
        conn->get_pending.store(false, std::memory_order_release);

        if (conn->want_close) {
            // conn_destroy() deferred cleanup until get_pending cleared.
            // Now complete the destruction.
            close(conn->fd);
            g.fd2conn[conn->fd] = NULL;
            dlist_detach(&conn->idle_node);
            delete conn;
        } else {
            // Append the worker's response.
            buf_append(conn->outgoing, r->response.data(), r->response.size());
            conn->want_read  = true;
            conn->want_write = true;

            // Edge-triggered mode: the kernel won't re-fire EV_READ if the
            // socket receive buffer is already empty (all pipelined requests
            // were read in one shot into conn->incoming).  Drive the request
            // loop directly so buffered commands get processed now.
            while (try_one_request(conn)) {}

            // Send any queued responses (including this GET's response).
            if (!conn->outgoing.empty()) {
                conn->want_write = true;
                handle_write(conn);
            }

            // Reload: handle_write may have set want_close on I/O error.
            conn = g.fd2conn[r->fd];
            if (conn) {
                // Invariant: if a GET was just dispatched (get_pending=true),
                // keep the connection suspended (want_read=false) so that
                // kqueue/epoll doesn't fire EV_READ while the worker is
                // still running — that would dispatch a second concurrent GET
                // for this connection and break response ordering.
                if (conn->get_pending.load(std::memory_order_acquire)) {
                    conn->want_read  = false;
                    conn->want_write = false;
                }
                if (conn->want_close) conn_destroy(conn);
                else                  conn_update_el(conn);
            }
        }
        delete r;
    }
}


// ── Entry lifecycle ───────────────────────────────────────────────────────────

static void entry_set_ttl(Entry *ent, int64_t ttl_ms);

static void entry_del_sync(Entry *ent) {
    if (ent->type == T_ZSET) zset_clear(&ent->zset);
    if (ent->type == T_STR)  mvcc_free_chain(ent);
    delete ent;
}
static void entry_del_func(void *arg) { entry_del_sync((Entry*)arg); }

static void entry_del(Entry *ent) {
    entry_set_ttl(ent,-1);
    size_t set_size = (ent->type==T_ZSET) ? hm_size(&ent->zset.hmap) : 0;
    if (set_size > 1000)
        thread_pool_queue(&g.thread_pool, &entry_del_func, ent);
    else
        entry_del_sync(ent);
}


// ── Checkpoint ────────────────────────────────────────────────────────────────

static bool cb_snapshot(HNode *node, void *arg) {
    MMapStore *ms  = (MMapStore *)arg;
    Entry     *ent = container_of(node, Entry, node);
    if (ent->type == T_STR) {
        Version *v = mvcc_visible(ent, g.clock);   // latest committed value
        if (v && !v->deleted) mmap_set(ms, ent->key, v->value);
    }
    return true;
}

static void maybe_checkpoint() {
    if (++g.mutations_since_ckpt < k_ckpt_ops) return;
    g.mutations_since_ckpt = 0;
    mmap_compact(&g.mstore, [](MMapStore *ms) {
        hm_foreach(&g.db, &cb_snapshot, ms);
    });
    wal_checkpoint(&g.wal);
}


// ── LRU tracking, memory accounting, eviction & tombstone GC ────────────────

// Recompute an entry's accounted bytes (key + newest visible value) and fold
// the delta into the global total.
static void mem_reaccount(Entry *ent) {
    uint64_t now = ent->key.size();
    Version *v = mvcc_visible(ent, g.clock);
    if (v && !v->deleted) now += v->value.size();
    g.mem_used -= ent->mem;
    ent->mem = now;
    g.mem_used += now;
}

// Move a string entry to the most-recently-used end of the LRU list. With this
// sentinel list, insert-before-sentinel places the node at `.prev` (the MRU
// end), so the least-recently-used entry is always at `g.lru_list.next`.
static void lru_touch(Entry *ent) {
    if (ent->type != T_STR) return;
    if (ent->in_lru) dlist_detach(&ent->lru_node);
    dlist_insert_before(&g.lru_list, &ent->lru_node);   // MRU end (.prev)
    ent->in_lru = true;
}

static void lru_untrack(Entry *ent) {
    if (ent->in_lru) { dlist_detach(&ent->lru_node); ent->in_lru = false; }
    g.mem_used -= ent->mem; ent->mem = 0;
}

// Physically remove a string entry from the keyspace (used by GC + eviction).
static void reap_entry(Entry *ent) {
    lru_untrack(ent);
    hm_delete(&g.db, &ent->node, &entry_eq);
    entry_set_ttl(ent, -1);
    mvcc_free_chain(ent);
    delete ent;
}

// Tombstone GC: if a key's newest version is a delete that no live snapshot can
// still see, the entry is invisible to everyone — reclaim it from the hash map.
static bool gc_reapable(Entry *ent) {
    if (ent->type != T_STR || !ent->vhead) return false;
    return ent->vhead->deleted && ent->vhead->commit_ts <= mvcc_min_active();
}

// Enforce maxmemory by evicting least-recently-used string keys. Skips keys a
// live transaction snapshot might still need (never breaks snapshot isolation).
static void maybe_evict() {
    if (!cfg.maxmemory) return;
    uint64_t floor = mvcc_min_active();
    int guard = 0;
    while (g.mem_used > cfg.maxmemory && !dlist_empty(&g.lru_list) && guard++ < 100000) {
        Entry *lru = container_of(g.lru_list.next, Entry, lru_node);  // LRU end (.next)
        // Only evict keys fully visible-as-committed to every live snapshot.
        if (lru->vhead && lru->vhead->commit_ts > floor) break;
        LOG_D("evicting key '%s' (maxmemory)", lru->key.c_str());
        reap_entry(lru);
        g.stat_evictions++;
    }
}


// ── Command helpers ───────────────────────────────────────────────────────────

static bool str2int(const std::string &s, int64_t &out) {
    char *e = NULL; out = strtoll(s.c_str(),&e,10);
    return e == s.c_str()+s.size();
}
static bool str2dbl(const std::string &s, double &out) {
    char *e = NULL; out = strtod(s.c_str(),&e);
    return e == s.c_str()+s.size() && !isnan(out);
}


// ── Command handlers ──────────────────────────────────────────────────────────

// Look up the string Entry for `key` (nullptr if absent or a non-string type).
static Entry *str_entry(const std::string &key) {
    LookupKey lk; lk.key = key;
    lk.node.hcode = str_hash((uint8_t*)key.data(), key.size());
    HNode *node = hm_lookup(&g.db, &lk.node, &entry_eq);
    return node ? container_of(node, Entry, node) : nullptr;
}

// GET — MVCC read. Inside a transaction: read-your-own-writes, then the
// snapshot (read_ts); otherwise the latest committed value (g.clock).
static void do_get(Conn *conn, std::vector<std::string> &cmd, Buffer &out) {
    const std::string &key = cmd[1];
    if (conn && conn->txn.active) {
        auto it = conn->txn.writes.find(key);
        if (it != conn->txn.writes.end()) {
            if (it->second.deleted) return out_nil(out);
            return out_str(out, it->second.value.data(), it->second.value.size());
        }
    }
    uint64_t read_ts = (conn && conn->txn.active) ? conn->txn.read_ts : g.clock;
    Entry *ent = str_entry(key);
    if (!ent) return out_nil(out);
    if (ent->type != T_STR) return out_err(out,ERR_BAD_TYP,"not a string value");
    Version *v = mvcc_visible(ent, read_ts);
    if (!v || v->deleted) return out_nil(out);
    return out_str(out, v->value.data(), v->value.size());
}

// Apply one committed string write as a new version at commit_ts (also logs to
// WAL + mmap unless replaying). Caller holds the rcu write lock.
static bool apply_write(const std::string &key, uint64_t commit_ts,
                        bool deleted, std::string value) {
    Entry *ent = str_entry(key);
    if (ent && ent->type != T_STR) return false;   // type clash — skip
    if (!ent) {
        ent = new Entry();
        ent->type = T_STR; ent->key = key;
        ent->node.hcode = str_hash((uint8_t*)key.data(), key.size());
        hm_insert(&g.db, &ent->node);
    }
    if (!g.replaying) {
        if (deleted) { wal_append(&g.wal, WAL_DEL, {key}); mmap_del(&g.mstore, key); }
        else { wal_append(&g.wal, WAL_SET, {key, value}); mmap_set(&g.mstore, key, value); }
    }
    mvcc_push(ent, commit_ts, deleted, std::move(value));
    // LRU + memory accounting; evict if over the configured budget.
    if (deleted) { lru_untrack(ent); }
    else { lru_touch(ent); mem_reaccount(ent); if (!g.replaying) maybe_evict(); }
    return true;
}

// SET — buffered in a transaction, otherwise an autocommit version bump.
static void do_set(Conn *conn, std::vector<std::string> &cmd, Buffer &out) {
    const std::string &key = cmd[1];
    Entry *cur = str_entry(key);
    if (cur && cur->type != T_STR)
        return out_err(out,ERR_BAD_TYP,"a non-string value exists");

    if (conn && conn->txn.active) {
        conn->txn.writes[key] = TxnWrite{false, cmd[2]};
        return out_nil(out);
    }
    rcu_write_lock(&g.rcu);
    apply_write(key, ++g.clock, false, cmd[2]);
    rcu_write_unlock(&g.rcu);
    if (!g.replaying) maybe_checkpoint();
    return out_nil(out);
}

// DEL — buffered in a transaction, otherwise an autocommit tombstone.
static void do_del(Conn *conn, std::vector<std::string> &cmd, Buffer &out) {
    const std::string &key = cmd[1];
    Entry *ent = str_entry(key);

    if (conn && conn->txn.active) {
        bool existed = false;
        auto it = conn->txn.writes.find(key);
        if (it != conn->txn.writes.end()) existed = !it->second.deleted;
        else if (ent && ent->type == T_STR) {
            Version *v = mvcc_visible(ent, conn->txn.read_ts);
            existed = v && !v->deleted;
        } else if (ent && ent->type == T_ZSET) existed = true;
        conn->txn.writes[key] = TxnWrite{true, ""};
        return out_int(out, existed ? 1 : 0);
    }

    // Autocommit. ZSet keys are not versioned — delete them physically.
    if (ent && ent->type == T_ZSET) {
        LookupKey lk; lk.key = key;
        lk.node.hcode = str_hash((uint8_t*)key.data(), key.size());
        rcu_write_lock(&g.rcu);
        HNode *node = hm_delete(&g.db, &lk.node, &entry_eq);
        rcu_write_unlock(&g.rcu);
        if (node) entry_del(container_of(node,Entry,node));
        if (!g.replaying) maybe_checkpoint();
        return out_int(out, 1);
    }
    Version *v = ent ? mvcc_visible(ent, g.clock) : nullptr;
    bool existed = v && !v->deleted;
    if (existed) {
        rcu_write_lock(&g.rcu);
        apply_write(key, ++g.clock, true, "");
        rcu_write_unlock(&g.rcu);
        if (!g.replaying) maybe_checkpoint();
    }
    return out_int(out, existed ? 1 : 0);
}

// ── MVCC transaction control ────────────────────────────────────────────────

static void do_begin(Conn *conn, Buffer &out) {
    if (conn->txn.active) return out_err(out,ERR_UNKNOWN,"already in transaction");
    conn->txn.active  = true;
    conn->txn.read_ts = g.clock;
    conn->txn.writes.clear();
    g.active_snaps.insert(conn->txn.read_ts);
    return out_str(out, "OK", 2);
}

// Defined below; needed by do_commit to replay buffered non-string mutations.
static void do_zadd(std::vector<std::string> &cmd, Buffer &out);
static void do_zrem(std::vector<std::string> &cmd, Buffer &out);
static void do_expire(std::vector<std::string> &cmd, Buffer &out);

static void txn_end(Conn *conn) {
    auto it = g.active_snaps.find(conn->txn.read_ts);
    if (it != g.active_snaps.end()) g.active_snaps.erase(it);
    conn->txn.active = false;
    conn->txn.writes.clear();
    conn->txn.deferred.clear();
}

static void do_rollback(Conn *conn, Buffer &out) {
    if (!conn->txn.active) return out_err(out,ERR_UNKNOWN,"no transaction in progress");
    txn_end(conn);
    return out_str(out, "OK", 2);
}

static void do_commit(Conn *conn, Buffer &out) {
    if (!conn->txn.active) return out_err(out,ERR_UNKNOWN,"no transaction in progress");

    // Write-write conflict check (snapshot isolation, first-committer-wins):
    // abort if any key in the write set was committed after our snapshot.
    for (auto &kv : conn->txn.writes) {
        Entry *ent = str_entry(kv.first);
        if (!ent) continue;
        if (ent->type != T_STR) {
            txn_end(conn);
            return out_err(out,ERR_BAD_TYP,"a non-string value exists; transaction aborted");
        }
        if (ent->vhead && ent->vhead->commit_ts > conn->txn.read_ts) {
            txn_end(conn);
            return out_err(out,ERR_UNKNOWN,"write-write conflict; transaction aborted");
        }
    }

    // No conflict — apply every buffered string write atomically at one
    // commit_ts, then replay buffered non-string mutations in order. All run on
    // the single event-loop thread with no interleaving, and their WAL records
    // land in the same group-commit batch, so the transaction is atomic.
    uint64_t ct = ++g.clock;
    rcu_write_lock(&g.rcu);
    for (auto &kv : conn->txn.writes)
        apply_write(kv.first, ct, kv.second.deleted, kv.second.value);
    rcu_write_unlock(&g.rcu);

    Buffer scratch;
    for (auto &cmd : conn->txn.deferred) {
        if      (cmd[0]=="zadd")    do_zadd(cmd, scratch);
        else if (cmd[0]=="zrem")    do_zrem(cmd, scratch);
        else if (cmd[0]=="pexpire") do_expire(cmd, scratch);
    }

    txn_end(conn);
    if (!g.replaying) maybe_checkpoint();
    return out_str(out, "OK", 2);
}

static void heap_delete(std::vector<HeapItem> &a, size_t pos) {
    a[pos] = a.back(); a.pop_back();
    if (pos < a.size()) heap_update(a.data(),pos,a.size());
}
static void heap_upsert(std::vector<HeapItem> &a, size_t pos, HeapItem t) {
    if (pos < a.size()) a[pos] = t; else { pos=a.size(); a.push_back(t); }
    heap_update(a.data(),pos,a.size());
}

static void entry_set_ttl(Entry *ent, int64_t ttl_ms) {
    if (ttl_ms < 0 && ent->heap_idx != (size_t)-1) {
        heap_delete(g.heap, ent->heap_idx); ent->heap_idx = -1;
    } else if (ttl_ms >= 0) {
        uint64_t expire_at = get_monotonic_msec() + (uint64_t)ttl_ms;
        HeapItem item = {expire_at, &ent->heap_idx};
        heap_upsert(g.heap, ent->heap_idx, item);
    }
}

static void do_expire(std::vector<std::string> &cmd, Buffer &out) {
    int64_t ttl_ms = 0;
    if (!str2int(cmd[2],ttl_ms)) return out_err(out,ERR_BAD_ARG,"expect int64");
    if (!g.replaying) wal_append(&g.wal, WAL_EXPIRE, {cmd[1], cmd[2]});
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t*)key.key.data(), key.key.size());
    HNode *node = hm_lookup(&g.db, &key.node, &entry_eq);
    if (node) entry_set_ttl(container_of(node,Entry,node), ttl_ms);
    return out_int(out, node ? 1 : 0);
}

static void do_ttl(std::vector<std::string> &cmd, Buffer &out) {
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t*)key.key.data(), key.key.size());
    HNode *node = hm_lookup(&g.db, &key.node, &entry_eq);
    if (!node) return out_int(out,-2);
    Entry *ent = container_of(node,Entry,node);
    if (ent->heap_idx == (size_t)-1) return out_int(out,-1);
    uint64_t expire_at = g.heap[ent->heap_idx].val;
    uint64_t now_ms    = get_monotonic_msec();
    return out_int(out, expire_at > now_ms ? (int64_t)(expire_at-now_ms) : 0);
}

// Snapshot-consistent KEYS. A string key is visible if the version at the
// caller's snapshot (read_ts) is non-deleted, with the transaction's own
// buffered writes overlaid (read-your-own-writes). ZSet keys are shown live.
struct KeysCtx {
    Conn                     *conn;
    uint64_t                  read_ts;
    std::vector<std::string>  keys;
    std::unordered_set<std::string> seen;
};

static bool cb_keys(HNode *node, void *arg) {
    KeysCtx *c   = (KeysCtx *)arg;
    Entry   *ent = container_of(node, Entry, node);
    c->seen.insert(ent->key);
    if (ent->type == T_STR) {
        // Transaction's own buffered write wins (read-your-own-writes).
        if (c->conn && c->conn->txn.active) {
            auto it = c->conn->txn.writes.find(ent->key);
            if (it != c->conn->txn.writes.end()) {
                if (!it->second.deleted) c->keys.push_back(ent->key);
                return true;
            }
        }
        Version *v = mvcc_visible(ent, c->read_ts);
        if (!v || v->deleted) return true;     // not visible at the snapshot
    }
    c->keys.push_back(ent->key);
    return true;
}

static void do_keys(Conn *conn, std::vector<std::string> &, Buffer &out) {
    KeysCtx c;
    c.conn    = conn;
    c.read_ts = (conn && conn->txn.active) ? conn->txn.read_ts : g.clock;
    hm_foreach(&g.db, &cb_keys, &c);
    // Add keys created by this transaction that don't exist committed yet.
    if (conn && conn->txn.active)
        for (auto &kv : conn->txn.writes)
            if (!kv.second.deleted && !c.seen.count(kv.first))
                c.keys.push_back(kv.first);
    out_arr(out,(uint32_t)c.keys.size());
    for (const std::string &k : c.keys) out_str(out, k.data(), k.size());
}

// Inside a transaction, buffer a non-string mutation for atomic replay at
// COMMIT (returns QUEUED); outside one, run it immediately.
static void defer_or_run(Conn *conn, std::vector<std::string> &cmd, Buffer &out,
                         void (*fn)(std::vector<std::string>&, Buffer&)) {
    if (conn && conn->txn.active) {
        conn->txn.deferred.push_back(cmd);
        return out_str(out, "QUEUED", 6);
    }
    fn(cmd, out);
}

static void do_zadd(std::vector<std::string> &cmd, Buffer &out) {
    double score = 0;
    if (!str2dbl(cmd[2],score)) return out_err(out,ERR_BAD_ARG,"expect float");
    if (!g.replaying) wal_append(&g.wal, WAL_ZADD, {cmd[1], cmd[2], cmd[3]});
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t*)key.key.data(), key.key.size());
    HNode *hnode = hm_lookup(&g.db, &key.node, &entry_eq);
    Entry *ent = NULL;
    if (!hnode) {
        ent = new Entry(); ent->type = T_ZSET;
        ent->key.swap(key.key); ent->node.hcode = key.node.hcode;
        hm_insert(&g.db, &ent->node);
    } else {
        ent = container_of(hnode,Entry,node);
        if (ent->type != T_ZSET) return out_err(out,ERR_BAD_TYP,"expect zset");
    }
    bool added = zset_insert(&ent->zset, cmd[3].data(), cmd[3].size(), score);
    if (!g.replaying) maybe_checkpoint();
    return out_int(out,(int64_t)added);
}

static ZSet *expect_zset(std::string &s) {
    LookupKey key; key.key.swap(s);
    key.node.hcode = str_hash((uint8_t*)key.key.data(), key.key.size());
    HNode *hnode = hm_lookup(&g.db, &key.node, &entry_eq);
    if (!hnode) return nullptr;
    Entry *ent = container_of(hnode,Entry,node);
    return ent->type==T_ZSET ? &ent->zset : nullptr;
}

static void do_zrem(std::vector<std::string> &cmd, Buffer &out) {
    if (!g.replaying) wal_append(&g.wal, WAL_ZREM, {cmd[1], cmd[2]});
    ZSet *zset = expect_zset(cmd[1]);
    if (!zset) return out_err(out,ERR_BAD_TYP,"expect zset");
    ZNode *znode = zset_lookup(zset, cmd[2].data(), cmd[2].size());
    if (znode) zset_delete(zset,znode);
    if (!g.replaying) maybe_checkpoint();
    return out_int(out, znode ? 1 : 0);
}

static void do_zscore(std::vector<std::string> &cmd, Buffer &out) {
    ZSet *zset = expect_zset(cmd[1]);
    if (!zset) return out_err(out,ERR_BAD_TYP,"expect zset");
    ZNode *znode = zset_lookup(zset, cmd[2].data(), cmd[2].size());
    return znode ? out_dbl(out,znode->score) : out_nil(out);
}

static void do_zquery(std::vector<std::string> &cmd, Buffer &out) {
    double score = 0;
    if (!str2dbl(cmd[2],score)) return out_err(out,ERR_BAD_ARG,"expect fp");
    int64_t offset=0, limit=0;
    if (!str2int(cmd[4],offset)||!str2int(cmd[5],limit))
        return out_err(out,ERR_BAD_ARG,"expect int");
    ZSet *zset = expect_zset(cmd[1]);
    if (!zset) return out_err(out,ERR_BAD_TYP,"expect zset");
    if (limit<=0) return out_arr(out,0);
    ZNode *znode = zset_seekge(zset,score,cmd[3].data(),cmd[3].size());
    znode = znode_offset(znode,offset);
    size_t ctx = out_begin_arr(out);
    int64_t n = 0;
    while (znode && n < limit) {
        out_str(out,znode->name,znode->len); out_dbl(out,znode->score);
        znode = znode_offset(znode,+1); n += 2;
    }
    out_end_arr(out,ctx,(uint32_t)n);
}

// INFO — return server metrics as a newline-delimited key:value text blob,
// mirroring the shape of Redis INFO. Read-only; safe to poll for monitoring.
// Render metrics into `buf`. If `prom` is set, emit Prometheus text-exposition
// format (# TYPE lines + kvs_<name> <value>); otherwise Redis-style key:value.
static int fill_metrics(char *buf, size_t cap, bool prom) {
    uint64_t up = (get_monotonic_msec() - g.stat_start_ms) / 1000;
    struct M { const char *name, *type; unsigned long long val; };
    M m[] = {
        {"uptime_seconds",        "gauge",   (unsigned long long)up},
        {"connections_received",  "counter", g.stat_conns_total},
        {"connections_active",    "gauge",   g.stat_conns_active},
        {"commands_processed",    "counter", g.stat_commands_total},
        {"reads",                 "counter", g.stat_reads},
        {"writes",                "counter", g.stat_writes},
        {"keyspace_keys",         "gauge",   (unsigned long long)hm_size(&g.db)},
        {"memory_used_bytes",     "gauge",   g.mem_used},
        {"maxmemory_bytes",       "gauge",   cfg.maxmemory},
        {"evicted_keys",          "counter", g.stat_evictions},
        {"expired_keys",          "counter", g.stat_expired},
        {"gc_reaped_tombstones",  "counter", g.stat_gc_reaped},
        {"wal_records",           "counter", g.wal.records},
        {"wal_syncs",             "counter", g.wal.syncs},
        {"wal_bytes",             "gauge",   g.wal.file_size},
    };
    int off = 0;
    for (const M &e : m) {
        if (prom)
            off += snprintf(buf+off, cap-off,
                "# TYPE kvs_%s %s\nkvs_%s %llu\n", e.name, e.type, e.name, e.val);
        else
            off += snprintf(buf+off, cap-off, "%s:%llu\r\n", e.name, e.val);
        if (off >= (int)cap) break;
    }
    return off < 0 ? 0 : off;
}

// INFO — server metrics as newline-delimited key:value text (Redis-style).
static void do_info(Buffer &out) {
    char buf[1536];
    int n = fill_metrics(buf, sizeof(buf), false);
    out_str(out, buf, (size_t)n);
}

// Serve one Prometheus scrape: accept, read+discard the HTTP request, reply
// with 200 + text-exposition metrics, close. Synchronous — scrapes are rare
// and tiny, so this never contends with the KV hot path.
static void serve_metrics(int listen_fd) {
    while (true) {
        int c = accept(listen_fd, NULL, NULL);
        if (c < 0) break;                       // backlog drained
        char req[2048];
        (void)!read(c, req, sizeof(req));       // consume request line/headers
        char body[1536];
        int blen = fill_metrics(body, sizeof(body), true);
        char resp[2048];
        int rlen = snprintf(resp, sizeof(resp),
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: text/plain; version=0.0.4\r\n"
            "Content-Length: %d\r\n\r\n", blen);
        (void)!write(c, resp, rlen);
        (void)!write(c, body, blen);
        close(c);
    }
}

static void do_request(Conn *conn, std::vector<std::string> &cmd, Buffer &out) {
    if      (cmd.size()==2 && cmd[0]=="auth") {   // already authed (gate passed)
        out_str(out, cfg.requirepass.empty() ? "OK (no auth required)" : "OK",
                cfg.requirepass.empty() ? 20 : 2);
    }
    else if (cmd.size()==1 && cmd[0]=="info")     do_info(out);
    else if (cmd.size()==2 && cmd[0]=="get")      do_get(conn,cmd,out);
    else if (cmd.size()==3 && cmd[0]=="set")      do_set(conn,cmd,out);
    else if (cmd.size()==2 && cmd[0]=="del")      do_del(conn,cmd,out);
    else if (cmd.size()==1 && cmd[0]=="begin")    { if (conn) do_begin(conn,out);
                                                    else out_err(out,ERR_UNKNOWN,"no connection"); }
    else if (cmd.size()==1 && cmd[0]=="commit")   { if (conn) do_commit(conn,out);
                                                    else out_err(out,ERR_UNKNOWN,"no connection"); }
    else if (cmd.size()==1 && cmd[0]=="rollback") { if (conn) do_rollback(conn,out);
                                                    else out_err(out,ERR_UNKNOWN,"no connection"); }
    else if (cmd.size()==1 && cmd[0]=="keys")     do_keys(conn,cmd,out);
    // Non-string mutations buffer into the transaction (atomic at COMMIT);
    // outside a transaction they apply immediately.
    else if (cmd.size()==3 && cmd[0]=="pexpire")  defer_or_run(conn,cmd,out,&do_expire);
    else if (cmd.size()==4 && cmd[0]=="zadd")     defer_or_run(conn,cmd,out,&do_zadd);
    else if (cmd.size()==3 && cmd[0]=="zrem")     defer_or_run(conn,cmd,out,&do_zrem);
    // Non-string reads run live (sorted sets / TTLs are not versioned).
    else if (cmd.size()==2 && cmd[0]=="pttl")     do_ttl(cmd,out);
    else if (cmd.size()==3 && cmd[0]=="zscore")   do_zscore(cmd,out);
    else if (cmd.size()==6 && cmd[0]=="zquery")   do_zquery(cmd,out);
    else out_err(out,ERR_UNKNOWN,"unknown command");
}


// ── Protocol framing ──────────────────────────────────────────────────────────

static void response_begin(Buffer &out, size_t *hdr) {
    *hdr = out.size(); buf_append_u32(out,0);
}
static void response_end(Buffer &out, size_t hdr) {
    size_t msg_size = out.size()-hdr-4;
    if (msg_size > k_max_msg) {
        out.resize(hdr+4);
        out_err(out,ERR_TOO_BIG,"response is too big");
        msg_size = out.size()-hdr-4;
    }
    uint32_t len = (uint32_t)msg_size;
    memcpy(&out[hdr],&len,4);
}

// ── inline_get — read fast-path on the event-loop thread ─────────────────────
//
// Executes a GET directly into conn->outgoing under the same seqlock read
// section the thread-pool worker uses, instead of dispatch_get().
//
// Why: dispatch_get() suspends the connection until a worker posts the result,
// so a pipelined client only ever has ONE GET in flight per connection — the
// dispatch→worker→wakeup_pipe→drain_results round-trip serialises the pipeline.
// Measured: 130k ops/s (dispatch) vs ~845k ops/s (inline, same hardware) at
// pipeline=64. For read-mostly pipelined traffic the worker offload is pure
// latency with no concurrency win, because writers are single-threaded anyway.
//
// Correctness: identical to worker_get_func — rcu_read_begin/retry guards the
// HMap against the single-threaded writer; the value copy is discarded and
// retried if a writer races. (On the event-loop thread no writer can run
// concurrently, so the retry never fires here, but the protocol is kept so the
// read remains safe if reads are offloaded again.) dispatch_get/worker_get_func
// are retained for that path and for WAL replay.
static void inline_get(Conn *conn, const std::string &key) {
    size_t hdr;
    response_begin(conn->outgoing, &hdr);

    // Read-your-own-writes inside a transaction: serve from the write set.
    if (conn->txn.active) {
        auto it = conn->txn.writes.find(key);
        if (it != conn->txn.writes.end()) {
            if (it->second.deleted) out_nil(conn->outgoing);
            else out_str(conn->outgoing, it->second.value.data(), it->second.value.size());
            response_end(conn->outgoing, hdr);
            return;
        }
    }
    uint64_t read_ts = conn->txn.active ? conn->txn.read_ts : g.clock;

    LookupKey lk;
    lk.key        = key;
    lk.node.hcode = str_hash((uint8_t*)key.data(), key.size());

    size_t body_start = conn->outgoing.size();
    uint64_t seq;
    do {
        conn->outgoing.resize(body_start);   // drop any partial body from a retry
        seq = rcu_read_begin(&g.rcu);

        HNode *node = hm_lookup(&g.db, &lk.node, &entry_eq);
        if (!node) {
            out_nil(conn->outgoing);
        } else {
            Entry *ent = container_of(node, Entry, node);
            if (ent->type != T_STR) {
                out_err(conn->outgoing, ERR_BAD_TYP, "not a string value");
            } else {
                // MVCC visibility: newest version at/below the snapshot.
                Version *v = mvcc_visible(ent, read_ts);
                if (!v || v->deleted) out_nil(conn->outgoing);
                else out_str(conn->outgoing, v->value.data(), v->value.size());
            }
        }
    } while (rcu_read_retry(&g.rcu, seq));

    response_end(conn->outgoing, hdr);
}

static bool try_one_request(Conn *conn) {
    if (conn->incoming.size() < 4) return false;
    uint32_t len = 0;
    memcpy(&len, conn->incoming.data(), 4);
    if (len > k_max_msg) { conn->want_close=true; return false; }
    if (4+len > conn->incoming.size()) return false;

    std::vector<std::string> cmd;
    if (parse_req(&conn->incoming[4], len, cmd) < 0) {
        conn->want_close=true; return false;
    }

    // ── AUTH gate: until authenticated, only AUTH is accepted ─────────────
    if (!conn->authenticated) {
        size_t hdr; response_begin(conn->outgoing, &hdr);
        if (cmd.size()==2 && cmd[0]=="auth") {
            if (cmd[1] == cfg.requirepass) { conn->authenticated = true;
                                             out_str(conn->outgoing, "OK", 2); }
            else out_err(conn->outgoing, ERR_UNKNOWN, "ERR invalid password");
        } else {
            out_err(conn->outgoing, ERR_UNKNOWN, "NOAUTH authentication required");
        }
        response_end(conn->outgoing, hdr);
        buf_consume(conn->incoming, 4+len);
        return true;
    }

    // ── Observability: classify and count the request ─────────────────────
    g.stat_commands_total++;
    bool is_write = !cmd.empty() &&
        (cmd[0]=="set" || cmd[0]=="del" || cmd[0]=="pexpire" ||
         cmd[0]=="zadd" || cmd[0]=="zrem");
    if (is_write) g.stat_writes++; else g.stat_reads++;

    // ── GET fast-path: execute inline under the seqlock read section ──────
    // Keeps the connection's pipeline flowing (no suspend/wakeup round-trip).
    if (cmd.size() == 2 && cmd[0] == "get") {
        inline_get(conn, cmd[1]);
        buf_consume(conn->incoming, 4+len);
        return true;
    }
    // ─────────────────────────────────────────────────────────────────────

    size_t hdr = 0;
    response_begin(conn->outgoing, &hdr);
    do_request(conn, cmd, conn->outgoing);
    response_end(conn->outgoing, hdr);
    buf_consume(conn->incoming, 4+len);
    return true;
}


// ── I/O handlers (edge-triggered drain loops) ─────────────────────────────────

static void handle_write(Conn *conn) {
    while (!conn->outgoing.empty()) {
        ssize_t rv = write(conn->fd,
                           conn->outgoing.data(), conn->outgoing.size());
        if (rv < 0 && errno == EAGAIN) break;
        if (rv < 0) { conn->want_close=true; return; }
        buf_consume(conn->outgoing, (size_t)rv);
    }
    if (conn->outgoing.empty()) {
        conn->want_read  = true;
        conn->want_write = false;
    }
}

static void handle_read(Conn *conn) {
    uint8_t buf[64*1024];
    while (true) {
        ssize_t rv = read(conn->fd, buf, sizeof(buf));
        if (rv < 0 && errno == EAGAIN) break;
        if (rv < 0) { msg_errno("read()"); conn->want_close=true; return; }
        if (rv == 0) { conn->want_close=true; return; }
        buf_append(conn->incoming, buf, (size_t)rv);
    }
    while (try_one_request(conn)) {}

    // Group commit: one fdatasync() covers every WAL record appended while
    // processing this pipeline batch. Must run BEFORE any response is written to
    // the client, so a mutation is acked only after it is durable — the same
    // durability contract as per-write fsync, at a fraction of the syscall cost
    // (one fsync per batch instead of one per write).
    wal_sync(&g.wal);

    if (!conn->outgoing.empty()) {
        conn->want_read  = false;
        conn->want_write = true;
        handle_write(conn);
    }
}


// ── Connection lifecycle ──────────────────────────────────────────────────────

static void handle_accept(int listen_fd) {
    // Edge-triggered: drain the entire accept backlog before returning.
    // A single EV_READ on the listen fd may represent multiple queued
    // connections; we must loop until accept() returns EAGAIN.
    while (true) {
    struct sockaddr_in client_addr = {};
    socklen_t addrlen = sizeof(client_addr);
    int connfd = accept(listen_fd, (struct sockaddr*)&client_addr, &addrlen);
    if (connfd < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;   // backlog drained
        msg_errno("accept()"); return;
    }

    // Backpressure: reject connections beyond the configured cap.
    if ((int)g.stat_conns_active >= cfg.max_clients) {
        LOG_W("connection limit %d reached — rejecting new client", cfg.max_clients);
        close(connfd);
        continue;
    }

    uint32_t ip = client_addr.sin_addr.s_addr;
    LOG_D("new client %u.%u.%u.%u:%u",
          ip&255,(ip>>8)&255,(ip>>16)&255,ip>>24, ntohs(client_addr.sin_port));

    fd_set_nb(connfd);

    Conn *conn = new Conn();
    conn->fd             = connfd;
    conn->want_read      = true;
    conn->authenticated  = cfg.requirepass.empty();   // auth off ⇒ pre-authed
    conn->last_active_ms = get_monotonic_msec();
    dlist_insert_before(&g.idle_list, &conn->idle_node);

    if (g.fd2conn.size() <= (size_t)connfd)
        g.fd2conn.resize(connfd+1);
    assert(!g.fd2conn[connfd]);
    g.fd2conn[connfd] = conn;

    el_add(&g.el, connfd, EV_READ|EV_ERR, (void*)(intptr_t)connfd);
    g.stat_conns_total++;
    g.stat_conns_active++;
    } // end while(true) accept loop
}

static void conn_destroy(Conn *conn) {
    // Always remove from the event loop first — no more events for this fd.
    el_del(&g.el, conn->fd);

    if (conn->get_pending.load(std::memory_order_acquire)) {
        // A worker thread still holds a GetTask for this fd.
        // Mark for deferred cleanup; drain_results() will complete it
        // after the worker posts its (discarded) result.
        conn->want_close = true;
        return;
    }

    close(conn->fd);
    g.fd2conn[conn->fd] = NULL;
    dlist_detach(&conn->idle_node);
    delete conn;
    if (g.stat_conns_active) g.stat_conns_active--;
}


// ── Timer management ──────────────────────────────────────────────────────────

const uint64_t k_idle_timeout_ms = 5000;

static int32_t next_timer_ms() {
    uint64_t now  = get_monotonic_msec();
    uint64_t next = (uint64_t)-1;
    if (!dlist_empty(&g.idle_list)) {
        Conn *c = container_of(g.idle_list.next, Conn, idle_node);
        next = c->last_active_ms + k_idle_timeout_ms;
    }
    if (!g.heap.empty() && g.heap[0].val < next) next = g.heap[0].val;
    if (next == (uint64_t)-1) return -1;
    if (next <= now)          return 0;
    return (int32_t)(next - now);
}

static bool hnode_same(HNode *a, HNode *b) { return a==b; }

// Reclaim a bounded batch of dead tombstone entries (keys whose newest version
// is a delete no live snapshot can still see). Runs each timer tick, bounded so
// it never stalls the event loop on a large keyspace.
static bool cb_gc(HNode *node, void *arg) {
    auto *dead = (std::vector<Entry*>*)arg;
    Entry *ent = container_of(node, Entry, node);
    if (gc_reapable(ent) && dead->size() < 256) dead->push_back(ent);
    return true;
}
static void gc_sweep() {
    // Throttle: a full keyspace scan is O(N), so run at most ~1×/sec.
    static uint64_t last_gc_ms = 0;
    uint64_t now = get_monotonic_msec();
    if (now - last_gc_ms < 1000) return;
    last_gc_ms = now;
    std::vector<Entry*> dead;
    hm_foreach(&g.db, &cb_gc, &dead);
    for (Entry *e : dead) { reap_entry(e); g.stat_gc_reaped++; }
}

static void process_timers() {
    uint64_t now = get_monotonic_msec();
    while (!dlist_empty(&g.idle_list)) {
        Conn *c = container_of(g.idle_list.next, Conn, idle_node);
        if (c->last_active_ms + k_idle_timeout_ms >= now) break;
        LOG_D("idle timeout fd=%d", c->fd);
        conn_destroy(c);
    }
    const size_t k_max_works = 2000;
    size_t nworks = 0;
    while (!g.heap.empty() && g.heap[0].val < now) {
        Entry *ent = container_of(g.heap[0].ref, Entry, heap_idx);
        HNode *node = hm_delete(&g.db, &ent->node, &hnode_same);
        assert(node == &ent->node);
        if (ent->in_lru) { dlist_detach(&ent->lru_node); ent->in_lru=false; }
        g.mem_used -= ent->mem;
        entry_del(ent);
        g.stat_expired++;
        if (++nworks >= k_max_works) break;
    }
    gc_sweep();
}


// ── WAL / mmap replay ─────────────────────────────────────────────────────────

static void wal_replay(WALOp op, const std::vector<std::string> &args) {
    std::vector<std::string> cmd;
    Buffer dummy;
    switch (op) {
    case WAL_SET:
        if (args.size() >= 2) { cmd={"set",args[0],args[1]}; do_set(nullptr,cmd,dummy); } break;
    case WAL_DEL:
        if (args.size() >= 1) { cmd={"del",args[0]}; do_del(nullptr,cmd,dummy); } break;
    case WAL_ZADD:
        if (args.size() >= 3) { cmd={"zadd",args[0],args[1],args[2]}; do_zadd(cmd,dummy); } break;
    case WAL_ZREM:
        if (args.size() >= 2) { cmd={"zrem",args[0],args[1]}; do_zrem(cmd,dummy); } break;
    case WAL_EXPIRE:
        if (args.size() >= 2) { cmd={"pexpire",args[0],args[1]}; do_expire(cmd,dummy); } break;
    }
}

static void mmap_replay(const std::string &key, const std::string &val) {
    if (val.empty()) {
        LookupKey lk; lk.key = key;
        lk.node.hcode = str_hash((uint8_t*)key.data(), key.size());
        HNode *n = hm_delete(&g.db, &lk.node, &entry_eq);
        if (n) entry_del(container_of(n, Entry, node));
    } else {
        LookupKey lk; lk.key = key;
        lk.node.hcode = str_hash((uint8_t*)key.data(), key.size());
        HNode *n = hm_lookup(&g.db, &lk.node, &entry_eq);
        Entry *ent;
        if (n) {
            ent = container_of(n, Entry, node);
        } else {
            ent = new Entry();
            ent->type  = T_STR; ent->key = key;
            ent->node.hcode = lk.node.hcode;
            hm_insert(&g.db, &ent->node);
        }
        mvcc_push(ent, ++g.clock, false, val);
    }
}


// ── main ──────────────────────────────────────────────────────────────────────

// Open a non-blocking listening socket on `port` (INADDR_ANY).
static int listen_on(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) die("socket()");
    int val = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
    struct sockaddr_in addr = {};
    addr.sin_family      = AF_INET;
    addr.sin_port        = ntohs((uint16_t)port);
    addr.sin_addr.s_addr = ntohl(0);
    if (bind(fd, (const sockaddr*)&addr, sizeof(addr))) die("bind()");
    fd_set_nb(fd);
    if (listen(fd, SOMAXCONN)) die("listen()");
    return fd;
}

// Flush and checkpoint on the way out so a graceful stop loses no acked data.
static void graceful_shutdown() {
    LOG_I("shutting down: syncing WAL and checkpointing snapshot");
    wal_sync(&g.wal);
    mmap_compact(&g.mstore, [](MMapStore *ms) {
        hm_foreach(&g.db, &cb_snapshot, ms);
    });
    wal_checkpoint(&g.wal);
    if (g.listen_fd >= 0) close(g.listen_fd);
    LOG_I("shutdown complete");
}

int main(int argc, char **argv) {
    parse_config(argc, argv);

    // Graceful shutdown on SIGTERM/SIGINT; ignore SIGPIPE (dead peer writes).
    struct sigaction sa = {};
    sa.sa_handler = on_signal;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT,  &sa, NULL);
    signal(SIGPIPE, SIG_IGN);

    dlist_init(&g.idle_list);
    dlist_init(&g.lru_list);
    seqlock_init(&g.rcu);
    thread_pool_init(&g.thread_pool, 4);
    el_init(&g.el);

    // ── Init multithreaded GET dispatch ───────────────────────────────────
    pthread_mutex_init(&g.result_mu, NULL);
    if (pipe(g.wakeup_pipe) != 0) die("pipe()");
    fd_set_nb(g.wakeup_pipe[0]);   // non-blocking so drain_results() won't stall
    fd_set_nb(g.wakeup_pipe[1]);   // non-blocking so workers never block on write

    // ── Restore state from disk (paths relative to data_dir) ──────────────
    std::string dat = cfg.data_dir + "/redis.dat";
    std::string wal = cfg.data_dir + "/redis.wal";
    g.stat_start_ms = get_monotonic_msec();
    g.replaying = true;
    mmap_open(&g.mstore, dat.c_str(), mmap_replay);
    wal_open(&g.wal, wal.c_str(), wal_replay);
    g.replaying = false;
    LOG_I("restored %zu keys from snapshot + WAL (dir=%s)",
          hm_size(&g.db), cfg.data_dir.c_str());

    // ── Listening socket(s) ───────────────────────────────────────────────
    g.listen_fd = listen_on(cfg.port);
    el_add(&g.el, g.listen_fd, EV_READ, (void*)(intptr_t)g.listen_fd);
    el_add(&g.el, g.wakeup_pipe[0], EV_READ, (void*)(intptr_t)g.wakeup_pipe[0]);

    if (cfg.metrics_port > 0) {
        g.metrics_fd = listen_on(cfg.metrics_port);
        el_add(&g.el, g.metrics_fd, EV_READ, (void*)(intptr_t)g.metrics_fd);
        LOG_I("Prometheus metrics on :%d/metrics", cfg.metrics_port);
    }

    LOG_I("listening on :%d  (kqueue/epoll + WAL + mmap + MVCC%s%s)",
          cfg.port,
          cfg.requirepass.empty() ? "" : " + auth",
          cfg.maxmemory ? " + maxmemory" : "");

    // ── Event loop ────────────────────────────────────────────────────────
    ELEvent events[1024];
    while (!g_stop) {
        int32_t timeout_ms = next_timer_ms();
        int n = el_wait(&g.el, events, 1024, timeout_ms);
        if (n < 0) continue;   // EINTR from a signal — re-check g_stop

        for (int i = 0; i < n; i++) {
            int efd = (int)(intptr_t)events[i].data;
            if (efd < 0) efd = events[i].fd;

            // ── Wakeup pipe: a worker thread posted a GET result ──────────
            if (efd == g.wakeup_pipe[0]) {
                drain_results();
                continue;
            }

            if (efd == g.listen_fd) {
                handle_accept(g.listen_fd);
                continue;
            }

            if (g.metrics_fd >= 0 && efd == g.metrics_fd) {
                serve_metrics(g.metrics_fd);
                continue;
            }

            if (efd < 0 || efd >= (int)g.fd2conn.size()) continue;
            Conn *conn = g.fd2conn[efd];
            if (!conn) continue;

            conn->last_active_ms = get_monotonic_msec();
            dlist_detach(&conn->idle_node);
            dlist_insert_before(&g.idle_list, &conn->idle_node);

            if (events[i].events & EV_READ)  handle_read(conn);
            if (events[i].events & EV_WRITE) handle_write(conn);

            conn = g.fd2conn[efd];
            if (!conn) continue;

            if ((events[i].events & EV_ERR) || conn->want_close) {
                conn_destroy(conn);
            } else {
                conn_update_el(conn);
            }
        }

        process_timers();
    }

    graceful_shutdown();
    return 0;
}
