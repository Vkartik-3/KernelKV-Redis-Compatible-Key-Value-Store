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
// C++
#include <string>
#include <vector>
#include <atomic>
#include <deque>
// proj — shared core modules
#include "../core/common.h"
#include "../core/hashtable.h"
#include "../core/zset.h"
#include "../core/list.h"
#include "../core/heap.h"
#include "../core/thread_pool.h"
// new modules
#include "event_loop.h"
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

const size_t k_max_msg  = 32 << 20;
const size_t k_max_args = 200 * 1000;


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

static bool read_u32(const uint8_t *&cur, const uint8_t *end, uint32_t &out) {
    if (cur+4 > end) return false;
    memcpy(&out,cur,4); cur+=4; return true;
}
static bool read_str(const uint8_t *&cur, const uint8_t *end,
                     size_t n, std::string &out) {
    if (cur+n > end) return false;
    out.assign(cur,cur+n); cur+=n; return true;
}
static int32_t parse_req(const uint8_t *data, size_t size,
                          std::vector<std::string> &out) {
    const uint8_t *end = data+size;
    uint32_t nstr = 0;
    if (!read_u32(data,end,nstr)) return -1;
    if (nstr > k_max_args) return -1;
    while (out.size() < nstr) {
        uint32_t len = 0;
        if (!read_u32(data,end,len)) return -1;
        out.push_back(std::string());
        if (!read_str(data,end,len,out.back())) return -1;
    }
    return (data==end) ? 0 : -1;
}


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

struct Conn {
    int  fd         = -1;
    bool want_read  = false;
    bool want_write = false;
    bool want_close = false;
    Buffer incoming;
    Buffer outgoing;
    uint64_t last_active_ms = 0;
    DList    idle_node;
    // True while a worker thread holds a GetTask for this fd.
    // conn_destroy() defers actual cleanup until the worker posts its result.
    std::atomic<bool> get_pending{false};
};


// ── KV entry ─────────────────────────────────────────────────────────────────

enum { T_INIT=0, T_STR=1, T_ZSET=2 };

struct Entry {
    HNode   node;
    std::string key;
    size_t  heap_idx = (size_t)-1;
    uint32_t type    = 0;
    std::string str;
    ZSet    zset;
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

    // ── Multithreaded GET dispatch ─────────────────────────────────────────
    // Worker threads post GetResult* here, then write 1 byte to wakeup_pipe[1].
    // The event loop reads wakeup_pipe[0] and drains result_queue.
    int             wakeup_pipe[2]  = {-1, -1};
    pthread_mutex_t result_mu;                   // protects result_queue
    std::deque<GetResult *> result_queue;
} g;

static const uint64_t k_ckpt_ops = 1000;


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
                // Copy the value while holding the seqlock read section.
                // If rcu_read_retry fires, this copy is discarded and we retry.
                std::string val = ent->str;
                out_str(resp, val.data(), val.size());
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

static void dispatch_get(Conn *conn, const std::string &key) {
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
    if (ent->type == T_STR) mmap_set(ms, ent->key, ent->str);
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

// do_get is kept for WAL replay only.
// Live GET requests are dispatched via dispatch_get() → worker_get_func().
static void do_get(std::vector<std::string> &cmd, Buffer &out) {
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t*)key.key.data(), key.key.size());
    HNode *node = hm_lookup(&g.db, &key.node, &entry_eq);
    if (!node) return out_nil(out);
    Entry *ent = container_of(node,Entry,node);
    if (ent->type != T_STR) return out_err(out,ERR_BAD_TYP,"not a string value");
    return out_str(out, ent->str.data(), ent->str.size());
}

static void do_set(std::vector<std::string> &cmd, Buffer &out) {
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t*)key.key.data(), key.key.size());

    if (!g.replaying) {
        wal_write(&g.wal, WAL_SET, {key.key, cmd[2]});
        mmap_set(&g.mstore, key.key, cmd[2]);
    }

    rcu_write_lock(&g.rcu);

    HNode *node = hm_lookup(&g.db, &key.node, &entry_eq);
    if (node) {
        Entry *ent = container_of(node,Entry,node);
        if (ent->type != T_STR) {
            rcu_write_unlock(&g.rcu);
            return out_err(out,ERR_BAD_TYP,"a non-string value exists");
        }
        ent->str.swap(cmd[2]);
    } else {
        Entry *ent = new Entry();
        ent->type  = T_STR;
        ent->key.swap(key.key);
        ent->node.hcode = key.node.hcode;
        ent->str.swap(cmd[2]);
        hm_insert(&g.db, &ent->node);
    }

    rcu_write_unlock(&g.rcu);
    if (!g.replaying) maybe_checkpoint();
    return out_nil(out);
}

static void do_del(std::vector<std::string> &cmd, Buffer &out) {
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t*)key.key.data(), key.key.size());

    if (!g.replaying) {
        wal_write(&g.wal, WAL_DEL, {key.key});
        mmap_del(&g.mstore, key.key);
    }

    rcu_write_lock(&g.rcu);
    HNode *node = hm_delete(&g.db, &key.node, &entry_eq);
    rcu_write_unlock(&g.rcu);

    if (node) entry_del(container_of(node,Entry,node));
    if (!g.replaying) maybe_checkpoint();
    return out_int(out, node ? 1 : 0);
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
    if (!g.replaying) wal_write(&g.wal, WAL_EXPIRE, {cmd[1], cmd[2]});
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

static bool cb_keys(HNode *node, void *arg) {
    Buffer &out = *(Buffer*)arg;
    out_str(out, container_of(node,Entry,node)->key.data(),
                 container_of(node,Entry,node)->key.size());
    return true;
}
static void do_keys(std::vector<std::string> &, Buffer &out) {
    out_arr(out,(uint32_t)hm_size(&g.db));
    hm_foreach(&g.db, &cb_keys, &out);
}

static void do_zadd(std::vector<std::string> &cmd, Buffer &out) {
    double score = 0;
    if (!str2dbl(cmd[2],score)) return out_err(out,ERR_BAD_ARG,"expect float");
    if (!g.replaying) wal_write(&g.wal, WAL_ZADD, {cmd[1], cmd[2], cmd[3]});
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
    if (!g.replaying) wal_write(&g.wal, WAL_ZREM, {cmd[1], cmd[2]});
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

static void do_request(std::vector<std::string> &cmd, Buffer &out) {
    if      (cmd.size()==2 && cmd[0]=="get")     do_get(cmd,out);
    else if (cmd.size()==3 && cmd[0]=="set")     do_set(cmd,out);
    else if (cmd.size()==2 && cmd[0]=="del")     do_del(cmd,out);
    else if (cmd.size()==3 && cmd[0]=="pexpire") do_expire(cmd,out);
    else if (cmd.size()==2 && cmd[0]=="pttl")    do_ttl(cmd,out);
    else if (cmd.size()==1 && cmd[0]=="keys")    do_keys(cmd,out);
    else if (cmd.size()==4 && cmd[0]=="zadd")    do_zadd(cmd,out);
    else if (cmd.size()==3 && cmd[0]=="zrem")    do_zrem(cmd,out);
    else if (cmd.size()==3 && cmd[0]=="zscore")  do_zscore(cmd,out);
    else if (cmd.size()==6 && cmd[0]=="zquery")  do_zquery(cmd,out);
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

    // ── Dispatch GET to thread pool ───────────────────────────────────────
    // Worker acquires seqlock read side, performs hm_lookup, copies value,
    // posts GetResult to result_queue, wakes event loop via wakeup_pipe.
    if (cmd.size() == 2 && cmd[0] == "get") {
        buf_consume(conn->incoming, 4+len);
        dispatch_get(conn, cmd[1]);
        // Connection is now suspended (el_mod to EV_ERR only).
        // Return false to stop the request processing loop — ordering is
        // preserved because no further requests are dispatched until the
        // GET result comes back and re-enables the connection.
        return false;
    }
    // ─────────────────────────────────────────────────────────────────────

    size_t hdr = 0;
    response_begin(conn->outgoing, &hdr);
    do_request(cmd, conn->outgoing);
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

    uint32_t ip = client_addr.sin_addr.s_addr;
    fprintf(stderr, "new client %u.%u.%u.%u:%u\n",
            ip&255,(ip>>8)&255,(ip>>16)&255,ip>>24,
            ntohs(client_addr.sin_port));

    fd_set_nb(connfd);

    Conn *conn = new Conn();
    conn->fd             = connfd;
    conn->want_read      = true;
    conn->last_active_ms = get_monotonic_msec();
    dlist_insert_before(&g.idle_list, &conn->idle_node);

    if (g.fd2conn.size() <= (size_t)connfd)
        g.fd2conn.resize(connfd+1);
    assert(!g.fd2conn[connfd]);
    g.fd2conn[connfd] = conn;

    el_add(&g.el, connfd, EV_READ|EV_ERR, (void*)(intptr_t)connfd);
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

static void process_timers() {
    uint64_t now = get_monotonic_msec();
    while (!dlist_empty(&g.idle_list)) {
        Conn *c = container_of(g.idle_list.next, Conn, idle_node);
        if (c->last_active_ms + k_idle_timeout_ms >= now) break;
        fprintf(stderr,"idle timeout fd=%d\n",c->fd);
        conn_destroy(c);
    }
    const size_t k_max_works = 2000;
    size_t nworks = 0;
    while (!g.heap.empty() && g.heap[0].val < now) {
        Entry *ent = container_of(g.heap[0].ref, Entry, heap_idx);
        HNode *node = hm_delete(&g.db, &ent->node, &hnode_same);
        assert(node == &ent->node);
        entry_del(ent);
        if (++nworks >= k_max_works) break;
    }
}


// ── WAL / mmap replay ─────────────────────────────────────────────────────────

static void wal_replay(WALOp op, const std::vector<std::string> &args) {
    std::vector<std::string> cmd;
    Buffer dummy;
    switch (op) {
    case WAL_SET:
        if (args.size() >= 2) { cmd={"set",args[0],args[1]}; do_set(cmd,dummy); } break;
    case WAL_DEL:
        if (args.size() >= 1) { cmd={"del",args[0]}; do_del(cmd,dummy); } break;
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
        if (n) {
            container_of(n, Entry, node)->str = val;
        } else {
            Entry *ent = new Entry();
            ent->type  = T_STR; ent->key = key;
            ent->node.hcode = lk.node.hcode; ent->str = val;
            hm_insert(&g.db, &ent->node);
        }
    }
}


// ── main ──────────────────────────────────────────────────────────────────────

int main() {
    dlist_init(&g.idle_list);
    seqlock_init(&g.rcu);
    thread_pool_init(&g.thread_pool, 4);
    el_init(&g.el);

    // ── Init multithreaded GET dispatch ───────────────────────────────────
    pthread_mutex_init(&g.result_mu, NULL);
    if (pipe(g.wakeup_pipe) != 0) die("pipe()");
    fd_set_nb(g.wakeup_pipe[0]);   // non-blocking so drain_results() won't stall
    fd_set_nb(g.wakeup_pipe[1]);   // non-blocking so workers never block on write

    // ── Restore state from disk ───────────────────────────────────────────
    g.replaying = true;
    mmap_open(&g.mstore, "redis.dat", mmap_replay);
    wal_open(&g.wal, "redis.wal", wal_replay);
    g.replaying = false;
    fprintf(stderr, "restored %zu keys from snapshot + WAL\n", hm_size(&g.db));

    // ── Listening socket ──────────────────────────────────────────────────
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) die("socket()");
    int val = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
    struct sockaddr_in addr = {};
    addr.sin_family      = AF_INET;
    addr.sin_port        = ntohs(1234);
    addr.sin_addr.s_addr = ntohl(0);
    if (bind(fd, (const sockaddr*)&addr, sizeof(addr))) die("bind()");
    fd_set_nb(fd);
    if (listen(fd, SOMAXCONN)) die("listen()");
    g.listen_fd = fd;

    // Register the listen fd and the wakeup pipe read end with the event loop.
    el_add(&g.el, fd, EV_READ, (void*)(intptr_t)fd);
    el_add(&g.el, g.wakeup_pipe[0], EV_READ, (void*)(intptr_t)g.wakeup_pipe[0]);

    fprintf(stderr,
            "listening on :1234  (kqueue/epoll + WAL + mmap + MT-GET)\n");

    // ── Event loop ────────────────────────────────────────────────────────
    ELEvent events[1024];
    while (true) {
        int32_t timeout_ms = next_timer_ms();
        int n = el_wait(&g.el, events, 1024, timeout_ms);

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
    return 0;
}
