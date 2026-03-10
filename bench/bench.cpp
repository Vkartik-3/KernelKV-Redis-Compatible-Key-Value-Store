// Redis-clone benchmark — measures latency and throughput.
//
// Usage:
//   ./bench [host] [port] [ops] [pipeline]
//
// Defaults:
//   host     = 127.0.0.1
//   port     = 1234
//   ops      = 100000
//   pipeline = 16   (requests in-flight before reading responses)
//
// Output (to stdout):
//   CSV header + one row of results:
//     ops,pipeline,total_ms,qps,p50_us,p99_us,p999_us
//
// Methodology:
//   • Each "batch" sends `pipeline` SET requests then reads all responses.
//   • Latency is recorded per-batch (not per-request) to avoid coordinated
//     omission — see Gil Tene's "How NOT to measure latency".
//   • Histogram uses 1 µs buckets up to 1 s (1 000 000 buckets × 4 bytes
//     = 4 MB, acceptable).
//
// Wire protocol (same as the server):
//   Request:  [total_len:4][nstr:4][len0:4][str0][len1:4][str1]...
//   Response: [total_len:4][tag:1][...]

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <math.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>

#include <string>
#include <vector>
#include <algorithm>
#include <numeric>

#include "gpu_profiler.h"

// ── Timing ────────────────────────────────────────────────────────────────────

static uint64_t now_us() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000u + (uint64_t)ts.tv_nsec / 1000u;
}

// ── Latency histogram ─────────────────────────────────────────────────────────

struct Histogram {
    static const int k_buckets = 1000000; // 1 µs … 1 s
    uint32_t counts[k_buckets] = {};
    uint64_t total_count = 0;

    void record(uint64_t us) {
        size_t b = (size_t)(us < (uint64_t)k_buckets ? us : k_buckets-1);
        counts[b]++;
        total_count++;
    }

    // Returns the latency (µs) at the given percentile [0..1].
    uint64_t percentile(double p) const {
        uint64_t target = (uint64_t)(p * (double)total_count);
        uint64_t cum = 0;
        for (int i = 0; i < k_buckets; i++) {
            cum += counts[i];
            if (cum > target) return (uint64_t)i;
        }
        return k_buckets - 1;
    }
};

// ── Network helpers ───────────────────────────────────────────────────────────

static int tcp_connect(const char *host, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); exit(1); }

    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
        fprintf(stderr, "bad host: %s\n", host); exit(1);
    }
    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
        perror("connect"); exit(1);
    }
    return fd;
}

static void write_all(int fd, const void *buf, size_t len) {
    const char *p = (const char*)buf;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n <= 0) { perror("write"); exit(1); }
        p += n; len -= (size_t)n;
    }
}

static void read_all(int fd, void *buf, size_t len) {
    char *p = (char*)buf;
    while (len > 0) {
        ssize_t n = read(fd, p, len);
        if (n <= 0) { perror("read"); exit(1); }
        p += n; len -= (size_t)n;
    }
}

// ── Wire protocol encoder ─────────────────────────────────────────────────────
// Encodes: [total_body_len:4][nstr:4][len:4][str]...

static std::vector<uint8_t> encode_request(const std::vector<std::string> &args) {
    // Build body: [nstr:4][len:4][str]...
    std::vector<uint8_t> body;
    auto append_u32 = [&](uint32_t v) {
        body.insert(body.end(), (uint8_t*)&v, (uint8_t*)&v+4);
    };
    append_u32((uint32_t)args.size());
    for (const auto &a : args) {
        append_u32((uint32_t)a.size());
        body.insert(body.end(), a.begin(), a.end());
    }
    // Prepend 4-byte length header.
    std::vector<uint8_t> out(4 + body.size());
    uint32_t blen = (uint32_t)body.size();
    memcpy(out.data(), &blen, 4);
    memcpy(out.data()+4, body.data(), body.size());
    return out;
}

// Skip one response from the fd (read and discard).
static void skip_response(int fd) {
    uint32_t len = 0;
    read_all(fd, &len, 4);
    if (len > 4*1024*1024u) { fprintf(stderr,"response too large\n"); exit(1); }
    std::vector<uint8_t> buf(len);
    read_all(fd, buf.data(), len);
}

// ── Benchmark ─────────────────────────────────────────────────────────────────

// out_latency_us : per-batch GET latency timeline (µs), for GPU correlation.
// out_bench_start_ms: wall-clock ms at start of GET pass.
static void run_bench(int fd, int total_ops, int pipeline,
                      std::vector<uint64_t> *out_latency_us,
                      uint64_t              *out_bench_start_ms) {
    Histogram hist;

    // Pre-build a SET request: "set bench_key bench_value"
    auto req = encode_request({"set", "bench_key", "bench_val_xxxxxxxxxxxxxxxx"});

    int sent = 0, recvd = 0;

    while (recvd < total_ops) {
        // Fill the pipeline.
        uint64_t t0 = now_us();
        int batch = 0;
        while (sent < total_ops && batch < pipeline) {
            write_all(fd, req.data(), req.size());
            sent++;
            batch++;
        }

        // Drain responses for this batch.
        for (int i = 0; i < batch; i++) {
            skip_response(fd);
            recvd++;
        }
        uint64_t elapsed = now_us() - t0;

        // Record one latency sample per batch (amortised over pipeline depth).
        hist.record(elapsed);
    }

    // ── Throughput measurement (second pass, GET) ─────────────────────────
    auto get_req = encode_request({"get", "bench_key"});
    int  get_ops = total_ops;

    // Record wall-clock start for GPU sample alignment.
    {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        *out_bench_start_ms = (uint64_t)ts.tv_sec * 1000u
                            + (uint64_t)ts.tv_nsec / 1000000u;
    }

    uint64_t thr_start = now_us();

    sent = recvd = 0;
    while (recvd < get_ops) {
        uint64_t b0 = now_us();
        int batch = 0;
        while (sent < get_ops && batch < pipeline) {
            write_all(fd, get_req.data(), get_req.size());
            sent++; batch++;
        }
        for (int i = 0; i < batch; i++) { skip_response(fd); recvd++; }
        out_latency_us->push_back(now_us() - b0);
    }

    uint64_t thr_us  = now_us() - thr_start;
    double   thr_ms  = (double)thr_us / 1000.0;
    double   qps     = (double)get_ops / ((double)thr_us / 1e6);

    // ── Report ────────────────────────────────────────────────────────────
    printf("ops,pipeline,total_ms,qps,p50_us,p99_us,p999_us\n");
    printf("%d,%d,%.1f,%.0f,%llu,%llu,%llu\n",
           total_ops, pipeline, thr_ms, qps,
           (unsigned long long)hist.percentile(0.50),
           (unsigned long long)hist.percentile(0.99),
           (unsigned long long)hist.percentile(0.999));

    fprintf(stderr, "\n── Benchmark Results ───────────────────────────────────\n");
    fprintf(stderr, "  ops          : %d SET + %d GET\n", total_ops, get_ops);
    fprintf(stderr, "  pipeline     : %d\n", pipeline);
    fprintf(stderr, "  throughput   : %.0f ops/s\n", qps);
    fprintf(stderr, "  latency p50  : %llu µs\n",
            (unsigned long long)hist.percentile(0.50));
    fprintf(stderr, "  latency p99  : %llu µs\n",
            (unsigned long long)hist.percentile(0.99));
    fprintf(stderr, "  latency p99.9: %llu µs\n",
            (unsigned long long)hist.percentile(0.999));
    fprintf(stderr, "────────────────────────────────────────────────────────\n");
}

// ── main ──────────────────────────────────────────────────────────────────────

int main(int argc, char **argv) {
    const char *host    = (argc>1) ? argv[1] : "127.0.0.1";
    int         port    = (argc>2) ? atoi(argv[2]) : 1234;
    int         ops     = (argc>3) ? atoi(argv[3]) : 100000;
    int         pipeln  = (argc>4) ? atoi(argv[4]) : 16;

    if (ops    < 1)    ops    = 100000;
    if (pipeln < 1)    pipeln = 1;
    if (pipeln > 1024) pipeln = 1024;

    fprintf(stderr, "connecting to %s:%d  ops=%d  pipeline=%d\n",
            host, port, ops, pipeln);

    int fd = tcp_connect(host, port);
    fprintf(stderr, "connected\n");

    // ── GPU profiler (no-op when built without HAVE_GPU) ──────────────────
    GpuProfiler gpu = {};
    gpu_profiler_init(&gpu);
    gpu_profiler_start(&gpu);

    std::vector<uint64_t> latency_timeline;
    uint64_t              bench_start_ms = 0;

    run_bench(fd, ops, pipeln, &latency_timeline, &bench_start_ms);

    gpu_profiler_stop(&gpu);

    // ── GPU correlation CSV ───────────────────────────────────────────────
    fprintf(stderr, "\n── GPU / Latency Correlation ────────────────────────────\n");
    gpu_profiler_report(&gpu, latency_timeline, bench_start_ms);

    gpu_profiler_destroy(&gpu);

    close(fd);
    return 0;
}
