#include "gpu_profiler.h"

#include <pthread.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <algorithm>
#include <vector>

#ifdef HAVE_GPU
#  include <nvml.h>
#endif

// ── Monotonic clock (ms) ──────────────────────────────────────────────────────

static uint64_t mono_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000u + (uint64_t)ts.tv_nsec / 1000000u;
}

// ── Internal impl struct ──────────────────────────────────────────────────────

struct GpuProfilerImpl {
    pthread_t thread;
    volatile bool stop = false;
    bool      have_gpu = false;
    GpuProfiler *owner = nullptr;   // back-pointer to public struct

#ifdef HAVE_GPU
    nvmlDevice_t device;
#endif
};

// ── Background sampling thread ────────────────────────────────────────────────

static void *sampler_thread(void *arg) {
    GpuProfilerImpl *impl = (GpuProfilerImpl *)arg;

    while (!impl->stop) {
        GpuSample s = {};
        s.timestamp_ms = mono_ms();

#ifdef HAVE_GPU
        if (impl->have_gpu) {
            // GPU compute + memory-controller utilization.
            nvmlUtilization_t util = {};
            nvmlDeviceGetUtilizationRates(impl->device, &util);
            s.gpu_util_pct = util.gpu;
            s.mem_util_pct = util.memory;

            // Framebuffer memory usage.
            nvmlMemory_t mem = {};
            nvmlDeviceGetMemoryInfo(impl->device, &mem);
            s.mem_used_mb = mem.used / (1024u * 1024u);

            // Board power in milliwatts → watts.
            unsigned int power_mw = 0;
            nvmlDeviceGetPowerUsage(impl->device, &power_mw);
            s.power_watts = (double)power_mw / 1000.0;
        }
#endif

        impl->owner->samples.push_back(s);

        // Sample every 100 ms.
        usleep(100000);
    }
    return NULL;
}

// ── Public API ────────────────────────────────────────────────────────────────

bool gpu_profiler_init(GpuProfiler *p) {
    p->_impl = nullptr;

    GpuProfilerImpl *impl = new GpuProfilerImpl();
    impl->owner   = p;
    impl->have_gpu = false;

#ifdef HAVE_GPU
    // nvmlInit() returns NVML_SUCCESS even when a GPU is present but the
    // driver is not running, so we also check DeviceGetHandleByIndex.
    nvmlReturn_t rc = nvmlInit();
    if (rc != NVML_SUCCESS) {
        fprintf(stderr, "gpu_profiler: nvmlInit failed: %s — CPU-only mode\n",
                nvmlErrorString(rc));
        p->_impl = impl;
        return false;
    }
    rc = nvmlDeviceGetHandleByIndex(0, &impl->device);
    if (rc != NVML_SUCCESS) {
        fprintf(stderr, "gpu_profiler: no GPU at index 0: %s — CPU-only mode\n",
                nvmlErrorString(rc));
        nvmlShutdown();
        p->_impl = impl;
        return false;
    }
    impl->have_gpu = true;
    char name[96] = {};
    nvmlDeviceGetName(impl->device, name, sizeof(name));
    fprintf(stderr, "gpu_profiler: attached to GPU 0: %s\n", name);
    p->_impl = impl;
    return true;
#else
    // Built without -DHAVE_GPU: NVML not linked.
    fprintf(stderr, "gpu_profiler: built without HAVE_GPU — CPU-only mode\n");
    p->_impl = impl;
    return false;
#endif
}

void gpu_profiler_start(GpuProfiler *p) {
    if (!p->_impl) return;
    GpuProfilerImpl *impl = (GpuProfilerImpl *)p->_impl;
    impl->stop = false;
    p->samples.clear();
    pthread_create(&impl->thread, NULL, sampler_thread, impl);
}

void gpu_profiler_stop(GpuProfiler *p) {
    if (!p->_impl) return;
    GpuProfilerImpl *impl = (GpuProfilerImpl *)p->_impl;
    impl->stop = true;
    pthread_join(impl->thread, NULL);
}

// ── Correlation report ────────────────────────────────────────────────────────
//
// For each 100 ms GPU sampling window, we find all benchmark batches that
// completed within that window and compute:
//   - ops_in_window (batch count × pipeline depth is not available here so
//     we report batch count; caller multiplied by pipeline to get op count)
//   - p99 latency among batches in that window
//
// Timeline alignment:
//   GPU sample i was taken at samples[i].timestamp_ms.
//   Benchmark batch j started at bench_start_ms + cumulative_offset_j.
//   We reconstruct per-batch absolute finish time as:
//     bench_start_ms + sum(latency_timeline_us[0..j]) / 1000
//   and assign each batch to the GPU window that contains its finish time.

void gpu_profiler_report(const GpuProfiler *p,
                         const std::vector<uint64_t> &latency_timeline_us,
                         uint64_t bench_start_ms) {
    printf("timestamp_ms,ops_in_window,gpu_util_pct,mem_used_mb,"
           "power_watts,p99_us_in_window\n");

    if (p->samples.empty()) {
        // No GPU samples — emit one row per 100 ms using zero GPU metrics.
        uint64_t total_us  = 0;
        for (auto v : latency_timeline_us) total_us += v;
        uint64_t total_ms  = total_us / 1000;
        uint64_t n_windows = (total_ms + 99) / 100;

        size_t batch_idx   = 0;
        uint64_t cursor_us = 0;

        for (uint64_t w = 0; w < n_windows; w++) {
            uint64_t win_start_ms = bench_start_ms + w * 100;
            uint64_t win_end_ms   = win_start_ms + 100;

            std::vector<uint64_t> window_lat;
            while (batch_idx < latency_timeline_us.size()) {
                cursor_us += latency_timeline_us[batch_idx];
                uint64_t finish_ms = bench_start_ms + cursor_us / 1000;
                if (finish_ms >= win_end_ms) {
                    cursor_us -= latency_timeline_us[batch_idx];
                    break;
                }
                window_lat.push_back(latency_timeline_us[batch_idx]);
                batch_idx++;
            }

            uint64_t p99 = 0;
            if (!window_lat.empty()) {
                std::vector<uint64_t> sorted = window_lat;
                std::sort(sorted.begin(), sorted.end());
                p99 = sorted[(size_t)(sorted.size() * 0.99)];
            }

            printf("%llu,%zu,0,0,0.0,%llu\n",
                   (unsigned long long)win_start_ms,
                   window_lat.size(),
                   (unsigned long long)p99);
        }
        return;
    }

    // GPU samples exist — correlate.
    size_t batch_idx   = 0;
    uint64_t cursor_us = 0;

    for (size_t si = 0; si < p->samples.size(); si++) {
        const GpuSample &s = p->samples[si];
        uint64_t win_start_ms = s.timestamp_ms;
        uint64_t win_end_ms   = (si + 1 < p->samples.size())
                                    ? p->samples[si+1].timestamp_ms
                                    : win_start_ms + 100;

        std::vector<uint64_t> window_lat;
        while (batch_idx < latency_timeline_us.size()) {
            uint64_t next_us = cursor_us + latency_timeline_us[batch_idx];
            uint64_t finish_ms = bench_start_ms + next_us / 1000;
            if (finish_ms >= win_end_ms) break;
            cursor_us = next_us;
            window_lat.push_back(latency_timeline_us[batch_idx]);
            batch_idx++;
        }

        uint64_t p99 = 0;
        if (!window_lat.empty()) {
            std::vector<uint64_t> sorted = window_lat;
            std::sort(sorted.begin(), sorted.end());
            p99 = sorted[(size_t)(sorted.size() * 0.99)];
        }

        printf("%llu,%zu,%u,%llu,%.1f,%llu\n",
               (unsigned long long)s.timestamp_ms,
               window_lat.size(),
               s.gpu_util_pct,
               (unsigned long long)s.mem_used_mb,
               s.power_watts,
               (unsigned long long)p99);
    }
}

void gpu_profiler_destroy(GpuProfiler *p) {
    if (!p->_impl) return;
    GpuProfilerImpl *impl = (GpuProfilerImpl *)p->_impl;

#ifdef HAVE_GPU
    if (impl->have_gpu) nvmlShutdown();
#endif

    delete impl;
    p->_impl = nullptr;
}
