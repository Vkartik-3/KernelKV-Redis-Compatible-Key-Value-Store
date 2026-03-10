#pragma once

#include <stdint.h>
#include <vector>

// GPU performance profiler using NVML.
//
// Samples GPU utilization, memory bandwidth, and power draw every 100 ms
// on a background pthread while the benchmark runs on the foreground thread.
//
// Build with -DHAVE_GPU to enable real NVML calls.
// Without that flag every metric is zero (CPU-only graceful fallback).
//
// Usage:
//   GpuProfiler gpu = {};
//   if (!gpu_profiler_init(&gpu)) { /* no GPU */ }
//   gpu_profiler_start(&gpu);
//   run_bench(...);
//   gpu_profiler_stop(&gpu);
//   gpu_profiler_report(&gpu, latency_timeline_us, bench_start_ms);
//   gpu_profiler_destroy(&gpu);

struct GpuSample {
    uint64_t timestamp_ms;   // absolute monotonic time of this sample
    uint32_t gpu_util_pct;   // GPU compute utilization 0-100
    uint32_t mem_util_pct;   // GPU memory controller utilization 0-100
    uint64_t mem_used_mb;    // GPU framebuffer used (MiB)
    double   power_watts;    // GPU board power (watts)
};

struct GpuProfiler {
    std::vector<GpuSample> samples;  // populated by background thread
    void *_impl;                     // opaque — points to GpuProfilerImpl
};

// Returns true if a GPU was found and NVML initialised.
// Returns false gracefully if no GPU, no NVML, or -DHAVE_GPU not set.
bool gpu_profiler_init(GpuProfiler *p);

// Spawn the background sampling thread.
void gpu_profiler_start(GpuProfiler *p);

// Join the background thread (call after the benchmark completes).
void gpu_profiler_stop(GpuProfiler *p);

// Print a correlation CSV to stdout.
// latency_timeline_us: one entry per benchmark batch (elapsed µs).
// bench_start_ms:      absolute monotonic time when the timed pass began.
//
// Output columns:
//   timestamp_ms, ops_in_window, gpu_util_pct, mem_used_mb,
//   power_watts, p99_us_in_window
void gpu_profiler_report(const GpuProfiler *p,
                         const std::vector<uint64_t> &latency_timeline_us,
                         uint64_t bench_start_ms);

void gpu_profiler_destroy(GpuProfiler *p);
