#pragma once

#include <stdint.h>
#include <atomic>

// Seqlock — a lightweight RCU primitive for a single-writer / multiple-reader
// scenario.
//
// How it works
// ────────────
// A shared 64-bit sequence counter drives two invariants:
//   • Even sequence  → no write in progress; readers may proceed.
//   • Odd  sequence  → write in progress;    readers must spin.
//
// Writer protocol (event-loop thread):
//   rcu_write_lock(sl)     // seq becomes odd  → readers see write in progress
//   ... modify shared data ...
//   rcu_write_unlock(sl)   // seq becomes even → write is visible
//
// Reader protocol (worker threads):
//   uint64_t seq;
//   do {
//       seq = rcu_read_begin(sl);   // spin until even, then snapshot seq
//       ... read shared data ...
//   } while (rcu_read_retry(sl, seq));  // retry if a write raced with us
//
// Properties
// ──────────
// • Zero overhead for the writer (two fetch_adds).
// • Readers retry on write collision — correct but not wait-free.
// • No dynamic memory allocation.
// • Works across all C++11 platforms (uses std::atomic).

struct SeqLock {
    std::atomic<uint64_t> seq{0};
};

void     seqlock_init(SeqLock *sl);
void     rcu_write_lock(SeqLock *sl);
void     rcu_write_unlock(SeqLock *sl);
uint64_t rcu_read_begin(SeqLock *sl);   // returns the sequence snapshot
bool     rcu_read_retry(SeqLock *sl, uint64_t seq); // true → must retry
