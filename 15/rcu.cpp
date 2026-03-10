#include "rcu.h"

#include <assert.h>

// On POSIX we can yield the CPU while spinning rather than burning cycles.
#ifdef _WIN32
#  include <windows.h>
#  define cpu_relax()  YieldProcessor()
#else
#  include <sched.h>
#  define cpu_relax()  sched_yield()
#endif

void seqlock_init(SeqLock *sl) {
    sl->seq.store(0, std::memory_order_relaxed);
}

// Writer: begin a write section.  Increments seq to an ODD value.
// Assertion: must not be called while another write is in progress.
void rcu_write_lock(SeqLock *sl) {
    uint64_t prev = sl->seq.fetch_add(1, std::memory_order_release);
    assert((prev & 1) == 0 && "rcu_write_lock: already locked");
    // All stores that follow are visible to any reader that sees the next even
    // sequence value (after rcu_write_unlock).
    (void)prev;
}

// Writer: end a write section.  Increments seq back to an EVEN value.
void rcu_write_unlock(SeqLock *sl) {
    // memory_order_release ensures all prior stores are ordered before the
    // sequence increment that readers use as a "write complete" signal.
    sl->seq.fetch_add(1, std::memory_order_release);
}

// Reader: spin until no write is in progress, then snapshot the sequence.
// The returned snapshot must be passed to rcu_read_retry().
uint64_t rcu_read_begin(SeqLock *sl) {
    uint64_t seq;
    while (true) {
        // memory_order_acquire pairs with the writer's release stores.
        seq = sl->seq.load(std::memory_order_acquire);
        if ((seq & 1) == 0)   // even → no write in progress
            break;
        cpu_relax();
    }
    return seq;
}

// Reader: returns true if a write raced with the current read section.
// If true the caller must discard whatever it read and retry from
// rcu_read_begin().
bool rcu_read_retry(SeqLock *sl, uint64_t seq) {
    // Fence ensures all loads within the read section complete before we
    // sample the sequence again.
    std::atomic_thread_fence(std::memory_order_acquire);
    return sl->seq.load(std::memory_order_relaxed) != seq;
}
