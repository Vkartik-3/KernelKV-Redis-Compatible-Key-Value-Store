#pragma once

#include <stdint.h>
#include <string>
#include <vector>
#include <functional>

// Write-Ahead Log (WAL) for crash recovery.
//
// Every mutation is appended to a flat file before being applied to the
// in-memory data structures.  Each record is flushed with fdatasync() so it
// survives a process crash.
//
// Record on-disk layout:
//   [ payload_len : uint32 ][ crc32 : uint32 ][ op : uint8 ]
//   [ nargs : uint32 ][ arg0_len : uint32 ][ arg0 bytes ] ...
//
// On startup call wal_open() with a replay callback; it iterates every valid
// record and invokes the callback so callers can reconstruct state.
// If the tail record has a bad CRC (partial write before crash) it is
// truncated and replay stops — no data is lost for any fully written record.
//
// After a checkpoint (state has been written to the mmap snapshot) call
// wal_checkpoint() to truncate the log back to zero.

enum WALOp : uint8_t {
    WAL_SET    = 1,
    WAL_DEL    = 2,
    WAL_ZADD   = 3,
    WAL_ZREM   = 4,
    WAL_EXPIRE = 5,
};

using WALReplayFn =
    std::function<void(WALOp op, const std::vector<std::string> &args)>;

struct WAL {
    int      fd        = -1;
    uint64_t file_size = 0;
};

// Open or create the WAL at `path`.  Existing records are replayed via
// `replay_fn` (pass nullptr to skip replay).
void wal_open(WAL *wal, const char *path, WALReplayFn replay_fn);

// Append one record and call fdatasync() before returning.
void wal_write(WAL *wal, WALOp op, const std::vector<std::string> &args);

// Truncate the WAL to zero (call after a successful mmap checkpoint).
void wal_checkpoint(WAL *wal);

void wal_close(WAL *wal);
