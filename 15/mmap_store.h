#pragma once

#include <stdint.h>
#include <stddef.h>
#include <string>
#include <functional>

// Memory-mapped persistent snapshot store.
//
// The file holds a flat sequence of string key-value records.  The entire
// file is mmap(MAP_SHARED)-ed so the OS can flush dirty pages asynchronously.
// When the file needs to grow we call ftruncate() to extend it and then
// re-mmap the larger region.
//
// File layout:
//   Bytes  0-7   : magic "REDIS001"
//   Bytes  8-15  : record count (uint64, informational only)
//   Bytes 16+    : records
//
// Each record:
//   [ key_len : uint32 ][ key bytes ][ type : uint8 ]
//   type 0 = tombstone (key deleted) — no value follows
//   type 1 = string    — followed by [ val_len : uint32 ][ val bytes ]
//
// On replay the last written entry for each key wins, so a tombstone after
// a set counts as a deletion.  Compact with mmap_snapshot() to eliminate
// stale entries and release disk space.

#define MMAP_MAGIC        "REDIS001"
#define MMAP_HEADER_SIZE  16u       // magic(8) + count(8)

struct MMapStore {
    int     fd           = -1;
    void   *base         = nullptr;
    size_t  file_size    = 0;   // current mapped region size (page-aligned)
    size_t  write_pos    = 0;   // next byte to write within the region
    uint64_t record_count = 0;
};

// Callback called once per live record during startup replay.
// An empty `val` means the key was deleted (tombstone).
using MMapReplayFn =
    std::function<void(const std::string &key, const std::string &val)>;

// Open or create the store at `path`.  Replays existing records via
// `replay_fn` (pass nullptr to skip).
void mmap_open(MMapStore *ms, const char *path, MMapReplayFn replay_fn);

// Append a SET record.  Grows the file with ftruncate() if needed.
void mmap_set(MMapStore *ms, const std::string &key, const std::string &val);

// Append a tombstone (DEL).
void mmap_del(MMapStore *ms, const std::string &key);

// Rewrite the file with only live records and re-map.
// Call this to compact after many deletes, then call wal_checkpoint().
void mmap_compact(MMapStore *ms,
                  std::function<void(MMapStore *)> enumerate_live_keys);

// msync the written region and close.
void mmap_close(MMapStore *ms);
