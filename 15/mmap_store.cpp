#include "mmap_store.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

// Growth quantum: extend the file in 4 MB chunks to amortise ftruncate cost.
static const size_t k_grow_step = 4u * 1024u * 1024u;

// Page size (standard on x86-64 and Apple Silicon).
static const size_t k_page = 4096u;

static size_t page_align(size_t n) {
    return (n + k_page - 1) & ~(k_page - 1);
}

// ── Internal: grow the mapped file to at least `min_size` bytes ───────────────
//
// Strategy:
//   1. ftruncate() to the new (page-aligned) size — extends the file with
//      zero bytes.
//   2. munmap() the old region (its backing file is already extended).
//   3. mmap() the larger region.
//
// mremap() is Linux-only; munmap+mmap works on both Linux and macOS.

static void grow(MMapStore *ms, size_t min_size) {
    size_t new_size = ms->file_size ? ms->file_size : k_grow_step;
    while (new_size < min_size)
        new_size += k_grow_step;
    new_size = page_align(new_size);

    // Extend the backing file.
    if (ftruncate(ms->fd, (off_t)new_size) != 0) {
        perror("mmap_store: ftruncate()");
        abort();
    }

    // Unmap the old region (if any) and map the new one.
    if (ms->base) {
        munmap(ms->base, ms->file_size);
        ms->base = nullptr;
    }
    ms->base = mmap(NULL, new_size,
                    PROT_READ | PROT_WRITE, MAP_SHARED, ms->fd, 0);
    if (ms->base == MAP_FAILED) {
        perror("mmap_store: mmap()");
        abort();
    }
    ms->file_size = new_size;
}

// ── Write helpers ─────────────────────────────────────────────────────────────

static void store_u32(uint8_t *p, uint32_t v) { memcpy(p, &v, 4); }
static void store_u64(uint8_t *p, uint64_t v) { memcpy(p, &v, 8); }

// Ensure at least `needed` bytes are available after write_pos, growing if not.
static uint8_t *reserve(MMapStore *ms, size_t needed) {
    if (ms->write_pos + needed > ms->file_size)
        grow(ms, ms->write_pos + needed);
    return (uint8_t *)ms->base + ms->write_pos;
}

static void bump_count(MMapStore *ms) {
    ms->record_count++;
    store_u64((uint8_t *)ms->base + 8, ms->record_count);
}

// ── Public API ────────────────────────────────────────────────────────────────

void mmap_open(MMapStore *ms, const char *path, MMapReplayFn replay_fn) {
    ms->fd = open(path, O_RDWR | O_CREAT, 0644);
    if (ms->fd < 0) {
        perror("mmap_store: open()");
        abort();
    }

    off_t existing = lseek(ms->fd, 0, SEEK_END);

    if (existing < (off_t)MMAP_HEADER_SIZE) {
        // Brand-new file — write header and initial blank region.
        grow(ms, k_grow_step);
        uint8_t *p = (uint8_t *)ms->base;
        memcpy(p, MMAP_MAGIC, 8);
        store_u64(p + 8, 0);
        ms->write_pos    = MMAP_HEADER_SIZE;
        ms->record_count = 0;
        msync(ms->base, MMAP_HEADER_SIZE, MS_SYNC);
        return;
    }

    // Existing file — map it.
    ms->file_size = (size_t)existing;
    ms->base = mmap(NULL, ms->file_size,
                    PROT_READ | PROT_WRITE, MAP_SHARED, ms->fd, 0);
    if (ms->base == MAP_FAILED) {
        perror("mmap_store: mmap()");
        abort();
    }

    uint8_t *base = (uint8_t *)ms->base;
    if (memcmp(base, MMAP_MAGIC, 8) != 0) {
        fprintf(stderr, "mmap_store: bad magic — ignoring existing file\n");
        ms->write_pos    = MMAP_HEADER_SIZE;
        ms->record_count = 0;
        return;
    }
    memcpy(&ms->record_count, base + 8, 8);

    // Replay records.
    size_t pos = MMAP_HEADER_SIZE;
    while (pos + 4 <= ms->file_size) {
        uint32_t key_len = 0;
        memcpy(&key_len, base + pos, 4);
        pos += 4;
        if (key_len == 0 || pos + key_len > ms->file_size) break;

        std::string key((char *)base + pos, key_len);
        pos += key_len;

        if (pos + 1 > ms->file_size) break;
        uint8_t type = base[pos++];

        if (type == 0) {
            // Tombstone — report as deleted.
            if (replay_fn) replay_fn(key, "");
            continue;
        }

        // type == 1: string value.
        if (pos + 4 > ms->file_size) break;
        uint32_t val_len = 0;
        memcpy(&val_len, base + pos, 4);
        pos += 4;
        if (pos + val_len > ms->file_size) break;

        std::string val((char *)base + pos, val_len);
        pos += val_len;

        if (replay_fn) replay_fn(key, val);
    }
    ms->write_pos = pos;
}

void mmap_set(MMapStore *ms, const std::string &key, const std::string &val) {
    // Record: [key_len:4][key][type=1:1][val_len:4][val]
    size_t rec = 4 + key.size() + 1 + 4 + val.size();
    uint8_t *p = reserve(ms, rec);

    uint32_t kl = (uint32_t)key.size();
    uint32_t vl = (uint32_t)val.size();
    store_u32(p, kl);               p += 4;
    memcpy(p, key.data(), kl);      p += kl;
    *p++ = 1;                       // type = string
    store_u32(p, vl);               p += 4;
    memcpy(p, val.data(), vl);

    ms->write_pos += rec;
    bump_count(ms);

    // Async flush — the OS will write dirty pages in the background.
    msync((uint8_t *)ms->base + ms->write_pos - rec, rec, MS_ASYNC);
}

void mmap_del(MMapStore *ms, const std::string &key) {
    // Tombstone: [key_len:4][key][type=0:1]
    size_t rec = 4 + key.size() + 1;
    uint8_t *p = reserve(ms, rec);

    uint32_t kl = (uint32_t)key.size();
    store_u32(p, kl);               p += 4;
    memcpy(p, key.data(), kl);      p += kl;
    *p = 0;                         // tombstone

    ms->write_pos += rec;
    bump_count(ms);

    msync((uint8_t *)ms->base + ms->write_pos - rec, rec, MS_ASYNC);
}

void mmap_compact(MMapStore *ms,
                  std::function<void(MMapStore *)> enumerate_live_keys) {
    // Flush whatever we have, then truncate to the used portion.
    msync(ms->base, ms->write_pos, MS_SYNC);
    munmap(ms->base, ms->file_size);
    ms->base = nullptr;

    // Truncate to just the valid data and remap.
    ftruncate(ms->fd, (off_t)ms->write_pos);
    ms->file_size = ms->write_pos;
    ms->base = mmap(NULL, ms->file_size,
                    PROT_READ | PROT_WRITE, MAP_SHARED, ms->fd, 0);
    if (ms->base == MAP_FAILED) {
        perror("mmap_store: mmap() in compact");
        abort();
    }

    // Reset write cursor to header end, then re-write only live keys.
    ms->write_pos    = MMAP_HEADER_SIZE;
    ms->record_count = 0;
    store_u64((uint8_t *)ms->base + 8, 0);

    // Caller provides a function that iterates the live HMap and calls
    // mmap_set() for each live string key-value pair.
    enumerate_live_keys(ms);

    msync(ms->base, ms->write_pos, MS_SYNC);
}

void mmap_close(MMapStore *ms) {
    if (ms->base) {
        msync(ms->base, ms->write_pos, MS_SYNC);
        munmap(ms->base, ms->file_size);
        ms->base = nullptr;
    }
    if (ms->fd >= 0) {
        close(ms->fd);
        ms->fd = -1;
    }
}
