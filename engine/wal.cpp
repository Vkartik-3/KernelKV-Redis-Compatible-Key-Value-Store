#include "wal.h"

#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>

// macOS doesn't expose fdatasync via its standard headers; fsync() is equivalent.
#ifdef __APPLE__
#  define fdatasync(fd) fsync(fd)
#endif

// ── CRC-32 (ISO 3309 / Ethernet polynomial 0xEDB88320) ───────────────────────

static uint32_t g_crc_table[256];

static void crc32_init_table() {
    static bool done = false;
    if (done) return;
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int j = 0; j < 8; j++)
            c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        g_crc_table[i] = c;
    }
    done = true;
}

static uint32_t crc32(const uint8_t *data, size_t len) {
    crc32_init_table();
    uint32_t c = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; i++)
        c = g_crc_table[(c ^ data[i]) & 0xFF] ^ (c >> 8);
    return c ^ 0xFFFFFFFFu;
}

// ── Low-level I/O helpers ─────────────────────────────────────────────────────

static void write_all(int fd, const void *buf, size_t len) {
    const char *p = (const char *)buf;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        assert(n > 0);
        p   += n;
        len -= (size_t)n;
    }
}

// ── WAL implementation ────────────────────────────────────────────────────────

void wal_open(WAL *wal, const char *path, WALReplayFn replay_fn) {
    wal->fd = open(path, O_RDWR | O_CREAT, 0644);
    if (wal->fd < 0) {
        perror("wal_open: open()");
        abort();
    }

    wal->file_size = (uint64_t)lseek(wal->fd, 0, SEEK_END);
    lseek(wal->fd, 0, SEEK_SET);

    // Replay every valid record from the beginning of the file.
    off_t pos = 0;
    while (replay_fn && pos + 8 <= (off_t)wal->file_size) {
        uint32_t payload_len = 0, stored_crc = 0;

        if (pread(wal->fd, &payload_len, 4, pos)     != 4) break;
        if (pread(wal->fd, &stored_crc,  4, pos + 4) != 4) break;
        if ((off_t)(pos + 8 + payload_len) > (off_t)wal->file_size) {
            // Partial write at the tail — truncate and stop.
            fprintf(stderr, "WAL: truncating partial tail record at offset %lld\n",
                    (long long)pos);
            ftruncate(wal->fd, pos);
            wal->file_size = (uint64_t)pos;
            break;
        }

        // Read and verify payload.
        std::vector<uint8_t> payload(payload_len);
        if (pread(wal->fd, payload.data(), payload_len, pos + 8)
                != (ssize_t)payload_len) break;

        uint32_t computed = crc32(payload.data(), payload_len);
        if (computed != stored_crc) {
            fprintf(stderr, "WAL: CRC mismatch at offset %lld — truncating\n",
                    (long long)pos);
            ftruncate(wal->fd, pos);
            wal->file_size = (uint64_t)pos;
            break;
        }

        // Deserialize: [op:1][nargs:4][arg0_len:4][arg0]...
        const uint8_t *p   = payload.data();
        const uint8_t *end = p + payload_len;

        WALOp    op    = (WALOp)*p++;
        uint32_t nargs = 0;
        memcpy(&nargs, p, 4); p += 4;

        std::vector<std::string> args;
        bool ok = true;
        for (uint32_t i = 0; i < nargs; i++) {
            if (p + 4 > end) { ok = false; break; }
            uint32_t alen = 0;
            memcpy(&alen, p, 4); p += 4;
            if (p + alen > end) { ok = false; break; }
            args.emplace_back((const char *)p, alen);
            p += alen;
        }

        if (ok)
            replay_fn(op, args);

        pos += 8 + (off_t)payload_len;
    }

    // Position the write cursor at the end of the valid data.
    lseek(wal->fd, (off_t)wal->file_size, SEEK_SET);
}

void wal_write(WAL *wal, WALOp op, const std::vector<std::string> &args) {
    // Serialize payload: [op:1][nargs:4][arg_len:4][arg]...
    std::vector<uint8_t> payload;
    payload.push_back((uint8_t)op);
    uint32_t nargs = (uint32_t)args.size();
    payload.insert(payload.end(),
                   (uint8_t *)&nargs, (uint8_t *)&nargs + 4);
    for (const auto &a : args) {
        uint32_t alen = (uint32_t)a.size();
        payload.insert(payload.end(),
                       (uint8_t *)&alen, (uint8_t *)&alen + 4);
        payload.insert(payload.end(),
                       (const uint8_t *)a.data(),
                       (const uint8_t *)a.data() + a.size());
    }

    uint32_t payload_len = (uint32_t)payload.size();
    uint32_t checksum    = crc32(payload.data(), payload_len);

    // Write header then payload in two calls — kernel may coalesce them.
    uint32_t hdr[2] = {payload_len, checksum};
    write_all(wal->fd, hdr,            8);
    write_all(wal->fd, payload.data(), payload_len);

    // Guarantee durability: data reaches stable storage before we return.
    // fdatasync() skips updating atime/mtime metadata — faster than fsync().
    if (fdatasync(wal->fd) != 0) {
        perror("wal_write: fdatasync()");
        abort();
    }

    wal->file_size += 8 + payload_len;
}

void wal_checkpoint(WAL *wal) {
    // State is now safe in the mmap snapshot — truncate the log.
    ftruncate(wal->fd, 0);
    lseek(wal->fd, 0, SEEK_SET);
    fdatasync(wal->fd);
    wal->file_size = 0;
}

void wal_close(WAL *wal) {
    if (wal->fd >= 0) {
        close(wal->fd);
        wal->fd = -1;
    }
}
