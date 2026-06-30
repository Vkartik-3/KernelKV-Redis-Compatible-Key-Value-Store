// Unit tests for the write-ahead log: append/sync (group commit), replay,
// CRC-mismatch truncation, and partial-tail recovery.
#include <string>
#include <vector>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <cstdio>

#include "../engine/wal.h"
#include "test_util.h"

namespace {

struct Rec {
    WALOp                    op;
    std::vector<std::string> args;
};

std::string tmp_path() {
    return "test_wal_" + std::to_string(getpid()) + ".tmp";
}

std::vector<Rec> replay_all(const std::string &path) {
    std::vector<Rec> out;
    WAL wal;
    wal_open(&wal, path.c_str(),
             [&](WALOp op, const std::vector<std::string> &args) {
                 out.push_back(Rec{op, args});
             });
    wal_close(&wal);
    return out;
}

off_t file_size(const std::string &path) {
    struct stat st{};
    return stat(path.c_str(), &st) == 0 ? st.st_size : -1;
}

// A SET written, synced, and replayed must come back identical.
void test_append_sync_replay() {
    std::string path = tmp_path();
    unlink(path.c_str());

    WAL wal;
    wal_open(&wal, path.c_str(), nullptr);
    wal_append(&wal, WAL_SET, {"k1", "v1"});
    wal_append(&wal, WAL_SET, {"k2", "v2"});
    wal_sync(&wal);                       // group commit: one fsync for both
    wal_close(&wal);

    std::vector<Rec> recs = replay_all(path);
    CHECK_EQ(recs.size(), 2);
    if (recs.size() == 2) {
        CHECK(recs[0].op == WAL_SET);
        CHECK(recs[0].args.size() == 2 && recs[0].args[0] == "k1" && recs[0].args[1] == "v1");
        CHECK(recs[1].args[0] == "k2" && recs[1].args[1] == "v2");
    }
    unlink(path.c_str());
}

// wal_sync with no pending records is a no-op; a synced batch is durable.
void test_group_commit_durability() {
    std::string path = tmp_path();
    unlink(path.c_str());

    WAL wal;
    wal_open(&wal, path.c_str(), nullptr);
    wal_sync(&wal);                       // nothing pending — must not crash/grow
    CHECK_EQ(file_size(path), 0);

    for (int i = 0; i < 10; i++)
        wal_append(&wal, WAL_SET, {"key" + std::to_string(i), "val"});
    wal_sync(&wal);                       // single fsync for all 10
    wal_close(&wal);

    CHECK_EQ(replay_all(path).size(), 10);
    unlink(path.c_str());
}

// A flipped byte in a record's payload must be caught by CRC; that record and
// everything after it is truncated on replay.
void test_crc_truncation() {
    std::string path = tmp_path();
    unlink(path.c_str());

    WAL wal;
    wal_open(&wal, path.c_str(), nullptr);
    wal_write(&wal, WAL_SET, {"good", "value"});   // append + sync
    wal_close(&wal);

    // Corrupt one byte inside the payload (layout: [len:4][crc:4][payload...]).
    int fd = open(path.c_str(), O_RDWR);
    CHECK(fd >= 0);
    uint8_t b = 0;
    pread(fd, &b, 1, 9);
    b ^= 0xFF;
    pwrite(fd, &b, 1, 9);
    close(fd);

    // Replay must reject the corrupted record (CRC mismatch → truncate to 0).
    CHECK_EQ(replay_all(path).size(), 0);
    CHECK_EQ(file_size(path), 0);
    unlink(path.c_str());
}

// A torn final record (header promises more bytes than exist) is dropped, but
// every fully written record before it survives.
void test_partial_tail_truncation() {
    std::string path = tmp_path();
    unlink(path.c_str());

    WAL wal;
    wal_open(&wal, path.c_str(), nullptr);
    wal_write(&wal, WAL_SET, {"k1", "v1"});
    wal_write(&wal, WAL_SET, {"k2", "v2"});
    wal_close(&wal);
    off_t good_size = file_size(path);

    // Append a bogus header claiming a 100-byte payload, then only 4 bytes.
    int fd = open(path.c_str(), O_WRONLY | O_APPEND);
    CHECK(fd >= 0);
    uint32_t hdr[2] = {100u, 0u};
    (void)!write(fd, hdr, 8);
    uint8_t junk[4] = {1, 2, 3, 4};
    (void)!write(fd, junk, 4);
    close(fd);
    CHECK(file_size(path) > good_size);

    // Replay keeps the two good records and truncates the torn tail.
    std::vector<Rec> recs = replay_all(path);
    CHECK_EQ(recs.size(), 2);
    CHECK_EQ(file_size(path), good_size);
    unlink(path.c_str());
}

// Checkpoint truncates the log to zero (state is safe in the snapshot).
void test_checkpoint() {
    std::string path = tmp_path();
    unlink(path.c_str());

    WAL wal;
    wal_open(&wal, path.c_str(), nullptr);
    wal_write(&wal, WAL_SET, {"k", "v"});
    CHECK(file_size(path) > 0);
    wal_checkpoint(&wal);
    CHECK_EQ(file_size(path), 0);
    wal_close(&wal);

    CHECK_EQ(replay_all(path).size(), 0);
    unlink(path.c_str());
}

} // namespace

int main() {
    fprintf(stderr, "test_wal\n");
    RUN(test_append_sync_replay);
    RUN(test_group_commit_durability);
    RUN(test_crc_truncation);
    RUN(test_partial_tail_truncation);
    RUN(test_checkpoint);
    return test_summary();
}
