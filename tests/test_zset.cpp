// Unit tests for the sorted set (ZSet): AVL-by-(score,name) + hash-by-name.
#include <string>
#include <vector>
#include <cstring>

#include "../core/zset.h"
#include "test_util.h"

namespace {

bool insert(ZSet *z, const std::string &name, double score) {
    return zset_insert(z, name.data(), name.size(), score);
}

ZNode *lookup(ZSet *z, const std::string &name) {
    return zset_lookup(z, name.data(), name.size());
}

void test_insert_and_lookup() {
    ZSet z{};
    CHECK(insert(&z, "a", 1.0) == true);   // newly inserted
    CHECK(insert(&z, "b", 2.0) == true);
    CHECK(insert(&z, "c", 3.0) == true);

    ZNode *n = lookup(&z, "b");
    CHECK(n != nullptr);
    CHECK(n && n->score == 2.0);
    CHECK(lookup(&z, "missing") == nullptr);

    zset_clear(&z);
}

void test_score_update() {
    ZSet z{};
    CHECK(insert(&z, "a", 1.0) == true);
    // Re-inserting an existing member updates its score, returns false (not new).
    CHECK(insert(&z, "a", 9.0) == false);

    ZNode *n = lookup(&z, "a");
    CHECK(n && n->score == 9.0);
    zset_clear(&z);
}

void test_delete() {
    ZSet z{};
    insert(&z, "a", 1.0);
    insert(&z, "b", 2.0);

    ZNode *b = lookup(&z, "b");
    CHECK(b != nullptr);
    if (b) zset_delete(&z, b);

    CHECK(lookup(&z, "b") == nullptr);
    CHECK(lookup(&z, "a") != nullptr);
    zset_clear(&z);
}

// seekge + znode_offset should walk members in ascending (score, name) order.
void test_range_order() {
    ZSet z{};
    insert(&z, "c", 3.0);
    insert(&z, "a", 1.0);
    insert(&z, "b", 2.0);
    insert(&z, "e", 5.0);
    insert(&z, "d", 4.0);

    // First node with score >= 2.0 should be "b".
    ZNode *n = zset_seekge(&z, 2.0, "", 0);
    std::vector<std::string> walked;
    while (n) {
        walked.emplace_back(n->name, n->len);
        n = znode_offset(n, +1);
    }
    CHECK_EQ(walked.size(), 4);
    if (walked.size() == 4) {
        CHECK(walked[0] == "b");
        CHECK(walked[1] == "c");
        CHECK(walked[2] == "d");
        CHECK(walked[3] == "e");
    }

    // Offset arithmetic: from "b", +2 lands on "d", -1 on "a".
    ZNode *b = zset_seekge(&z, 2.0, "", 0);
    ZNode *d = znode_offset(b, +2);
    CHECK(d != nullptr);
    CHECK(d && std::string(d->name, d->len) == "d");
    ZNode *a = znode_offset(b, -1);
    CHECK(a != nullptr);
    CHECK(a && std::string(a->name, a->len) == "a");

    zset_clear(&z);
}

} // namespace

int main() {
    fprintf(stderr, "test_zset\n");
    RUN(test_insert_and_lookup);
    RUN(test_score_update);
    RUN(test_delete);
    RUN(test_range_order);
    return test_summary();
}
