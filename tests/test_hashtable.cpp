// Unit tests for the intrusive HMap (progressive-rehash hash table).
#include <string>
#include <set>
#include <cstring>

#include "../core/hashtable.h"
#include "../core/common.h"
#include "test_util.h"

namespace {

// Payload that embeds an HNode, mirroring the server's Entry.
struct KV {
    HNode       node;
    std::string key;
};

bool kv_eq(HNode *a, HNode *b) {
    KV *ka = container_of(a, KV, node);
    KV *kb = container_of(b, KV, node);
    return ka->key == kb->key;
}

KV *make_kv(const std::string &k) {
    KV *kv = new KV();
    kv->key = k;
    kv->node.hcode = str_hash((const uint8_t *)k.data(), k.size());
    return kv;
}

// Look up by key string (builds a temporary probe node).
KV *lookup(HMap *m, const std::string &k) {
    KV probe;
    probe.key = k;
    probe.node.hcode = str_hash((const uint8_t *)k.data(), k.size());
    HNode *n = hm_lookup(m, &probe.node, &kv_eq);
    return n ? container_of(n, KV, node) : nullptr;
}

void test_insert_lookup() {
    HMap m{};
    hm_insert(&m, &make_kv("alpha")->node);
    hm_insert(&m, &make_kv("beta")->node);

    CHECK_EQ(hm_size(&m), 2);
    KV *a = lookup(&m, "alpha");
    CHECK(a != nullptr);
    CHECK(a && a->key == "alpha");
    CHECK(lookup(&m, "beta") != nullptr);
    CHECK(lookup(&m, "missing") == nullptr);

    hm_clear(&m);
}

void test_delete() {
    HMap m{};
    hm_insert(&m, &make_kv("x")->node);
    hm_insert(&m, &make_kv("y")->node);

    KV probe;
    probe.key = "x";
    probe.node.hcode = str_hash((const uint8_t *)probe.key.data(), probe.key.size());
    HNode *removed = hm_delete(&m, &probe.node, &kv_eq);
    CHECK(removed != nullptr);
    if (removed) delete container_of(removed, KV, node);

    CHECK_EQ(hm_size(&m), 1);
    CHECK(lookup(&m, "x") == nullptr);
    CHECK(lookup(&m, "y") != nullptr);

    // Deleting a missing key returns null and changes nothing.
    probe.key = "ghost";
    probe.node.hcode = str_hash((const uint8_t *)probe.key.data(), probe.key.size());
    CHECK(hm_delete(&m, &probe.node, &kv_eq) == nullptr);
    CHECK_EQ(hm_size(&m), 1);

    hm_clear(&m);
}

// Insert enough keys to force progressive rehashing, then verify every key is
// still findable and the count is exact.
void test_rehash_many() {
    HMap m{};
    const int N = 5000;
    std::set<std::string> keys;
    for (int i = 0; i < N; i++) {
        std::string k = "key:" + std::to_string(i);
        keys.insert(k);
        hm_insert(&m, &make_kv(k)->node);
    }
    CHECK_EQ(hm_size(&m), N);
    for (const auto &k : keys) {
        CHECK(lookup(&m, k) != nullptr);
    }
    CHECK(lookup(&m, "key:999999") == nullptr);
    hm_clear(&m);
}

} // namespace

int main() {
    fprintf(stderr, "test_hashtable\n");
    RUN(test_insert_lookup);
    RUN(test_delete);
    RUN(test_rehash_many);
    return test_summary();
}
