// Fuzz target for the wire-protocol request parser (engine/protocol.h).
//
// The parser turns raw, untrusted socket bytes into a command vector, so it is
// the server's primary attack surface. This harness exercises it under
// AddressSanitizer + UndefinedBehaviorSanitizer; any out-of-bounds read, signed
// overflow, or other UB aborts the run.
//
// Two build modes (same file):
//
//   1. Standalone (default, portable — used by `make fuzz` and CI):
//        clang++ -fsanitize=address,undefined fuzz_parser.cpp -o fuzz_parser
//      A seeded `main()` runs property-based testing: thousands of random byte
//      buffers plus crafted adversarial inputs (huge nstr, huge len, truncated
//      frames, trailing garbage), and asserts a set of known-good / known-bad
//      framings parse as expected. Deterministic, runs anywhere.
//
//   2. libFuzzer (coverage-guided, where the toolchain ships it — e.g. Linux):
//        clang++ -DUSE_LIBFUZZER -fsanitize=fuzzer,address,undefined fuzz_parser.cpp
//      Provides LLVMFuzzerTestOneInput for deep, coverage-guided fuzzing.

#include <stdint.h>
#include <stddef.h>
#include <string>
#include <vector>

#include "../engine/protocol.h"

// The single property every input must satisfy: parsing never crashes, reads
// out of bounds, or invokes UB — and any std::bad_alloc is caught, not leaked.
static void run_one(const uint8_t *data, size_t size) {
    std::vector<std::string> out;
    try {
        parse_req(data, size, out);
    } catch (const std::bad_alloc &) {
        // A length field can legitimately request more memory than available;
        // refusing to allocate is fine, crashing is not.
    }
}

#ifdef USE_LIBFUZZER

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    run_one(data, size);
    return 0;
}

#else // ── standalone portable fuzzer ──────────────────────────────────────

#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace {

// Build a request body: [nstr][len0][bytes0]...
std::vector<uint8_t> frame(const std::vector<std::string> &args) {
    std::vector<uint8_t> b;
    auto put32 = [&](uint32_t v) {
        b.insert(b.end(), (uint8_t *)&v, (uint8_t *)&v + 4);
    };
    put32((uint32_t)args.size());
    for (const auto &a : args) {
        put32((uint32_t)a.size());
        b.insert(b.end(), a.begin(), a.end());
    }
    return b;
}

int g_fail = 0;
void expect(bool cond, const char *what) {
    if (!cond) { g_fail++; fprintf(stderr, "    FAIL: %s\n", what); }
}

// Correctness regression checks: well-formed inputs accepted, malformed rejected.
void correctness_checks() {
    std::vector<std::string> out;

    out.clear();
    auto ok = frame({"set", "k", "v"});
    expect(parse_req(ok.data(), ok.size(), out) == 0 && out.size() == 3,
           "well-formed 3-arg request parses");

    out.clear();
    auto empty = frame({});
    expect(parse_req(empty.data(), empty.size(), out) == 0 && out.empty(),
           "zero-arg request parses");

    out.clear();
    expect(parse_req(nullptr, 0, out) == -1, "empty buffer rejected");

    // nstr says 2 args but the buffer ends early.
    out.clear();
    uint8_t truncated[8] = {2, 0, 0, 0, 5, 0, 0, 0}; // nstr=2, len=5, no bytes
    expect(parse_req(truncated, sizeof(truncated), out) == -1,
           "truncated arg rejected");

    // Trailing garbage after a complete request.
    out.clear();
    auto trailing = frame({"x"});
    trailing.push_back(0xAB);
    expect(parse_req(trailing.data(), trailing.size(), out) == -1,
           "trailing garbage rejected");

    // Oversized length field (claims 4 GB) must be rejected, not allocated.
    out.clear();
    uint8_t huge_len[8] = {1, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF};
    expect(parse_req(huge_len, sizeof(huge_len), out) == -1,
           "oversized length rejected without OOB");

    // Oversized nstr must be rejected by the k_max_args cap.
    out.clear();
    uint8_t huge_nstr[4] = {0xFF, 0xFF, 0xFF, 0xFF};
    expect(parse_req(huge_nstr, sizeof(huge_nstr), out) == -1,
           "oversized nstr rejected by cap");
}

// Adversarial framings: valid headers paired with too-few / too-many bytes.
void adversarial(unsigned &seed) {
    for (int i = 0; i < 20000; i++) {
        std::vector<uint8_t> b;
        uint32_t nstr = rand_r(&seed) % 6;
        b.insert(b.end(), (uint8_t *)&nstr, (uint8_t *)&nstr + 4);
        for (uint32_t a = 0; a < nstr; a++) {
            uint32_t len = rand_r(&seed) % 12;
            // Half the time, lie about the length to force a short read.
            uint32_t claimed = (rand_r(&seed) & 1) ? len : len + (rand_r(&seed) % 64);
            b.insert(b.end(), (uint8_t *)&claimed, (uint8_t *)&claimed + 4);
            for (uint32_t k = 0; k < len; k++) b.push_back((uint8_t)rand_r(&seed));
        }
        run_one(b.data(), b.size());
    }
}

// Pure random noise of varying lengths.
void random_noise(unsigned &seed) {
    for (int i = 0; i < 30000; i++) {
        size_t n = rand_r(&seed) % 256;
        std::vector<uint8_t> b(n);
        for (size_t k = 0; k < n; k++) b[k] = (uint8_t)rand_r(&seed);
        run_one(b.data(), b.size());
    }
    // A few large buffers to stress the length paths.
    for (int i = 0; i < 200; i++) {
        size_t n = rand_r(&seed) % 70000;
        std::vector<uint8_t> b(n);
        for (size_t k = 0; k < n; k++) b[k] = (uint8_t)rand_r(&seed);
        run_one(b.data(), b.size());
    }
}

} // namespace

int main() {
    fprintf(stderr, "fuzz_parser (standalone, ASAN/UBSAN)\n");
    unsigned seed = 0xC0FFEE;       // fixed seed → deterministic, reproducible
    correctness_checks();
    adversarial(seed);
    random_noise(seed);
    fprintf(stderr, "%s: correctness + ~50k fuzz iterations, %d failed\n",
            g_fail ? "FAILED" : "PASSED", g_fail);
    return g_fail ? 1 : 0;
}

#endif
