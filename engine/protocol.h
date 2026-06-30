#pragma once
// Wire-protocol request parser, factored out of server.cpp so it can be unit
// tested and fuzzed in isolation (see tests/fuzz_parser.cpp).
//
// Request body layout (little-endian):
//   [nstr : u32][len0 : u32][bytes0]...[lenN : u32][bytesN]
//
// parse_req is a pure function over an untrusted byte buffer: it must never
// read out of bounds, allocate unboundedly, or invoke UB for ANY input.

#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <string>
#include <vector>

// Cap on argument count, guarding against a huge nstr forcing a giant vector.
static const size_t k_max_args = 200 * 1000;

// Read a u32 if at least 4 bytes remain. Bounds are checked against the
// remaining byte count (end - cur) rather than `cur + 4`, so there is no
// pointer-arithmetic overflow even for adversarial inputs.
static inline bool read_u32(const uint8_t *&cur, const uint8_t *end, uint32_t &out) {
    if ((size_t)(end - cur) < 4) return false;
    memcpy(&out, cur, 4);
    cur += 4;
    return true;
}

// Read n bytes into out if at least n remain. Comparing n to the remaining
// byte count (instead of `cur + n > end`) avoids pointer overflow when a
// malicious length field is near SIZE_MAX, and bounds the allocation to data
// that actually exists in the buffer.
static inline bool read_str(const uint8_t *&cur, const uint8_t *end,
                            size_t n, std::string &out) {
    if (n > (size_t)(end - cur)) return false;
    out.assign(cur, cur + n);
    cur += n;
    return true;
}

// Parse one request. Returns 0 on a fully consumed, well-formed buffer; -1 on
// any short, malformed, or trailing-garbage input. Never throws on bad framing
// (only std::bad_alloc is possible, and only for inputs that genuinely contain
// that many bytes).
static inline int32_t parse_req(const uint8_t *data, size_t size,
                                std::vector<std::string> &out) {
    const uint8_t *cur = data;
    const uint8_t *end = data + size;

    uint32_t nstr = 0;
    if (!read_u32(cur, end, nstr)) return -1;
    if (nstr > k_max_args) return -1;

    while (out.size() < nstr) {
        uint32_t len = 0;
        if (!read_u32(cur, end, len)) return -1;
        out.push_back(std::string());
        if (!read_str(cur, end, len, out.back())) return -1;
    }
    return (cur == end) ? 0 : -1;
}
