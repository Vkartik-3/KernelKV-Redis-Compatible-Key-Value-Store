#pragma once
// Minimal assertion-based test harness (no external dependency).
//
//   RUN(some_test_fn);           // runs a test, prints its name
//   CHECK(cond);                 // records a failure if cond is false
//   CHECK_EQ(a, b);              // CHECK(a == b) with values printed on fail
//   return test_summary();       // prints totals, returns nonzero on any fail
//
// Each test binary's main() runs its RUN() blocks then returns test_summary(),
// so `make test` fails (nonzero exit) the moment any CHECK fails.

#include <cstdio>
#include <cstdint>

static int g_checks = 0;
static int g_fails  = 0;

#define CHECK(cond)                                                       \
    do {                                                                  \
        g_checks++;                                                       \
        if (!(cond)) {                                                    \
            g_fails++;                                                    \
            fprintf(stderr, "    FAIL %s:%d  CHECK(%s)\n",                \
                    __FILE__, __LINE__, #cond);                           \
        }                                                                 \
    } while (0)

#define CHECK_EQ(a, b)                                                    \
    do {                                                                  \
        g_checks++;                                                       \
        long long _va = (long long)(a);                                   \
        long long _vb = (long long)(b);                                   \
        if (_va != _vb) {                                                 \
            g_fails++;                                                    \
            fprintf(stderr, "    FAIL %s:%d  CHECK_EQ(%s, %s)  %lld != %lld\n", \
                    __FILE__, __LINE__, #a, #b, _va, _vb);                \
        }                                                                 \
    } while (0)

#define RUN(test)                                                         \
    do {                                                                  \
        fprintf(stderr, "  • %s\n", #test);                               \
        test();                                                           \
    } while (0)

static inline int test_summary() {
    fprintf(stderr, "\n%s: %d checks, %d failed\n",
            g_fails ? "FAILED" : "PASSED", g_checks, g_fails);
    return g_fails ? 1 : 0;
}
