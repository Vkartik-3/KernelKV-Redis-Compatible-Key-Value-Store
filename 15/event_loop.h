#pragma once

#include <stdint.h>

// Unified event loop abstraction.
// Uses epoll on Linux, kqueue on macOS.
// All filters are edge-triggered (EPOLLET / EV_CLEAR).

#define EV_READ  0x01u
#define EV_WRITE 0x02u
#define EV_ERR   0x04u

struct ELEvent {
    int      fd;      // file descriptor that fired
    uint32_t events;  // bitmask of EV_READ / EV_WRITE / EV_ERR
    void    *data;    // user pointer registered with el_add / el_mod
};

struct EventLoop {
    int efd;  // epoll fd (Linux) or kqueue fd (macOS)
};

void el_init(EventLoop *el);
void el_destroy(EventLoop *el);

// Register fd for the first time.
void el_add(EventLoop *el, int fd, uint32_t events, void *data);
// Update the event mask for an already-registered fd.
void el_mod(EventLoop *el, int fd, uint32_t events, void *data);
// Remove fd from the event loop.
void el_del(EventLoop *el, int fd);

// Wait for events.  timeout_ms == -1 means wait forever.
// Returns the number of events written to `out`.
int el_wait(EventLoop *el, ELEvent *out, int maxevents, int timeout_ms);
