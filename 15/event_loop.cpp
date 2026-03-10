#include "event_loop.h"

#include <assert.h>
#include <unistd.h>
#include <string.h>

// ── Linux: epoll ──────────────────────────────────────────────────────────────
#ifdef __linux__

#include <sys/epoll.h>

void el_init(EventLoop *el) {
    el->efd = epoll_create1(EPOLL_CLOEXEC);
    assert(el->efd >= 0);
}

void el_destroy(EventLoop *el) {
    close(el->efd);
    el->efd = -1;
}

static uint32_t to_epoll(uint32_t ev) {
    uint32_t e = EPOLLET;               // always edge-triggered
    if (ev & EV_READ)  e |= EPOLLIN;
    if (ev & EV_WRITE) e |= EPOLLOUT;
    if (ev & EV_ERR)   e |= EPOLLERR | EPOLLHUP;
    return e;
}

void el_add(EventLoop *el, int fd, uint32_t events, void *data) {
    struct epoll_event ev = {};
    ev.events   = to_epoll(events);
    ev.data.ptr = data;
    int rv = epoll_ctl(el->efd, EPOLL_CTL_ADD, fd, &ev);
    assert(rv == 0);
}

void el_mod(EventLoop *el, int fd, uint32_t events, void *data) {
    struct epoll_event ev = {};
    ev.events   = to_epoll(events);
    ev.data.ptr = data;
    int rv = epoll_ctl(el->efd, EPOLL_CTL_MOD, fd, &ev);
    assert(rv == 0);
}

void el_del(EventLoop *el, int fd) {
    epoll_ctl(el->efd, EPOLL_CTL_DEL, fd, NULL);
}

int el_wait(EventLoop *el, ELEvent *out, int maxevents, int timeout_ms) {
    struct epoll_event evs[maxevents];
    int n = epoll_wait(el->efd, evs, maxevents, timeout_ms);
    for (int i = 0; i < n; i++) {
        out[i].data   = evs[i].data.ptr;
        out[i].fd     = -1;     // not needed when using data.ptr
        out[i].events = 0;
        uint32_t e    = evs[i].events;
        if (e & (EPOLLIN  | EPOLLHUP)) out[i].events |= EV_READ;
        if (e & EPOLLOUT)              out[i].events |= EV_WRITE;
        if (e & EPOLLERR)              out[i].events |= EV_ERR;
    }
    return n;
}

// ── macOS / BSD: kqueue ───────────────────────────────────────────────────────
#elif defined(__APPLE__) || defined(__FreeBSD__)

#include <sys/event.h>

void el_init(EventLoop *el) {
    el->efd = kqueue();
    assert(el->efd >= 0);
}

void el_destroy(EventLoop *el) {
    close(el->efd);
    el->efd = -1;
}

void el_add(EventLoop *el, int fd, uint32_t events, void *data) {
    struct kevent changes[2];
    int n = 0;
    if (events & EV_READ)
        EV_SET(&changes[n++], fd, EVFILT_READ,  EV_ADD | EV_CLEAR, 0, 0, data);
    if (events & EV_WRITE)
        EV_SET(&changes[n++], fd, EVFILT_WRITE, EV_ADD | EV_CLEAR, 0, 0, data);
    if (n > 0)
        kevent(el->efd, changes, n, NULL, 0, NULL);
}

void el_mod(EventLoop *el, int fd, uint32_t events, void *data) {
    struct kevent changes[4];
    int n = 0;
    // Enable or disable each filter independently.
    EV_SET(&changes[n++], fd, EVFILT_READ,
           (events & EV_READ)  ? (EV_ADD | EV_CLEAR) : EV_DELETE, 0, 0, data);
    EV_SET(&changes[n++], fd, EVFILT_WRITE,
           (events & EV_WRITE) ? (EV_ADD | EV_CLEAR) : EV_DELETE, 0, 0, data);
    // Errors on EV_DELETE for non-registered filters are silently ignored.
    kevent(el->efd, changes, n, NULL, 0, NULL);
}

void el_del(EventLoop *el, int fd) {
    struct kevent changes[2];
    EV_SET(&changes[0], fd, EVFILT_READ,  EV_DELETE, 0, 0, NULL);
    EV_SET(&changes[1], fd, EVFILT_WRITE, EV_DELETE, 0, 0, NULL);
    kevent(el->efd, changes, 2, NULL, 0, NULL);
}

int el_wait(EventLoop *el, ELEvent *out, int maxevents, int timeout_ms) {
    // Use a fixed upper bound to avoid VLA (Clang warns on VLA in C++ mode).
    const int k_max = 1024;
    struct kevent evs[k_max];
    if (maxevents > k_max) maxevents = k_max;
    struct timespec ts, *pts = NULL;
    if (timeout_ms >= 0) {
        ts.tv_sec  = timeout_ms / 1000;
        ts.tv_nsec = (long)(timeout_ms % 1000) * 1000000L;
        pts = &ts;
    }
    int n = kevent(el->efd, NULL, 0, evs, maxevents, pts);
    for (int i = 0; i < n; i++) {
        out[i].fd     = (int)evs[i].ident;
        out[i].data   = evs[i].udata;
        out[i].events = 0;
        if (evs[i].filter == EVFILT_READ)  out[i].events |= EV_READ;
        if (evs[i].filter == EVFILT_WRITE) out[i].events |= EV_WRITE;
        if (evs[i].flags  & EV_ERROR)      out[i].events |= EV_ERR;
        if (evs[i].flags  & EV_EOF)        out[i].events |= EV_READ; // readable EOF
    }
    return n;
}

#else
#  error "Unsupported platform: need Linux (epoll) or macOS/BSD (kqueue)"
#endif
