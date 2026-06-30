#!/usr/bin/env python3
"""End-to-end integration tests for the `kvs` server.

Drives the real server over a TCP socket using its binary wire protocol, then
kills it with SIGKILL and restarts to verify WAL-backed crash recovery (the
durability contract behind group commit). Runs the server in a private temp
directory so redis.wal / redis.dat are isolated from the repo.

Exit code is nonzero if any assertion fails, so `make test` / CI gate on it.
"""
import os
import socket
import struct
import subprocess
import sys
import tempfile
import time

HOST, PORT = "127.0.0.1", 1234
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
KVS = os.path.join(REPO, "kvs")

checks = 0
fails = 0


def check(cond, msg):
    global checks, fails
    checks += 1
    if not cond:
        fails += 1
        print(f"    FAIL: {msg}")


# ── wire protocol ──────────────────────────────────────────────────────────
def encode(*args):
    body = struct.pack("<I", len(args))
    for a in args:
        a = a.encode() if isinstance(a, str) else a
        body += struct.pack("<I", len(a)) + a
    return struct.pack("<I", len(body)) + body


TAG_NIL, TAG_ERR, TAG_STR, TAG_INT, TAG_DBL, TAG_ARR = range(6)


def _recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("server closed connection")
        buf += chunk
    return buf


def _parse(body, off):
    tag = body[off]
    off += 1
    if tag == TAG_NIL:
        return None, off
    if tag == TAG_INT:
        (v,) = struct.unpack_from("<q", body, off)
        return v, off + 8
    if tag == TAG_DBL:
        (v,) = struct.unpack_from("<d", body, off)
        return v, off + 8
    if tag == TAG_STR:
        (n,) = struct.unpack_from("<I", body, off)
        off += 4
        return body[off:off + n].decode(), off + n
    if tag == TAG_ERR:
        (code,) = struct.unpack_from("<I", body, off)
        off += 4
        (n,) = struct.unpack_from("<I", body, off)
        off += 4
        return ("ERR", code, body[off:off + n].decode()), off + n
    if tag == TAG_ARR:
        (n,) = struct.unpack_from("<I", body, off)
        off += 4
        items = []
        for _ in range(n):
            v, off = _parse(body, off)
            items.append(v)
        return items, off
    raise ValueError(f"unknown tag {tag}")


def call(sock, *args):
    sock.sendall(encode(*args))
    (ln,) = struct.unpack("<I", _recv_exact(sock, 4))
    body = _recv_exact(sock, ln)
    val, _ = _parse(body, 0)
    return val


# ── server lifecycle ───────────────────────────────────────────────────────
def start_server(workdir):
    proc = subprocess.Popen([KVS], cwd=workdir,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.time() + 10
    while time.time() < deadline:
        try:
            s = socket.create_connection((HOST, PORT), timeout=0.5)
            s.close()
            return proc
        except OSError:
            if proc.poll() is not None:
                raise RuntimeError("server exited during startup")
            time.sleep(0.05)
    raise RuntimeError("server did not become ready")


def stop_server(proc, hard=False):
    proc.kill() if hard else proc.terminate()
    proc.wait(timeout=5)


# ── tests ──────────────────────────────────────────────────────────────────
def test_string_commands(s):
    check(call(s, "set", "foo", "bar") is None, "SET returns nil")
    check(call(s, "get", "foo") == "bar", "GET returns the set value")
    check(call(s, "get", "absent") is None, "GET missing returns nil")
    check(call(s, "del", "foo") == 1, "DEL existing returns 1")
    check(call(s, "del", "foo") == 0, "DEL missing returns 0")
    check(call(s, "get", "foo") is None, "GET after DEL returns nil")


def test_overwrite(s):
    call(s, "set", "k", "v1")
    call(s, "set", "k", "v2")
    check(call(s, "get", "k") == "v2", "SET overwrites previous value")


def test_expire(s):
    call(s, "set", "ttlkey", "v")
    check(call(s, "pexpire", "ttlkey", "100000") == 1, "PEXPIRE existing returns 1")
    ttl = call(s, "pttl", "ttlkey")
    check(isinstance(ttl, int) and 0 < ttl <= 100000, f"PTTL in range (got {ttl})")
    check(call(s, "pttl", "noexpire_missing") == -2, "PTTL missing key returns -2")


def test_zset(s):
    check(call(s, "zadd", "z", "1", "a") == 1, "ZADD new member returns 1")
    check(call(s, "zadd", "z", "2", "b") == 1, "ZADD second member returns 1")
    check(call(s, "zadd", "z", "3", "a") == 0, "ZADD existing member returns 0 (update)")
    check(call(s, "zscore", "z", "a") == 3.0, "ZSCORE reflects updated score")
    check(call(s, "zscore", "z", "missing") is None, "ZSCORE missing returns nil")
    # zquery z <score> <name> <offset> <limit>
    res = call(s, "zquery", "z", "0", "", "0", "10")
    check(isinstance(res, list) and "a" in res and "b" in res, "ZQUERY returns members")


def test_type_error(s):
    call(s, "zadd", "zt", "1", "m")
    res = call(s, "get", "zt")
    check(isinstance(res, tuple) and res[0] == "ERR", "GET on a zset returns an error")


def parse_info(text):
    info = {}
    for line in text.replace("\r\n", "\n").splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            info[k] = int(v) if v.isdigit() else v
    return info


def test_info(s):
    info = parse_info(call(s, "info"))
    for key in ("uptime_seconds", "connections_received", "commands_processed",
                "reads", "writes", "keyspace_keys",
                "wal_records", "wal_syncs", "wal_bytes"):
        check(key in info, f"INFO exposes {key}")
    check(info.get("commands_processed", 0) > 0, "INFO commands_processed > 0")
    check(info.get("connections_active", 0) >= 1, "INFO counts the live connection")


def test_info_group_commit(s):
    # A pipelined burst of writes must group-commit: many WAL records, but far
    # fewer fdatasync() calls. INFO makes that observable.
    before = parse_info(call(s, "info"))
    n = 100
    payload = b"".join(encode("set", f"gc{i}", "v") for i in range(n))
    s.sendall(payload)
    for _ in range(n):
        (ln,) = struct.unpack("<I", _recv_exact(s, 4))
        _recv_exact(s, ln)
    after = parse_info(call(s, "info"))

    recs = after["wal_records"] - before["wal_records"]
    syncs = after["wal_syncs"] - before["wal_syncs"]
    check(recs >= n, f"INFO records the {n} writes (got {recs})")
    check(syncs < recs, f"group commit: {syncs} fsyncs < {recs} records")


def test_pipeline(s):
    # Inline-GET fast-path: many pipelined GETs must all return correctly.
    call(s, "set", "p", "PVAL")
    n = 200
    payload = b"".join(encode("get", "p") for _ in range(n))
    s.sendall(payload)
    ok = 0
    for _ in range(n):
        (ln,) = struct.unpack("<I", _recv_exact(s, 4))
        body = _recv_exact(s, ln)
        val, _ = _parse(body, 0)
        if val == "PVAL":
            ok += 1
    check(ok == n, f"pipelined GET: {ok}/{n} correct")


def test_crash_recovery(workdir):
    proc = start_server(workdir)
    s = socket.create_connection((HOST, PORT))
    for i in range(100):
        # Each SET is acked only after group-commit fsync — so it must survive.
        check_ack = call(s, "set", f"persist{i}", f"value{i}")
    s.close()
    stop_server(proc, hard=True)            # SIGKILL — simulate power loss

    proc = start_server(workdir)            # restart, replay WAL
    s = socket.create_connection((HOST, PORT))
    recovered = sum(1 for i in range(100)
                    if call(s, "get", f"persist{i}") == f"value{i}")
    check(recovered == 100, f"crash recovery: {recovered}/100 keys restored")
    s.close()
    stop_server(proc, hard=True)


def main():
    if not os.path.exists(KVS):
        print(f"ERROR: {KVS} not built — run `make kvs` first", file=sys.stderr)
        return 2

    workdir = tempfile.mkdtemp(prefix="kvs_itest_")
    print("test_integration")
    try:
        proc = start_server(workdir)
        s = socket.create_connection((HOST, PORT))
        try:
            for t in (test_string_commands, test_overwrite, test_expire,
                      test_zset, test_type_error, test_info,
                      test_info_group_commit, test_pipeline):
                print(f"  • {t.__name__}")
                t(s)
        finally:
            s.close()
            stop_server(proc, hard=True)

        print("  • test_crash_recovery")
        test_crash_recovery(workdir)
    finally:
        for f in ("redis.wal", "redis.dat"):
            try:
                os.remove(os.path.join(workdir, f))
            except OSError:
                pass
        try:
            os.rmdir(workdir)
        except OSError:
            pass

    print(f"\n{'PASSED' if fails == 0 else 'FAILED'}: {checks} checks, {fails} failed")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
