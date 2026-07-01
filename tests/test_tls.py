#!/usr/bin/env python3
"""In-process TLS smoke test. Requires a TLS build: `make kvs TLS=1`.

Generates a self-signed cert, starts the server with --tls-cert/--tls-key, and
verifies a real TLS 1.3 client can handshake and round-trip commands (including
a pipelined batch, which exercises OpenSSL's decrypted-buffer draining under the
edge-triggered event loop). Run via `make test-tls`.
"""
import os, socket, ssl, struct, subprocess, sys, tempfile, time

HOST, PORT = "127.0.0.1", 1234
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
KVS = os.path.join(REPO, "kvs")

checks = fails = 0
def check(cond, msg):
    global checks, fails
    checks += 1
    if not cond:
        fails += 1
        print(f"    FAIL: {msg}")

def enc(*args):
    body = struct.pack("<I", len(args))
    for a in args:
        a = a.encode()
        body += struct.pack("<I", len(a)) + a
    return struct.pack("<I", len(body)) + body

def recv_exact(s, n):
    b = b""
    while len(b) < n:
        c = s.recv(n - len(b))
        if not c:
            raise ConnectionError("closed")
        b += c
    return b

def rd(s):
    ln = struct.unpack("<I", recv_exact(s, 4))[0]
    body = recv_exact(s, ln)
    t = body[0]
    if t == 0:
        return None
    if t == 2:
        n = struct.unpack_from("<I", body, 1)[0]
        return body[5:5+n].decode()
    return ("tag", t)

def main():
    if not os.path.exists(KVS):
        print("ERROR: build a TLS server first: make kvs TLS=1", file=sys.stderr)
        return 2
    wd = tempfile.mkdtemp(prefix="kvs_tls_")
    cert, key = os.path.join(wd, "cert.pem"), os.path.join(wd, "key.pem")
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                    "-days", "1", "-keyout", key, "-out", cert,
                    "-subj", "/CN=localhost"], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    proc = subprocess.Popen([KVS, "--dir", wd, "--tls-cert", cert, "--tls-key", key],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print("test_tls")
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        # Wait for readiness.
        s = None
        for _ in range(100):
            try:
                raw = socket.create_connection((HOST, PORT), timeout=0.5)
                s = ctx.wrap_socket(raw, server_hostname="localhost")
                break
            except OSError:
                if proc.poll() is not None:
                    print("    FAIL: server exited"); return 1
                time.sleep(0.05)
        check(s is not None, "TLS connection established")
        check(s.version() == "TLSv1.3", f"negotiated TLS 1.3 (got {s.version()})")

        s.sendall(enc("set", "foo", "bar"))
        check(rd(s) is None, "SET over TLS returns nil")
        s.sendall(enc("get", "foo"))
        check(rd(s) == "bar", "GET over TLS returns the value")

        # Pipelined batch in one TLS write — exercises OpenSSL buffer draining.
        n = 50
        s.sendall(b"".join(enc("get", "foo") for _ in range(n)))
        ok = sum(1 for _ in range(n) if rd(s) == "bar")
        check(ok == n, f"pipelined GET over TLS: {ok}/{n}")
        s.close()
    finally:
        proc.kill(); proc.wait(timeout=5)
        for f in (cert, key, os.path.join(wd, "redis.wal"), os.path.join(wd, "redis.dat")):
            try: os.remove(f)
            except OSError: pass
        try: os.rmdir(wd)
        except OSError: pass

    print(f"\n{'PASSED' if fails == 0 else 'FAILED'}: {checks} checks, {fails} failed")
    return 1 if fails else 0

if __name__ == "__main__":
    sys.exit(main())
