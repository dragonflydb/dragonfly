#!/usr/bin/env python3
"""
Dragonfly master-replica fuzzing orchestrator.

Reads a multiplexed binary input containing commands for master, commands for
replica, and network events (connection drops). Dispatches them to two running
Dragonfly instances and detects process crashes.

Input format (binary):
  byte 0      : N = number of actions (0-30)
  per action  :
    byte 0    : type  0=MASTER_CMD  1=REPLICA_CMD  2=NET_DROP  3=WAIT
    bytes 1-2 : payload length, uint16 little-endian
    bytes 3.. : payload (raw RESP for CMD types, empty for NET_DROP/WAIT)

Exit:
  SIGABRT  – master or replica process crashed (stopped responding)
  0        – clean iteration

Environment variables:
  MASTER_PORT      (default 7379)
  REPLICA_PORT     (default 7380)
  RESP_TIMEOUT     (default 3.0 s)
  WAIT_TIMEOUT_MS  (default 3000 ms)

Usage:
  # Single-shot / manual test
  python3 fuzz/replication_orchestrator.py seed.bin

  # With python-afl (persistent mode)
  AFL_PYTHON_MODULE=replication_mutator \\
    afl-fuzz -n -i corpus/replication -o artifacts/replication \\
    -- python3 fuzz/replication_orchestrator.py

  # See run_replication_fuzzer.sh for the full setup
"""

import logging
import os
import signal
import socket
import struct
import sys
import time

log = logging.getLogger("repl_fuzz")

# ── Action types ──────────────────────────────────────────────────────────────
ACTION_MASTER = 0  # send RESP payload to master
ACTION_REPLICA = 1  # send RESP payload to replica
ACTION_NET_DROP = 2  # disconnect replica from master and reconnect
ACTION_WAIT = 3  # wait briefly for replication to propagate

MAX_ACTIONS = 30
MAX_PAYLOAD = 65_536

# ── Connection parameters (overridable via env) ───────────────────────────────
MASTER_HOST = "127.0.0.1"
MASTER_PORT = int(os.environ.get("MASTER_PORT", "7379"))
REPLICA_HOST = "127.0.0.1"
REPLICA_PORT = int(os.environ.get("REPLICA_PORT", "7380"))
RESP_TIMEOUT = float(os.environ.get("RESP_TIMEOUT", "3.0"))
WAIT_TIMEOUT_MS = int(os.environ.get("WAIT_TIMEOUT_MS", "3000"))


# ── Buffered socket reader ────────────────────────────────────────────────────


class BufSock:
    """
    Thin wrapper around a socket with a read-ahead buffer.

    Using recv(1) per byte is extremely slow (one syscall per byte).
    This class reads up to _CHUNK bytes at a time and serves lines / bulk
    reads from the local buffer, cutting syscall overhead by ~100x.
    """

    _CHUNK = 4096

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._buf = bytearray()

    def readline(self) -> "bytes | None":
        """Read until \\r\\n. Returns the line WITHOUT the trailing \\r\\n,
        or None on connection loss / timeout."""
        while True:
            idx = self._buf.find(b"\r\n")
            if idx >= 0:
                line = bytes(self._buf[:idx])
                del self._buf[: idx + 2]
                return line
            try:
                chunk = self._sock.recv(self._CHUNK)
            except OSError:
                return None
            if not chunk:
                return None
            self._buf.extend(chunk)

    def readn(self, n: int) -> "bytes | None":
        """Read exactly n bytes. Returns None on connection loss / timeout."""
        while len(self._buf) < n:
            try:
                chunk = self._sock.recv(self._CHUNK)
            except OSError:
                return None
            if not chunk:
                return None
            self._buf.extend(chunk)
        data = bytes(self._buf[:n])
        del self._buf[:n]
        return data

    def sendall(self, data: bytes) -> bool:
        try:
            self._sock.sendall(data)
            return True
        except OSError:
            return False

    def close(self) -> None:
        try:
            self._sock.close()
        except OSError:
            pass

    @property
    def raw(self) -> socket.socket:
        return self._sock


# ── RESP helpers ──────────────────────────────────────────────────────────────


def encode_resp(*args) -> bytes:
    parts = [f"*{len(args)}\r\n".encode()]
    for a in args:
        if isinstance(a, str):
            a = a.encode()
        parts.append(f"${len(a)}\r\n".encode() + a + b"\r\n")
    return b"".join(parts)


def _connect(host: str, port: int) -> "BufSock | None":
    try:
        s = socket.socket()
        s.settimeout(2.0)
        s.connect((host, port))
        s.settimeout(RESP_TIMEOUT)
        return BufSock(s)
    except OSError:
        return None


def _drain(buf: BufSock) -> bool:
    """Read one complete RESP response from buf. Returns False if connection lost."""
    line = buf.readline()
    if line is None:
        return False
    if not line:
        return True

    if line[0:1] in (b"+", b"-", b":"):
        return True

    if line[0:1] == b"$":
        n = int(line[1:])
        if n < 0:
            return True
        data = buf.readn(n + 2)  # +2 for trailing \r\n
        return data is not None

    if line[0:1] == b"*":
        n = int(line[1:])
        if n < 0:
            return True
        for _ in range(n):
            if not _drain(buf):
                return False
        return True

    return True  # unknown type – ignore


def _send(buf: BufSock, data: bytes) -> bool:
    """Send RESP bytes and drain the response. Returns False on connection loss."""
    if not buf.sendall(data):
        return False
    return _drain(buf)


# ── Input parser ──────────────────────────────────────────────────────────────


def parse_actions(data: bytes) -> list:
    if not data:
        return []
    n = min(data[0], MAX_ACTIONS)
    pos = 1
    actions = []
    for _ in range(n):
        if pos + 3 > len(data):
            break
        atype = data[pos]
        plen = struct.unpack_from("<H", data, pos + 1)[0]
        pos += 3
        if atype > 3:
            pos += plen
            continue
        payload = data[pos : pos + min(plen, MAX_PAYLOAD)]
        pos += plen
        actions.append((atype, payload))
    return actions


# ── Health check ──────────────────────────────────────────────────────────────


def ping(host: str, port: int) -> bool:
    """Return True if the instance is alive (responds to any valid RESP).
    -LOADING is accepted — the process is alive, just busy with a full sync."""
    buf = _connect(host, port)
    if buf is None:
        return False
    try:
        buf.sendall(encode_resp("PING"))
        line = buf.readline()
        return line is not None  # any response means the process is alive
    finally:
        buf.close()


# ── State management ──────────────────────────────────────────────────────────


def _cmd_buf(buf: BufSock, *args) -> None:
    """Fire a single command on an existing BufSock, ignore errors."""
    _send(buf, encode_resp(*args))


def _cmd(host: str, port: int, *args) -> None:
    """Fire-and-forget a single command, ignore errors."""
    buf = _connect(host, port)
    if buf is None:
        return
    try:
        _send(buf, encode_resp(*args))
    finally:
        buf.close()


def net_drop_and_reconnect(rbuf: BufSock) -> "BufSock | None":
    """
    Simulate a network drop: send REPLICAOF NO ONE then re-enable replication.
    Closes rbuf and returns a new BufSock to the replica after reconnect.
    """
    _cmd_buf(rbuf, "REPLICAOF", "NO", "ONE")
    rbuf.close()
    time.sleep(0.01)
    new_rbuf = _connect(REPLICA_HOST, REPLICA_PORT)
    if new_rbuf is None:
        return None
    _cmd_buf(new_rbuf, "REPLICAOF", MASTER_HOST, str(MASTER_PORT))
    return new_rbuf


def reset_state() -> None:
    """Reset both instances between fuzzing iterations."""
    rbuf = _connect(REPLICA_HOST, REPLICA_PORT)
    mbuf = _connect(MASTER_HOST, MASTER_PORT)
    if rbuf:
        _cmd_buf(rbuf, "REPLICAOF", "NO", "ONE")
    if mbuf:
        _cmd_buf(mbuf, "FLUSHALL")
    if rbuf:
        _cmd_buf(rbuf, "FLUSHALL")
        _cmd_buf(rbuf, "REPLICAOF", MASTER_HOST, str(MASTER_PORT))
        rbuf.close()
    if mbuf:
        mbuf.close()


def setup_replication() -> None:
    """Initial replication setup: connect replica to master."""
    _cmd(REPLICA_HOST, REPLICA_PORT, "REPLICAOF", MASTER_HOST, str(MASTER_PORT))


# ── Core iteration ────────────────────────────────────────────────────────────


def run_iteration(data: bytes) -> bool:
    """
    Execute one fuzzing iteration.
    Returns True if both instances are alive, False if either crashed.
    """
    actions = parse_actions(data)
    if not actions:
        return True

    mbuf = _connect(MASTER_HOST, MASTER_PORT)
    rbuf = _connect(REPLICA_HOST, REPLICA_PORT)

    if mbuf is None or rbuf is None:
        for b in [mbuf, rbuf]:
            if b:
                b.close()
        alive_m = ping(MASTER_HOST, MASTER_PORT)
        alive_r = ping(REPLICA_HOST, REPLICA_PORT)
        if not alive_m:
            log.error("Master not responding at start of iteration")
        if not alive_r:
            log.error("Replica not responding at start of iteration")
        return alive_m and alive_r

    try:
        for atype, payload in actions:
            if atype == ACTION_MASTER:
                if payload:
                    ok = _send(mbuf, payload)
                    if not ok:
                        mbuf.close()
                        mbuf = _connect(MASTER_HOST, MASTER_PORT)
                        if mbuf is None:
                            log.error("Master crashed mid-iteration")
                            return False

            elif atype == ACTION_REPLICA:
                if payload:
                    ok = _send(rbuf, payload)
                    if not ok:
                        rbuf.close()
                        rbuf = _connect(REPLICA_HOST, REPLICA_PORT)
                        if rbuf is None:
                            log.error("Replica crashed mid-iteration")
                            return False

            elif atype == ACTION_NET_DROP:
                new_rbuf = net_drop_and_reconnect(rbuf)
                rbuf = None
                if new_rbuf is None:
                    log.error("Replica crashed after NET_DROP")
                    return False
                rbuf = new_rbuf
                mbuf.close()
                mbuf = _connect(MASTER_HOST, MASTER_PORT)
                if mbuf is None:
                    log.error("Master crashed after NET_DROP")
                    return False

            elif atype == ACTION_WAIT:
                # Small sleep to let replication propagate
                time.sleep(0.005)

    finally:
        for b in [mbuf, rbuf]:
            if b:
                try:
                    b.close()
                except OSError:
                    pass

    # Final liveness check
    if not ping(MASTER_HOST, MASTER_PORT):
        log.error("Master not responding after iteration")
        return False
    if not ping(REPLICA_HOST, REPLICA_PORT):
        log.error("Replica not responding after iteration")
        return False

    return True


# ── AFL++ integration ─────────────────────────────────────────────────────────


def report_crash() -> None:
    os.kill(os.getpid(), signal.SIGABRT)


def main() -> None:
    logging.basicConfig(
        level=logging.ERROR,
        format="%(levelname)s %(name)s: %(message)s",
    )

    # Try python-afl persistent mode
    try:
        import afl  # type: ignore

        afl.init()
        while afl.loop(500):
            data = sys.stdin.buffer.read()
            ok = run_iteration(data)
            reset_state()
            if not ok:
                report_crash()
        return
    except ImportError:
        pass

    # Single-shot: read from file arg or stdin
    if len(sys.argv) > 1:
        with open(sys.argv[1], "rb") as f:
            data = f.read()
    else:
        data = sys.stdin.buffer.read()

    ok = run_iteration(data)
    if not ok:
        report_crash()


if __name__ == "__main__":
    main()
