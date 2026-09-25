#!/usr/bin/env python3
"""Replays a crash from AFL++ persistent mode RECORD files and reports which input killed the server.

In persistent mode a crash depends on the server state built by all previous iterations;
AFL_PERSISTENT_RECORD saves those inputs as RECORD files. This script replays them in order
against a running Dragonfly instance, then sends the crashing input.

Modes:
  drain    (default) append a barrier command to every input on the same connection (PING with
           a random token for RESP, "version" for memcache) and wait for its reply. Commands on
           one connection execute in order, so the barrier's reply proves that every recorded
           command before it ran, whatever it replied (nothing, several messages, QUEUED ...).
           An input that ends in the middle of a command, or whose parse the server could
           decide differently from this script, is sent byte for byte without a barrier and is
           reported as unverified, as is an input whose barrier never answers (blocking
           command, open MULTI, connection closed by the server). This mode reproduces
           deterministic crashes and names the input that ran the fatal command.
  harness  mimic the in-process fuzz harness (SendFuzzInputToServer) byte for byte: connect,
           one send(), one recv() with a 200 ms timeout, close, next input at once; a refused
           connection is skipped like the harness does (and listed at the end, exit 4), and no
           grace pauses or probes are inserted between inputs. The server drops the unexecuted
           tail of a pipeline when the peer closes, so this mode is timing-dependent; it is what
           the fuzzer did.

Liveness is checked through /proc (--pid) or bare TCP connects; no extra command is ever sent
to the server outside the drain barrier. With --pid nothing is sent at all unless /proc shows
that the process itself accepted a probe connection to the exact address used for the replay
(the host is resolved once to one IPv4 address).

The fuzzer attributes a crash to the input in flight when the process died, which is often the
NEXT input: after an assertion the process keeps running for tens of milliseconds while the
stack trace is printed. Replaying in drain mode names the input that ran the fatal command.

Usage:
    # Start a plain (non-AFL) dragonfly on an empty --dir with the flags from repro.env, then:
    python3 fuzz/replay_crash.py fuzz/artifacts/resp/default/crashes 000000 [host] [port]
        [--mode drain|harness] [--pid PID] [--protocol resp|memcache] [--tail N]
        [--no-crash-input] [--timeout SEC] [--wait SEC] [--verbose]

Exit codes: 0 = server alive (drain: every input verified; harness: nothing is verified, the
server simply survived and no input was skipped); 3 = server died (the input is printed, with a note when earlier inputs
were unverified, since the state may then differ from the fuzz run); 4 = server alive but some
inputs could not be verified (listed); 1 = nothing to replay, the server could not be reached,
or the endpoint could not be confirmed to belong to --pid; 2 = invalid arguments.
"""

import argparse
import glob
import math
import os
import socket
import sys
import time

IDLE_GAP_SEC = 0.2  # silence that ends the read of an input that cannot carry a barrier
DEATH_GRACE_SEC = 0.3  # how long a dying process may keep its listener up after an abort
FINAL_GRACE_SEC = 1.0  # same, after the last input: nothing else will observe the death
CONNECT_RETRIES = 5  # drain mode: a refused connection while the process is alive is retried
MC_MAX_VALUE_LEN = 1 << 20  # the fuzz run's --max_bulk_len; larger store headers are rejected
MC_MAX_KEY_LEN = 250
HARNESS_BUF = 64 * 1024  # RunAflFuzzingIteration reads one buffer of this size per iteration


def record_files(crash_dir, crash_id):
    files = glob.glob(os.path.join(crash_dir, f"RECORD:{crash_id},cnt:*"))
    return sorted(files, key=lambda p: int(os.path.basename(p).rsplit("cnt:", 1)[1]))


def crash_input_file(crash_dir, crash_id):
    files = [
        f
        for f in glob.glob(os.path.join(crash_dir, f"id:{crash_id},*"))
        if not os.path.basename(f).startswith("RECORD:")
    ]
    return files[0] if files else None


# ─── Walking an input the way the server parses it ───────────────────────────
#
# Each walker returns (names, prefix_len, certain): the command names the server will dispatch
# (a rejected store header is dispatched too, and its would-be data line is parsed as the next
# command), the length of the prefix made of complete commands, and whether the walk is exact.
# Only a complete and certain input gets a barrier; anything else is sent byte for byte.


def resp_walk(data):
    names, i, size = [], 0, len(data)
    while i < size:
        if data[i : i + 1] != b"*":  # inline command, complete once its line ends
            end = data.find(b"\n", i)
            if end < 0:
                return names, i, True
            words = data[i:end].split()
            names.append(words[0].upper() if words else b"")
            i = end + 1
            continue
        end = data.find(b"\r\n", i)
        if end < 0:
            return names, i, True
        try:
            argc = int(data[i + 1 : end])
        except ValueError:
            return names, i, True
        if argc < 0:
            return names, i, True
        j = end + 2
        name = b""
        for n in range(argc):
            end = data.find(b"\r\n", j)
            if data[j : j + 1] != b"$" or end < 0:
                return names, i, True
            try:
                length = int(data[j + 1 : end])
            except ValueError:
                return names, i, True
            if length < 0 or end + 2 + length + 2 > size:
                return names, i, True
            if n == 0:
                name = data[end + 2 : end + 2 + length].upper()
            j = end + 2 + length + 2
        names.append(name)
        i = j
    return names, size, True


def _uint(token, bits):
    """absl::SimpleAtoi for an unsigned integer: surrounding ASCII whitespace and a leading
    '+' are accepted, nothing else but digits."""
    token = token.strip(b" \t\r\n\v\f")
    if token[:1] == b"+":
        token = token[1:]
    if not token or not token.isdigit():
        return None
    token = token.lstrip(b"0") or b"0"
    if len(token) > 20:  # beyond uint64 anyway; int() refuses huge digit strings
        return None
    value = int(token)
    return value if value < (1 << bits) else None


def _store_data_len(name, args):
    """Data block length of a classic store command, None when the server rejects the header
    (MemcacheParser::ParseInternal / ParseStore)."""
    if len(args) < 4 or len(args[0]) > MC_MAX_KEY_LEN:
        return None
    opt = 4
    if name == b"cas":
        if len(args) <= opt:
            return None
        opt = 5
    flags, expire, length = _uint(args[1], 32), _uint(args[2], 32), _uint(args[3], 32)
    if flags is None or expire is None or length is None or length > MC_MAX_VALUE_LEN:
        return None
    if name == b"cas" and _uint(args[4], 64) is None:
        return None
    if len(args) == opt + 1 and args[opt] != b"noreply":
        return None
    if len(args) > opt + 1:
        return None
    return length


def _meta_set_data_len(args):
    """Same for "ms <key> <datalen> <flags>*" (MemcacheParser::ParseMeta). Returns (length,
    certain); certain is False for flags whose validation this script does not mirror."""
    if len(args) < 2 or len(args[0]) > MC_MAX_KEY_LEN:
        return None, True
    length = _uint(args[1], 32)
    if length is None or length > MC_MAX_VALUE_LEN:
        return None, True
    for flag in args[2:]:
        kind = flag[:1]
        if kind in (b"T", b"F"):
            if _uint(flag[1:], 32) is None:
                return None, True
        elif kind == b"D":
            if _uint(flag[1:], 64) is None:
                return None, True
        elif kind in (b"q", b"f", b"v", b"t", b"l", b"h", b"c"):
            pass
        elif kind in (b"b", b"M"):
            return length, False  # base64 keys and mode flags are not mirrored here
        else:
            return None, True
    return length, True


def memcache_walk(data):
    names, i, size = [], 0, len(data)
    certain = True
    while i < size:
        nl = data.find(b"\n", i)
        if nl < 0:
            return names, i, certain
        line, nxt = data[i:nl], nl + 1
        if not line.endswith(b"\r"):  # PARSE_ERROR, nothing consumed beyond the line
            names.append(b"")
            i = nxt
            continue
        tokens = [t for t in line[:-1].split(b" ") if t.strip()]
        name = tokens[0] if tokens else b""
        args = tokens[1:]
        length = None
        if name in (b"set", b"add", b"replace", b"append", b"prepend", b"cas"):
            length = _store_data_len(name, args)
        elif name == b"ms":
            length, exact = _meta_set_data_len(args)
            certain = certain and exact
        if length is not None:
            # ConsumeValue: the data bytes, then '\r' and '\n' one at a time. On a mismatch the
            # parser is reset and parsing resumes at the offending byte, so a bad terminator
            # turns the rest of the line into the next command.
            end = nxt + length
            if end >= size or (data[end : end + 1] == b"\r" and end + 1 >= size):
                return names, i, certain  # value or terminator still incomplete
            if data[end : end + 2] == b"\r\n":
                nxt = end + 2
            elif data[end : end + 1] == b"\r":
                nxt = end + 1
            else:
                nxt = end
        names.append(name)
        i = nxt
    return names, size, certain


# ─── Barriers ─────────────────────────────────────────────────────────────────


class RespBarrier:
    """PING <token>: the reply carries the token back, as a bulk string or, in subscribe mode,
    inside the pong push. No earlier reply can contain a random token."""

    def __init__(self, names):
        self.token = os.urandom(8).hex().encode()
        self.command = b"*2\r\n$4\r\nPING\r\n$%d\r\n%s\r\n" % (len(self.token), self.token)
        self.tail = b""
        self.seen = False

    def feed(self, chunk):
        window = self.tail + chunk
        if self.token in window:
            self.seen = True
        self.tail = window[-(len(self.token) - 1) :]


class MemcacheBarrier:
    """version: answered even with "noreply". Complete once as many VERSION reply lines were
    seen as the input dispatches "version" commands, plus one for the barrier. VALUE/VA data
    blocks are skipped so stored values cannot be mistaken for replies."""

    command = b"version\r\n"

    def __init__(self, names):
        self.needed = 1 + names.count(b"version")
        self.buf = b""
        self.versions = 0
        self.skip = None  # bytes of a data block still to consume

    @property
    def seen(self):
        return self.versions >= self.needed

    def feed(self, chunk):
        self.buf += chunk
        while True:
            if self.skip is not None:
                if len(self.buf) < self.skip + 2:
                    return
                self.buf = self.buf[self.skip + 2 :]
                self.skip = None
                continue
            end = self.buf.find(b"\r\n")
            if end < 0:
                return
            # Split on spaces only, like the server: a key may contain a tab, and splitting on
            # any whitespace would shift the length field of a VALUE line.
            tokens = [t for t in self.buf[:end].split(b" ") if t]
            self.buf = self.buf[end + 2 :]
            if not tokens:
                continue
            if tokens[0] in (b"VALUE", b"VA"):
                try:
                    length = int(tokens[3] if tokens[0] == b"VALUE" else tokens[1])
                except (IndexError, ValueError):
                    length = -1
                if length >= 0:
                    self.skip = length
            elif tokens[0] == b"VERSION":
                self.versions += 1


PROTOCOLS = {"resp": (RespBarrier, resp_walk), "memcache": (MemcacheBarrier, memcache_walk)}


# ─── Transport ────────────────────────────────────────────────────────────────


def connect(host, port, timeout):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((host, port))
    except OSError:
        s.close()
        return None
    return s


def send_harness(host, port, data):
    s = connect(host, port, 0.2)
    if s is None:
        return None
    closed = False
    delivered = False
    try:
        delivered = s.send(data) > 0  # one send() like the harness; a short write is not retried
        closed = s.recv(4096) == b""
    except socket.timeout:
        pass
    except OSError:  # reset by peer: the server may be dying
        closed = True
    s.close()
    reason = "" if delivered else "send failed, input not delivered"
    return {
        "closed": closed,
        "verified": delivered,
        "reason": reason,
        "bytes": None,
        "delivered": delivered,
    }


def send_drain(host, port, data, barrier_cls, walk, timeout):
    s = connect(host, port, timeout)
    if s is None:
        return None
    names, prefix_len, certain = walk(data)
    raw = prefix_len < len(data) or not certain
    barrier = None if raw else barrier_cls(names)
    closed = False
    sent = 0
    received = 0
    deadline = time.monotonic() + timeout  # shared by the input, the barrier and the replies
    timed_out = False
    try:
        # send() by send(): a write that fails half-way has still delivered a prefix of the
        # input, and that prefix may be what crashes the server. Every write is bounded by the
        # same deadline as the replies.
        while sent < len(data):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise socket.timeout("deadline reached while sending")
            s.settimeout(remaining)
            n = s.send(data[sent:])
            if n == 0:
                raise OSError("send() wrote nothing")
            sent += n
        if barrier is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise socket.timeout("deadline reached before the barrier")
            s.settimeout(remaining)
            s.sendall(barrier.command)
        while barrier is None or not barrier.seen:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            # Without a barrier the only end signal is silence (or the server closing).
            s.settimeout(min(IDLE_GAP_SEC, remaining) if raw else remaining)
            try:
                chunk = s.recv(65536)
            except socket.timeout:
                break
            if not chunk:
                closed = True
                break
            received += len(chunk)
            if barrier is not None:
                barrier.feed(chunk)
    except socket.timeout:
        timed_out = True
    except OSError:
        closed = True
    s.close()
    verified = barrier is not None and barrier.seen
    delivered = sent > 0
    if verified:
        reason = ""
    elif not delivered:
        reason = (
            "send timed out, input not delivered"
            if timed_out
            else "send failed, input not delivered"
        )
    elif sent < len(data):
        what = "timed out" if timed_out else "failed"
        reason = f"send {what} after {sent} of {len(data)} bytes"
    elif prefix_len < len(data):
        reason = "ends mid-command, sent without a barrier"
    elif raw:
        reason = "parse not mirrored exactly, sent without a barrier"
    elif closed:
        reason = "server closed the connection"
    else:
        reason = "no reply in time"
    return {
        "closed": closed,
        "verified": verified,
        "reason": reason,
        "bytes": received,
        "delivered": delivered,
    }


def alive(host, port, pid):
    """/proc when a pid is known, otherwise bare TCP connects: never sends a command. Three
    refused connects in a row are needed to call the server dead, one may be transient."""
    if pid is not None:
        try:
            with open(f"/proc/{pid}/stat") as f:
                return ") Z " not in f.read()
        except OSError:
            return False
    for attempt in range(3):
        if listening(host, port):
            return True
        if attempt < 2:
            time.sleep(0.05)
    return False


def listening(host, port):
    s = connect(host, port, 1.0)
    if s is None:
        return False
    s.close()
    return True


def _proc_addr(hex_addr):
    """Decodes a /proc/net/tcp{,6} address column into (ipv4 or None, is_wildcard)."""
    raw = bytes.fromhex(hex_addr)
    if len(raw) == 4:
        ip = socket.inet_ntop(socket.AF_INET, raw[::-1])
        return ip, ip == "0.0.0.0"
    words = [raw[k : k + 4][::-1] for k in range(0, 16, 4)]
    ip = socket.inet_ntop(socket.AF_INET6, b"".join(words))
    if ip == "::":
        return None, True
    return (ip[7:] if ip.startswith("::ffff:") else None), False


def _proc_tcp_rows(pid):
    """(local_ip, local_wildcard, local_port, remote_ip, remote_port, state, inode) for every
    row of the pid's /proc/<pid>/net/tcp and tcp6, or None when neither file is readable."""
    rows, readable = [], False
    for name in ("tcp", "tcp6"):
        try:
            with open(f"/proc/{pid}/net/{name}") as f:
                lines = f.readlines()[1:]
        except OSError:
            continue
        readable = True
        for line in lines:
            cols = line.split()
            if len(cols) < 10:
                continue
            laddr, lport = cols[1].rsplit(":", 1)
            raddr, rport = cols[2].rsplit(":", 1)
            lip, lwild = _proc_addr(laddr)
            rip, _ = _proc_addr(raddr)
            rows.append((lip, lwild, int(lport, 16), rip, int(rport, 16), cols[3], cols[9]))
    return rows if readable else None


def endpoint_owned_by(pid, ip, port):
    """True only when a probe connection (no bytes sent) to ip:port provably lands on `pid`:
    the connection shows up as ESTABLISHED in the pid's network namespace, and the one listening
    socket the kernel would pick for ip:port there (an exact-address listener wins over a
    wildcard) is a file descriptor of the pid. False otherwise, None when /proc is unreadable.
    Accepted connections are not looked up in the fd table on purpose: with io_uring they may
    live as registered files without an fd."""
    probe = connect(ip, port, 1.0)
    if probe is None:
        return False
    try:
        our_ip, our_port = probe.getsockname()[:2]
        deadline = time.monotonic() + 1.0
        while True:
            rows = _proc_tcp_rows(pid)
            if rows is None:
                return None
            # ESTABLISHED, or SYN_RECV: a listener with TCP_DEFER_ACCEPT keeps a connection
            # that has not sent data yet as a request socket, which is still ours.
            established = any(
                st in ("01", "03") and lip == ip and lp == port and rip == our_ip and rp == our_port
                for lip, _, lp, rip, rp, st, _ in rows
            )
            if established:
                break
            if time.monotonic() >= deadline:
                return False  # the connection does not terminate in this namespace
            time.sleep(0.02)
    finally:
        probe.close()
    listeners = [
        (lip, lwild, ino) for lip, lwild, lp, _, _, st, ino in rows if st == "0A" and lp == port
    ]
    exact = [ino for lip, _, ino in listeners if lip == ip]
    wild = [ino for _, lwild, ino in listeners if lwild]
    chosen = exact or wild
    if len(chosen) != 1:
        return False  # no listener, or several (SO_REUSEPORT / v6-only plus v4): not provable
    try:
        fds = os.listdir(f"/proc/{pid}/fd")
    except OSError:
        return None
    for fd in fds:
        try:
            target = os.readlink(f"/proc/{pid}/fd/{fd}")
        except OSError:
            continue
        if target == f"socket:[{chosen[0]}]":
            return True
    return False


def died_within(grace, liveness):
    # /proc is free to poll; without a pid every poll is a TCP connect, so poll less often.
    interval = 0.01 if liveness[2] is not None else 0.1
    deadline = time.monotonic() + grace
    while True:
        if not alive(*liveness):
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(interval)


# ─── Main ─────────────────────────────────────────────────────────────────────


def report_death(inputs, idx, unverified):
    label = inputs[idx][0]
    print(f"\033[0;31m[DEAD]\033[0m Server died after input {label} ({idx + 1} of {len(inputs)})")
    if idx:
        prev = inputs[idx - 1][0]
        print(
            f"\033[0;31m[DEAD]\033[0m If the fatal message in the server log predates this input,"
            f" the previous one ({prev}) ran the fatal command; replay both alone to confirm."
        )
    earlier = [(name, reason) for name, reason in unverified if name != label]
    if earlier:
        shown = ", ".join(f"{name} ({reason})" for name, reason in earlier[:5])
        more = f", and {len(earlier) - 5} more" if len(earlier) > 5 else ""
        print(
            f"\033[0;31m[DEAD]\033[0m Note: {len(earlier)} other input(s) could not be"
            f" verified or were skipped, so the state may differ from the fuzz run: {shown}{more}"
        )
    sys.exit(3)


def tcp_port(text):
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a number: {text!r}")
    if not 1 <= value <= 65535:
        raise argparse.ArgumentTypeError(f"port must be 1..65535: {text!r}")
    return value


def non_negative_int(text):
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a number: {text!r}")
    if value < 0:
        raise argparse.ArgumentTypeError(f"must be >= 0: {text!r}")
    return value


def report_death_after(inputs, last_sent, unverified):
    """For a death noticed while an input could not be delivered: the current input never
    reached the server, so the death belongs to the last one that did."""
    if last_sent is None:
        print("\033[0;31m[DEAD]\033[0m Server died before any input was delivered")
        sys.exit(3)
    report_death(inputs, last_sent, unverified)


def finite_seconds(minimum):
    def parse(text):
        try:
            value = float(text)
        except ValueError:
            raise argparse.ArgumentTypeError(f"not a number: {text!r}")
        if math.isnan(value) or math.isinf(value) or value < minimum:
            raise argparse.ArgumentTypeError(f"must be a finite number >= {minimum:g}: {text!r}")
        return value

    return parse


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("crash_dir")
    ap.add_argument("crash_id")
    ap.add_argument("host", nargs="?", default="127.0.0.1")
    ap.add_argument("port", nargs="?", type=tcp_port, default=6379)
    ap.add_argument("--mode", choices=["drain", "harness"], default="drain")
    ap.add_argument("--pid", type=int, help="server pid; liveness is then checked via /proc")
    ap.add_argument("--protocol", choices=sorted(PROTOCOLS), default="resp")
    ap.add_argument("--tail", type=non_negative_int, help="replay only the last N RECORD inputs")
    ap.add_argument("--no-crash-input", action="store_true", help="do not send the crash input")
    ap.add_argument(
        "--timeout", type=finite_seconds(0.01), default=2.0, help="drain cap per input, seconds"
    )
    ap.add_argument(
        "--wait", type=finite_seconds(0), default=10.0, help="seconds to wait for the listener"
    )
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    records = record_files(args.crash_dir, args.crash_id)
    if args.tail is not None:
        records = records[-args.tail :] if args.tail > 0 else []
    inputs = [(os.path.basename(r).split(",", 1)[1], r) for r in records]

    if not args.no_crash_input:
        crash_file = crash_input_file(args.crash_dir, args.crash_id)
        if not crash_file:
            print(f"\033[0;31m[ERROR]\033[0m Crash input not found for id:{args.crash_id}")
            sys.exit(1)
        inputs.append((f"crash input {os.path.basename(crash_file)}", crash_file))
    if not inputs:
        print("\033[0;31m[ERROR]\033[0m Nothing to replay")
        sys.exit(1)

    print(
        f"\033[0;32m[INFO]\033[0m Replaying crash {args.crash_id} against {args.host}:{args.port}"
    )
    print(
        f"\033[0;32m[INFO]\033[0m Mode: {args.mode}, protocol: {args.protocol}, inputs:"
        f" {len(inputs)} ({len(records)} RECORD files)"
    )

    # One concrete IPv4 address for everything that follows: the ownership check and every
    # connect must talk to the same endpoint even when the name has several addresses.
    try:
        resolved = socket.getaddrinfo(args.host, args.port, socket.AF_INET, socket.SOCK_STREAM)
    except OSError:
        resolved = []
    if not resolved:
        print(f"\033[0;31m[ERROR]\033[0m {args.host} does not resolve to an IPv4 address")
        sys.exit(1)
    if resolved[0][4][0] != args.host:
        print(f"\033[0;32m[INFO]\033[0m Using {resolved[0][4][0]} for {args.host}")
        args.host = resolved[0][4][0]

    # Readiness means an open listener, whatever --pid says: the process exists before it listens.
    deadline = time.monotonic() + args.wait
    while not listening(args.host, args.port):
        if time.monotonic() >= deadline:
            print(
                f"\033[0;31m[ERROR]\033[0m No listener at {args.host}:{args.port} after"
                f" {args.wait:g}s; is Dragonfly running?"
            )
            sys.exit(1)
        time.sleep(0.1)
    # Never feed fuzz input to whatever else happens to answer on the port (e.g. when the
    # server failed to bind): with --pid that process must provably accept our connections.
    if args.pid is not None and endpoint_owned_by(args.pid, args.host, args.port) is not True:
        print(
            f"\033[0;31m[ERROR]\033[0m Cannot confirm that {args.host}:{args.port} is served by"
            f" pid {args.pid} (wrong pid, another service, or /proc not readable);"
            " not sending anything."
        )
        sys.exit(1)

    liveness = (args.host, args.port, args.pid)
    barrier_cls, walk = PROTOCOLS[args.protocol]
    unverified = []
    skipped = 0
    last_sent = None  # index of the last input that actually reached the server
    truncated = 0
    started = time.monotonic()
    for idx, (label, path) in enumerate(inputs):
        if idx % 1000 == 0 and idx:
            print(f"\033[1;33m[REPLAY]\033[0m Progress: {idx} / {len(inputs)}")
        with open(path, "rb") as f:  # the harness never sent more than one buffer per iteration
            data = f.read(HARNESS_BUF + 1)
        if len(data) > HARNESS_BUF:
            data = data[:HARNESS_BUF]
            truncated += 1

        def send():
            if args.mode == "harness":
                return send_harness(args.host, args.port, data)
            return send_drain(args.host, args.port, data, barrier_cls, walk, args.timeout)

        result = send()
        if result is None:
            if args.mode == "harness":  # the harness neither retries nor pauses: it moves on
                # With --pid the /proc check is free; without it no probe is made here, the
                # final check attributes a death to the input before the first refusal.
                if args.pid is not None and not alive(*liveness):
                    report_death_after(inputs, last_sent, unverified)
                unverified.append((label, "connection refused, input skipped"))
                skipped += 1
                continue
            # Refused: a dying process, or a transient failure while the server is alive.
            if died_within(DEATH_GRACE_SEC, liveness):
                report_death_after(inputs, last_sent, unverified)
            for _ in range(CONNECT_RETRIES):
                time.sleep(0.05)
                result = send()
                if result is not None:
                    break
            if result is None:
                if alive(*liveness):
                    print(
                        f"\033[0;31m[ERROR]\033[0m Cannot connect to {args.host}:{args.port} for"
                        f" input {label} although the server is alive; check host/port."
                    )
                    sys.exit(1)
                report_death_after(inputs, last_sent, unverified)

        if result["delivered"]:
            last_sent = idx
        if not result["verified"]:
            unverified.append((label, result["reason"]))
        if args.verbose and args.mode == "drain":
            state = "verified" if result["verified"] else f"UNVERIFIED ({result['reason']})"
            print(
                f"    {label}: {state}, {result['bytes']} reply bytes,"
                f" closed by server={result['closed']}"
            )

        # A server-side close means the process may be dying right now. Drain mode waits for
        # it; harness mode keeps the fuzzer's pace and lets a later input observe the death.
        # Without --pid a liveness check is a TCP probe, so harness mode never probes between
        # inputs (a QUIT or a crash closes the connection either way); the final check below
        # catches a death.
        if args.mode == "harness" and args.pid is None:
            continue
        if not alive(*liveness) or (
            args.mode == "drain" and result["closed"] and died_within(DEATH_GRACE_SEC, liveness)
        ):
            report_death_after(inputs, last_sent, unverified)

    # The last input gets a longer grace period: an abort raised by its tail would otherwise
    # be observed by nobody. Refused inputs never reached the server, so the death is charged
    # to the last one that did.
    if died_within(FINAL_GRACE_SEC, liveness):
        report_death_after(inputs, last_sent, unverified)

    elapsed = time.monotonic() - started
    print(
        f"\033[0;32m[INFO]\033[0m Replayed {len(inputs)} inputs in {elapsed:.1f}s;"
        " server still alive."
    )
    if truncated:
        print(
            f"\033[0;32m[INFO]\033[0m {truncated} input(s) were cut to {HARNESS_BUF} bytes,"
            " the harness buffer size."
        )
    if args.mode == "harness" and not unverified:
        sys.exit(0)
    if unverified:
        shown = ", ".join(f"{label} ({reason})" for label, reason in unverified[:10])
        more = f", and {len(unverified) - 10} more" if len(unverified) > 10 else ""
        print(
            f"\033[1;33m[WARN]\033[0m {len(unverified)} input(s) could not be verified, so their"
            f" tails may not have run: {shown}{more}"
        )
        sys.exit(4)
    print(
        "\033[0;32m[INFO]\033[0m Every recorded command executed;"
        " try --mode harness for timing-dependent bugs."
    )
    sys.exit(0)


if __name__ == "__main__":
    main()
