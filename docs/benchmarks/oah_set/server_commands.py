#!/usr/bin/env python3
"""Measure set commands in a running Dragonfly.

Every server runs one proactor thread pinned to --cpu. OAHSet is measured in every --dragonfly
binary and StringSet in the first one.

Whole-set commands: pipelined SADD batches of 1,000 build a set of --members random 32-byte ASCII
members, and the slow log keeps every batch that ran for at least 1 ms: the batches that grew the
table. A client walks the set with SSCAN, and SAVE writes a snapshot. DEL frees the set in one step
and DFLY LOAD reads the snapshot back, three times, so the first DEL frees the set that SADD built
and the others free loaded sets. The client walks the loaded set with SSCAN again, and UNLINK frees
it in steps between other work; the UNLINK row lasts until the memory is freed, polled every 2 ms.
Throughout, a probe connection sends PING about every 0.15 ms and records how long each reply took,
which is how long any other client would have waited.

Single-member commands: memtier_benchmark saturates the server with SISMEMBER for random members
of a set of --members 32-byte members. The CPU time of all server threads, read from /proc, is
divided by the number of commands the server processed.
"""

import argparse
import csv
import datetime
import hashlib
import io
import json
import os
from pathlib import Path
import random
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time

PREFIX = "oah-set-command-overhead"  # 24 characters; 8 more digits make 32-byte members
FIRST = 10_000_000
# (connections per client thread, commands in flight per connection). Both keep the server busy:
# at lower load, Dragonfly polls for 20 µs after each event, which would count as command time.
LOADS = ((20, 1), (2, 32))
ALPHABET = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_"
SEED = 20261116
BATCH = 1000
SLOW_US = 1000
SNAPSHOT = "oah-set"
SAVES = 3
LOADS_PER_SERVER = 3
SSCAN_COUNT = 1000
PROBE_SLOW_NS = 100_000

PROBE = r"""
import json, signal, socket, sys, time

stop = False


def handle(*_):
    global stop
    stop = True


signal.signal(signal.SIGTERM, handle)
sock = socket.create_connection(("127.0.0.1", int(sys.argv[1])))
sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
slow_ns, pings, slow = int(sys.argv[2]), 0, []
print("ready", flush=True)
while not stop:
    start = time.monotonic_ns()
    sock.sendall(b"*1\r\n$4\r\nPING\r\n")
    reply = b""
    while not reply.endswith(b"\r\n"):
        chunk = sock.recv(64)
        if not chunk:
            sys.exit("the server closed the probe connection")
        reply += chunk
    elapsed = time.monotonic_ns() - start
    pings += 1
    if elapsed > slow_ns:
        slow.append((start, elapsed))
    time.sleep(0.0001)
print(json.dumps({"pings": pings, "slow": slow}), flush=True)
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dragonfly",
        action="append",
        required=True,
        metavar="LABEL=PATH",
        help="a Dragonfly binary; StringSet is measured in the first one",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cpu", type=int, default=min(os.sched_getaffinity(0)))
    parser.add_argument("--client-cpus", default="1-3")
    parser.add_argument("--port", type=int, default=16379)
    parser.add_argument("--members", type=int, default=1_000_000)
    parser.add_argument("--seconds", type=int, default=10)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    binaries = [parse_binary(value) for value in args.dragonfly]
    servers = [(label, path, "OAHSet") for label, path in binaries]
    servers.append((*binaries[0], "StringSet"))
    args.output.mkdir(parents=True, exist_ok=True)

    members = random_members(args.members)
    whole, point, commands = [], [], []
    for repeat in range(args.repeats):
        for build, binary, container in servers:
            labels = {"repeat": repeat, "build": build, "set": container}
            with tempfile.TemporaryDirectory() as scratch:
                server = start_server(args, binary, container == "OAHSet", Path(scratch))
                try:
                    client = Client(args.port)
                    empty = info(client, "MEMORY", "used_memory")
                    rows = whole_set(args, server.pid, client, members, Path(scratch), empty)
                    for row in rows:
                        whole.append({**labels, **row})
                        print(whole[-1], flush=True)
                    client.call("FLUSHALL")
                    settle(client, empty)
                    add_members(client, b"s", [f"{PREFIX}{i}".encode() for i in key_range(args)])
                    for clients, pipeline in LOADS:
                        row, command = sismember(args, server.pid, client, clients, pipeline)
                        point.append({**labels, **row})
                        commands.append(command)
                        print(point[-1], flush=True)
                finally:
                    stop_server(args, server)

    (args.output / "whole_set_commands.csv").write_text(as_csv(whole))
    (args.output / "command_overhead.csv").write_text(as_csv(point))
    metadata = {
        "finished_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "binaries": [describe(label, path) for label, path in binaries],
        "server_flags": server_flags(args, "true for OAHSet, false for StringSet", "SCRATCH")[4:],
        "server_cpu": args.cpu,
        "client_cpus": args.client_cpus,
        "members": args.members,
        "member_bytes": 32,
        "seed": SEED,
        "sadd_batch": BATCH,
        "slowlog_us": SLOW_US,
        "saves_per_server": SAVES,
        "loads_per_server": LOADS_PER_SERVER,
        "sscan_count": SSCAN_COUNT,
        "probe_slow_us": PROBE_SLOW_NS // 1000,
        "memtier_commands": commands,
    }
    (args.output / "server_commands.json").write_text(json.dumps(metadata, indent=2) + "\n")


def parse_binary(value):
    label, separator, path = value.partition("=")
    if not separator or not label or not path:
        raise SystemExit(f"--dragonfly expects LABEL=PATH, got {value!r}")
    return label, Path(path)


def describe(label, path):
    version = subprocess.run([path, "--version"], capture_output=True, text=True)
    return {
        "label": label,
        "path": str(path.resolve()),
        "version": re.sub(r"\x1b\[[\d;]*m", "", version.stdout).strip(),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "mtime_utc": datetime.datetime.fromtimestamp(
            path.stat().st_mtime, datetime.timezone.utc
        ).isoformat(),
    }


def random_members(count):
    rng = random.Random(SEED)
    members, seen = [], set()
    while len(members) < count:
        member = bytes(rng.choices(ALPHABET, k=32))
        if member not in seen:
            seen.add(member)
            members.append(member)
    return members


def key_range(args):
    return range(FIRST, FIRST + args.members)


def server_flags(args, use_oah_set, scratch):
    return [
        "taskset",
        "-c",
        str(args.cpu),
        "DRAGONFLY",
        f"--port={args.port}",
        "--proactor_threads=1",
        f"--use_oah_set={use_oah_set}",
        f"--dbfilename={SNAPSHOT}",
        f"--dir={scratch}",
        f"--log_dir={scratch}",
        f"--slowlog_log_slower_than={SLOW_US}",
        "--slowlog_max_len=1000",
    ]


def start_server(args, binary, oah, scratch):
    command = server_flags(args, "true" if oah else "false", scratch)
    command[3] = str(binary)
    server = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(100):
        try:
            if Client(args.port).call("PING") == "PONG":
                return server
        except (OSError, RuntimeError):
            pass
        time.sleep(0.1)
    server.terminate()
    raise RuntimeError("Dragonfly did not start")


def stop_server(args, server):
    try:
        Client(args.port).call("SHUTDOWN", "NOSAVE")
    except (OSError, RuntimeError):
        pass
    try:
        server.wait(timeout=30)
    except subprocess.TimeoutExpired:
        server.terminate()
        server.wait(timeout=30)


class Client:
    """A blocking RESP2 client: enough to time commands without a client library."""

    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port))
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.file = self.sock.makefile("rb")

    def call(self, *command):
        self.sock.sendall(encode(command))
        return self.read()

    def read(self):
        line = self.file.readline()
        kind, rest = line[:1], line[1:-2]
        if kind == b"+":
            return rest.decode()
        if kind == b"-":
            raise RuntimeError(rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"$":
            size = int(rest)
            return None if size < 0 else self.file.read(size + 2)[:-2]
        if kind == b"*":
            size = int(rest)
            return None if size < 0 else [self.read() for _ in range(size)]
        raise RuntimeError(f"unexpected reply {line!r}")


def encode(command):
    parts = [part if isinstance(part, bytes) else str(part).encode() for part in command]
    return b"*%d\r\n" % len(parts) + b"".join(b"$%d\r\n%s\r\n" % (len(p), p) for p in parts)


def add_members(client, key, members):
    batches = [members[i : i + BATCH] for i in range(0, len(members), BATCH)]
    client.sock.sendall(b"".join(encode((b"SADD", key, *batch)) for batch in batches))
    added = sum(client.read() for _ in batches)
    if added != len(members) or client.call("SCARD", key) != len(members):
        raise RuntimeError(f"SADD added {added} of {len(members)} members")


def info(client, section, field):
    text = client.call("INFO", section).decode()
    return int(
        next(line for line in text.splitlines() if line.startswith(field + ":")).split(":")[1]
    )


def settle(client, empty, interval=0.05, margin=4 << 20):
    # Wait until the deleted set is freed, in case that happens in the background.
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if client.call("DBSIZE") == 0 and info(client, "MEMORY", "used_memory") < empty + margin:
            return
        time.sleep(interval)
    raise RuntimeError("the deleted set was not freed")


def server_schedstat(pid):
    # taskset execs Dragonfly in place, so pid is the server's. The first two schedstat fields are
    # the nanoseconds a thread has run and has waited, runnable, for a CPU.
    run = wait = 0
    for task in Path(f"/proc/{pid}/task").iterdir():
        fields = (task / "schedstat").read_text().split()
        run += int(fields[0])
        wait += int(fields[1])
    return run, wait


def whole_set(args, pid, client, members, scratch, empty):
    probe = subprocess.Popen(
        ["taskset", "-c", str(min(cpu_list(args.client_cpus))), sys.executable, "-c", PROBE]
        + [str(args.port), str(PROBE_SLOW_NS)],
        stdout=subprocess.PIPE,
        text=True,
    )
    if probe.stdout.readline().strip() != "ready":
        raise RuntimeError("the probe did not start")
    rows, windows = [], []

    def timed(operation, function):
        cpu_before, start = server_schedstat(pid)[0], time.monotonic_ns()
        extra = function() or {}
        end, cpu_after = time.monotonic_ns(), server_schedstat(pid)[0]
        row = {
            "operation": operation,
            "wall_ms": round((end - start) / 1e6, 2),
            "server_cpu_ms": round((cpu_after - cpu_before) / 1e6, 2),
            **extra,
        }
        rows.append(row)
        windows.append((row, start, end))
        return row

    try:
        build = timed("build", lambda: add_members(client, b"r", members))
        build["bytes_per_member"] = round(
            (info(client, "MEMORY", "used_memory") - empty) / len(members), 2
        )
        rows.extend(grows(client, members))

        def walk():
            cursor, walked, calls = b"0", 0, 0
            while True:
                cursor, batch = client.call("SSCAN", b"r", cursor, "COUNT", SSCAN_COUNT)
                walked, calls = walked + len(batch), calls + 1
                if cursor == b"0":
                    break
            if walked != len(members):
                raise RuntimeError(f"SSCAN returned {walked} of {len(members)} members")
            return {"calls": calls}

        timed("sscan", walk)

        def save():
            if client.call("SAVE") != "OK":
                raise RuntimeError("SAVE failed")
            return {"snapshot_bytes": sum(f.stat().st_size for f in scratch.glob(SNAPSHOT + "*"))}

        for _ in range(SAVES):
            timed("save", save)
        summary = next(scratch.glob(SNAPSHOT + "*summary*"))

        def load():
            if client.call("DFLY", "LOAD", str(summary)) != "OK":
                raise RuntimeError("DFLY LOAD failed")

        def delete(command):
            if client.call(command, b"r") != 1:
                raise RuntimeError(f"{command} did not delete the set")
            if command == "UNLINK":
                settle(client, empty, interval=0.002, margin=64 << 10)

        for _ in range(LOADS_PER_SERVER):
            timed("del", lambda: delete("DEL"))
            settle(client, empty)
            timed("load", load)
            if client.call("SCARD", b"r") != len(members):
                raise RuntimeError("DFLY LOAD lost members")

        timed("sscan", walk)
        timed("unlink", lambda: delete("UNLINK"))
    finally:
        probe.send_signal(signal.SIGTERM)
        result = json.loads(probe.communicate(timeout=30)[0])
    for row, start, end in windows:
        waits = [ns for begin, ns in result["slow"] if begin < end and begin + ns > start]
        row["max_wait_ms"] = round(max(waits, default=0) / 1e6, 2)
    return rows


def grows(client, members):
    # A slow-log entry holds the first 31 arguments; the first member identifies the batch.
    batch_of = {members[i]: i for i in range(0, len(members), BATCH)}
    rows = []
    for entry in client.call("SLOWLOG", "GET", "-1"):
        duration_us, command = entry[2], entry[3]
        if command[0].upper() != b"SADD":
            continue
        rows.append(
            {
                "operation": "slow_sadd",
                "wall_ms": round(duration_us / 1000, 2),
                "members_before": batch_of[command[2]],
            }
        )
    client.call("SLOWLOG", "RESET")
    return sorted(rows, key=lambda row: row["members_before"])


def sismember(args, pid, client, clients, pipeline):
    threads = len(cpu_list(args.client_cpus))
    command = [
        "taskset",
        "-c",
        args.client_cpus,
        "memtier_benchmark",
        "--server=127.0.0.1",
        f"--port={args.port}",
        "--protocol=redis",
        f"--threads={threads}",
        f"--clients={clients}",
        f"--pipeline={pipeline}",
        f"--test-time={args.seconds + 2}",
        "--command=SISMEMBER s __key__",
        "--command-key-pattern=R",
        f"--key-prefix={PREFIX}",
        f"--key-minimum={FIRST}",
        f"--key-maximum={FIRST + args.members - 1}",
        "--hide-histogram",
    ]

    # The window starts after the clients connect and ends before they disconnect. The INFO calls
    # add two commands, which is negligible next to millions of SISMEMBERs.
    with subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) as tool:
        time.sleep(1)
        before = (processed(client), *server_schedstat(pid), time.perf_counter())
        time.sleep(args.seconds)
        after = (processed(client), *server_schedstat(pid), time.perf_counter())
    if tool.returncode != 0:
        raise RuntimeError("memtier_benchmark failed")
    count, run, wait, seconds = (end - start for start, end in zip(before, after))
    return {
        "connections": threads * clients,
        "pipeline": pipeline,
        "commands_per_second": round(count / seconds),
        "server_busy": round(run / 1e9 / seconds, 3),
        "server_wait_ms": round(wait / 1e6, 1),
        "cpu_ns_per_command": round(run / count, 1),
    }, command


def processed(client):
    return info(client, "STATS", "total_commands_processed")


def cpu_list(spec):
    cpus = set()
    for part in spec.split(","):
        first, _, last = part.partition("-")
        cpus.update(range(int(first), int(last or first) + 1))
    return cpus


def as_csv(rows):
    fields = list(dict.fromkeys(field for row in rows for field in row))
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


if __name__ == "__main__":
    main()
