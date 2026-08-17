#!/usr/bin/env python3
"""Benchmark full-sync fanout with one master and one to ten replicas.

Run from the repository root:

    ./tools/full_sync_fanout_benchmark.py 4GiB

The sole benchmark argument is the requested data size. The runner starts one master and, for
each replica count, starts that many replicas. It measures the full-sync time with fanout disabled
and enabled. Every Dragonfly process uses ten proactors, while ten client threads continuously
issue random GET, SET, HGET, HSET, and INCRBY commands against the master.

The runner uses only the Python standard library. It expects an optimized Dragonfly binary at
build-opt/dragonfly. Set DRAGONFLY_BIN to use a binary elsewhere. The target machine must permit
the default io_uring backend to lock sufficient memory. Unlimited locked memory is supported but
not required; each started process is checked to ensure that it is actually using io_uring.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime
import json
import math
import os
import random
import re
import socket
import subprocess
import sys
import threading
import time
import traceback
from collections import Counter
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, Union


MAX_REPLICAS = 10
PROACTOR_THREADS = 10
LOAD_THREADS = 10
FANOUT_DELAY_SECONDS = 1
FULL_SYNC_TIMEOUT_SECONDS = 3600
SNAPSHOT_VALUE_BYTES = 1024 * 1024
LOAD_STRING_KEY_COUNT = 1024
LOAD_HASH_KEY_COUNT = 128
LOAD_COUNTER_KEY_COUNT = 128
LOAD_VALUE = b"x" * 1024
LOAD_COMMANDS = ("GET", "SET", "HGET", "HSET", "INCRBY")
LOAD_RECONNECT_TIMEOUT_SECONDS = 30
LOAD_RECONNECT_RETRY_SECONDS = 0.1


class RespError(RuntimeError):
    """A Redis/RESP error reply."""


class BenchmarkSetupError(RuntimeError):
    """The host does not meet a prerequisite for this benchmark."""


class RespConnection:
    """A tiny synchronous RESP client used to keep the benchmark dependency-free."""

    def __init__(self, port: int, timeout: float = 60) -> None:
        self._socket = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        self._socket.settimeout(timeout)
        self._buffer = bytearray()
        self._closed = False

    def __enter__(self) -> "RespConnection":
        return self

    def __exit__(self, exc_type, exc_value, traceback_value) -> None:
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._socket.close()

    def command(self, *parts: Union[str, bytes, int]) -> object:
        self._socket.sendall(self._encode_command(parts))
        return self._read_response()

    @staticmethod
    def _encode_command(parts: Sequence[Union[str, bytes, int]]) -> bytes:
        encoded_parts = [part if isinstance(part, bytes) else str(part).encode() for part in parts]
        output = [f"*{len(encoded_parts)}\r\n".encode()]
        for value in encoded_parts:
            output.append(b"$" + str(len(value)).encode() + b"\r\n")
            output.append(value)
            output.append(b"\r\n")
        return b"".join(output)

    def _read_response(self) -> object:
        prefix = self._read_exactly(1)
        if prefix == b"+":
            return self._read_line().decode()
        if prefix == b"-":
            raise RespError(self._read_line().decode())
        if prefix == b":":
            return int(self._read_line())
        if prefix == b"$":
            size = int(self._read_line())
            if size == -1:
                return None
            result = self._read_exactly(size)
            if self._read_exactly(2) != b"\r\n":
                raise RespError("malformed bulk RESP response")
            return result
        if prefix == b"*":
            size = int(self._read_line())
            if size == -1:
                return None
            return [self._read_response() for _ in range(size)]
        raise RespError(f"unexpected RESP prefix: {prefix!r}")

    def _read_line(self) -> bytes:
        while True:
            end = self._buffer.find(b"\r\n")
            if end >= 0:
                line = bytes(self._buffer[:end])
                del self._buffer[: end + 2]
                return line
            self._receive()

    def _read_exactly(self, size: int) -> bytes:
        while len(self._buffer) < size:
            self._receive()
        result = bytes(self._buffer[:size])
        del self._buffer[:size]
        return result

    def _receive(self) -> None:
        chunk = self._socket.recv(65536)
        if not chunk:
            raise ConnectionError("connection closed")
        self._buffer.extend(chunk)


def command(port: int, *parts: Union[str, bytes, int], timeout: float = 60) -> object:
    with RespConnection(port, timeout) as connection:
        return connection.command(*parts)


def parse_size(value: str) -> int:
    """Parse a human-friendly byte size such as 4GiB, 4GB, or 4096MiB."""

    match = re.fullmatch(r"\s*(\d+)\s*([A-Za-z]*)\s*", value)
    if not match:
        raise argparse.ArgumentTypeError("size must look like 4GiB, 4GB, or 4096MiB")

    amount = int(match.group(1))
    unit = match.group(2).upper() or "B"
    multipliers = {
        "B": 1,
        "K": 1000,
        "KB": 1000,
        "M": 1000**2,
        "MB": 1000**2,
        "G": 1000**3,
        "GB": 1000**3,
        "T": 1000**4,
        "TB": 1000**4,
        "KI": 1024,
        "KIB": 1024,
        "MI": 1024**2,
        "MIB": 1024**2,
        "GI": 1024**3,
        "GIB": 1024**3,
        "TI": 1024**4,
        "TIB": 1024**4,
    }
    if unit not in multipliers:
        raise argparse.ArgumentTypeError(f"unsupported size unit: {match.group(2)}")

    byte_count = amount * multipliers[unit]
    if byte_count < SNAPSHOT_VALUE_BYTES:
        raise argparse.ArgumentTypeError("size must be at least 1MiB")
    return byte_count


def format_bytes(byte_count: int) -> str:
    for unit, divisor in (("TiB", 1024**4), ("GiB", 1024**3), ("MiB", 1024**2), ("KiB", 1024)):
        if byte_count >= divisor:
            return f"{byte_count / divisor:.2f} {unit}"
    return f"{byte_count} B"


def available_memory_bytes() -> Optional[int]:
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    return None


def describe_io_uring_memlock() -> str:
    """Report the inherited memlock limit without guessing the io_uring requirement.

    The kernel, io_uring configuration, and Dragonfly version determine how much locked memory a
    process needs. A finite limit can be entirely sufficient, so the authoritative check is the
    backend reported by each started Dragonfly process.
    """

    try:
        import resource

        soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_MEMLOCK)
        if soft_limit != resource.RLIM_INFINITY and hard_limit > soft_limit:
            try:
                resource.setrlimit(resource.RLIMIT_MEMLOCK, (hard_limit, hard_limit))
                soft_limit, _ = resource.getrlimit(resource.RLIMIT_MEMLOCK)
            except (OSError, ValueError):
                pass
        if soft_limit == resource.RLIM_INFINITY:
            return "io_uring (default; memlock is unlimited)"
        return f"io_uring (default; locked-memory limit: {format_bytes(soft_limit)})"
    except (AttributeError, ImportError, OSError, ValueError):
        return "io_uring (default; locked-memory limit unavailable)"


@dataclasses.dataclass
class PortPlan:
    data_base: int
    admin_base: int

    def data_port(self, index: int) -> int:
        return self.data_base + index

    def admin_port(self, index: int) -> int:
        return self.admin_base + index


def find_free_port_plan() -> PortPlan:
    """Find two contiguous, currently unused port ranges for eleven Dragonfly processes."""

    for _ in range(200):
        data_base = random.randrange(18000, 35000)
        admin_base = data_base + 20000
        ports = [data_base + index for index in range(MAX_REPLICAS + 1)]
        ports.extend(admin_base + index for index in range(MAX_REPLICAS + 1))
        sockets: List[socket.socket] = []
        try:
            for port in ports:
                probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                probe.bind(("127.0.0.1", port))
                sockets.append(probe)
            return PortPlan(data_base, admin_base)
        except OSError:
            pass
        finally:
            for probe in sockets:
                probe.close()
    raise RuntimeError("could not find a free contiguous port range")


@dataclasses.dataclass
class DragonflyProcess:
    role: str
    port: int
    process: subprocess.Popen
    log_path: Path
    log_file: object

    def stop(self) -> None:
        try:
            command(self.port, "SHUTDOWN", "NOSAVE", timeout=3)
        except (ConnectionError, OSError, RespError, TimeoutError):
            pass

        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        self.log_file.close()

    def log_tail(self, line_count: int = 30) -> str:
        try:
            lines = self.log_path.read_text(errors="replace").splitlines()
            return "\n".join(lines[-line_count:])
        except OSError:
            return "<log unavailable>"


def wait_for_server(port: int, process: DragonflyProcess) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if process.process.poll() is not None:
            raise RuntimeError(
                f"{process.role} on port {port} exited with {process.process.returncode}:\n"
                f"{process.log_tail()}"
            )
        try:
            if command(port, "PING", timeout=1) == "PONG":
                return
        except (ConnectionError, OSError, RespError, TimeoutError):
            pass
        time.sleep(0.05)
    raise TimeoutError(f"{process.role} on port {port} did not become ready")


def require_io_uring_backend(process: DragonflyProcess) -> None:
    reply = command(process.port, "INFO", "SERVER", timeout=5)
    if isinstance(reply, bytes):
        info = reply.decode(errors="replace")
    else:
        info = str(reply)
    if "multiplexing_api:iouring" not in info:
        raise BenchmarkSetupError(
            f"{process.role} started without io_uring. Ensure the target kernel supports io_uring and "
            "has sufficient locked memory. `ulimit -l unlimited` is one way to provide it."
        )


def start_dragonfly(
    binary: Path,
    run_dir: Path,
    role: str,
    port: int,
    admin_port: int,
    fanout_enabled: bool,
) -> DragonflyProcess:
    log_path = run_dir / f"{role}.log"
    log_file = log_path.open("wb", buffering=0)
    arguments = [
        str(binary),
        f"--port={port}",
        f"--admin_port={admin_port}",
        f"--proactor_threads={PROACTOR_THREADS}",
        f"--num_shards={PROACTOR_THREADS}",
        "--compression_mode=0",
        "--dbfilename=",
        "--alsologtostderr",
    ]
    if role == "master":
        arguments.extend(
            [
                f"--full_sync_fanout={'true' if fanout_enabled else 'false'}",
                f"--full_sync_fanout_delay={FANOUT_DELAY_SECONDS}",
                f"--full_sync_fanout_max_replicas={MAX_REPLICAS}",
            ]
        )
    process = DragonflyProcess(
        role=role,
        port=port,
        process=subprocess.Popen(
            arguments,
            stdin=subprocess.DEVNULL,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            cwd=run_dir,
        ),
        log_path=log_path,
        log_file=log_file,
    )
    try:
        wait_for_server(port, process)
        require_io_uring_backend(process)
        return process
    except Exception:
        process.stop()
        raise


class RandomCommandLoad:
    """Ten master-side clients issuing random commands and recovering from dropped connections."""

    def __init__(self, port: int) -> None:
        self._port = port
        self._stop = threading.Event()
        self._ready = [threading.Event() for _ in range(LOAD_THREADS)]
        self._connections: List[RespConnection] = []
        self._connections_lock = threading.Lock()
        self._errors: List[Exception] = []
        self._errors_lock = threading.Lock()
        self._stats: List[Counter] = [
            Counter({command_name: 0 for command_name in LOAD_COMMANDS})
            for _ in range(LOAD_THREADS)
        ]
        self._threads = [
            threading.Thread(
                target=self._run_worker,
                args=(worker_index,),
                name=f"fanout-random-load-{worker_index}",
            )
            for worker_index in range(LOAD_THREADS)
        ]

    def start(self) -> None:
        for thread in self._threads:
            thread.start()
        for worker_index, ready in enumerate(self._ready):
            if not ready.wait(timeout=15):
                raise TimeoutError(f"random-command worker {worker_index} did not start")
        self.raise_if_failed()

    def stop(self) -> None:
        self._stop.set()
        with self._connections_lock:
            connections = list(self._connections)
            self._connections.clear()
        for connection in connections:
            connection.close()
        for thread in self._threads:
            thread.join(timeout=15)
            if thread.is_alive():
                raise RuntimeError(f"{thread.name} did not stop")
        self.raise_if_failed()

    def snapshot(self) -> Counter:
        combined: Counter = Counter()
        for stats in self._stats:
            combined.update(stats)
        return combined

    def raise_if_failed(self) -> None:
        with self._errors_lock:
            if self._errors:
                raise RuntimeError("random-command worker failed") from self._errors[0]

    def _run_worker(self, worker_index: int) -> None:
        connection: Optional[RespConnection] = None
        random_source = random.Random(worker_index)
        try:
            while not self._stop.is_set():
                if connection is None:
                    connection = self._connect(worker_index)
                    if connection is None:
                        break
                    self._ready[worker_index].set()

                try:
                    command_name = self._random_command(connection, random_source)
                    self._stats[worker_index][command_name] += 1
                except (ConnectionError, OSError, TimeoutError):
                    self._remove_connection(connection)
                    connection = None
        except Exception as error:
            if not self._stop.is_set():
                with self._errors_lock:
                    self._errors.append(error)
        finally:
            self._ready[worker_index].set()
            if connection is not None:
                self._remove_connection(connection)

    def _connect(self, worker_index: int) -> Optional[RespConnection]:
        deadline = time.monotonic() + LOAD_RECONNECT_TIMEOUT_SECONDS
        last_error: Optional[Exception] = None
        while not self._stop.is_set() and time.monotonic() < deadline:
            connection: Optional[RespConnection] = None
            try:
                connection = RespConnection(self._port)
                if connection.command("PING") != "PONG":
                    raise RuntimeError("unexpected PING response")
                with self._connections_lock:
                    if self._stop.is_set():
                        connection.close()
                        return None
                    self._connections.append(connection)
                return connection
            except (ConnectionError, OSError, TimeoutError) as error:
                last_error = error
                if connection is not None:
                    connection.close()
                self._stop.wait(LOAD_RECONNECT_RETRY_SECONDS)
            except Exception:
                if connection is not None:
                    connection.close()
                raise

        if self._stop.is_set():
            return None
        raise RuntimeError(
            f"random-command worker {worker_index} could not reconnect within "
            f"{LOAD_RECONNECT_TIMEOUT_SECONDS} seconds"
        ) from last_error

    def _remove_connection(self, connection: RespConnection) -> None:
        with self._connections_lock:
            if connection in self._connections:
                self._connections.remove(connection)
        connection.close()

    @staticmethod
    def _random_command(connection: RespConnection, random_source: random.Random) -> str:
        choice = random_source.randrange(100)
        if choice < 40:
            key = f"fanout:load:string:{random_source.randrange(LOAD_STRING_KEY_COUNT)}"
            if connection.command("GET", key) is None:
                raise RuntimeError(f"missing load key {key}")
            return "GET"
        if choice < 80:
            key = f"fanout:load:string:{random_source.randrange(LOAD_STRING_KEY_COUNT)}"
            if connection.command("SET", key, LOAD_VALUE) != "OK":
                raise RuntimeError("unexpected SET response")
            return "SET"
        if choice < 90:
            key = f"fanout:load:hash:{random_source.randrange(LOAD_HASH_KEY_COUNT)}"
            if connection.command("HGET", key, "value") is None:
                raise RuntimeError(f"missing load hash {key}")
            return "HGET"
        if choice < 95:
            key = f"fanout:load:hash:{random_source.randrange(LOAD_HASH_KEY_COUNT)}"
            if connection.command("HSET", key, "value", str(random_source.getrandbits(64))) != 0:
                raise RuntimeError("HSET unexpectedly added a field")
            return "HSET"

        key = f"fanout:load:counter:{random_source.randrange(LOAD_COUNTER_KEY_COUNT)}"
        connection.command("INCRBY", key, 1)
        return "INCRBY"


def populate_master(master_port: int, data_size: int) -> Tuple[int, Tuple[str, ...]]:
    key_count = math.ceil(data_size / SNAPSHOT_VALUE_BYTES)
    print(
        f"Populating {key_count:,} random values of {format_bytes(SNAPSHOT_VALUE_BYTES)}...",
        flush=True,
    )
    result = command(
        master_port,
        "DEBUG",
        "POPULATE",
        key_count,
        "fanout:data",
        SNAPSHOT_VALUE_BYTES,
        "RAND",
        timeout=FULL_SYNC_TIMEOUT_SECONDS,
    )
    if result != "OK":
        raise RuntimeError(f"DEBUG POPULATE returned {result!r}")

    sample_indexes = sorted({0, key_count // 2, key_count - 1})
    return key_count, tuple(f"fanout:data:{index}" for index in sample_indexes)


def seed_load_keyspaces(master_port: int) -> None:
    string_parts: List[Union[str, bytes]] = ["MSET"]
    counter_parts: List[Union[str, bytes]] = ["MSET"]
    for index in range(LOAD_STRING_KEY_COUNT):
        string_parts.extend((f"fanout:load:string:{index}", LOAD_VALUE))
    for index in range(LOAD_COUNTER_KEY_COUNT):
        counter_parts.extend((f"fanout:load:counter:{index}", "0"))
    if command(master_port, *string_parts) != "OK":
        raise RuntimeError("failed to seed string load keys")
    if command(master_port, *counter_parts) != "OK":
        raise RuntimeError("failed to seed counter load keys")
    for index in range(LOAD_HASH_KEY_COUNT):
        if command(master_port, "HSET", f"fanout:load:hash:{index}", "value", "0") != 1:
            raise RuntimeError("failed to seed hash load keys")


def start_replication(replica_ports: Iterable[int], master_port: int) -> None:
    """Send REPLICAOF concurrently so all replicas can join the same fanout batch."""

    ports = list(replica_ports)
    ready = [threading.Event() for _ in ports]
    start = threading.Event()
    errors: List[Exception] = []
    errors_lock = threading.Lock()

    def initiate(port: int, ready_event: threading.Event) -> None:
        try:
            with RespConnection(port, timeout=60) as connection:
                ready_event.set()
                if not start.wait(timeout=30):
                    raise TimeoutError("replication request did not receive a start signal")
                if connection.command("REPLICAOF", "127.0.0.1", master_port) != "OK":
                    raise RuntimeError("unexpected REPLICAOF response")
        except Exception as error:
            with errors_lock:
                errors.append(error)
            ready_event.set()

    threads = [
        threading.Thread(target=initiate, args=(port, ready_event), name=f"replicaof-{port}")
        for port, ready_event in zip(ports, ready)
    ]
    for thread in threads:
        thread.start()
    try:
        for ready_event in ready:
            if not ready_event.wait(timeout=30):
                raise TimeoutError("replication client did not become ready")
        if errors:
            raise RuntimeError("failed to prepare replication request") from errors[0]

        start.set()
        for thread in threads:
            thread.join(timeout=60)
            if thread.is_alive():
                raise TimeoutError(f"{thread.name} did not return")
        if errors:
            raise RuntimeError("failed to start replication") from errors[0]
    finally:
        # Do not leave worker threads blocked if one connection fails before the requests start.
        start.set()
        for thread in threads:
            if thread.is_alive():
                thread.join(timeout=60)


def replica_is_stable(replica_port: int) -> bool:
    reply = command(replica_port, "ROLE", timeout=2)
    if not isinstance(reply, list) or len(reply) < 4:
        return False
    state = reply[3]
    if isinstance(state, bytes):
        state = state.decode()
    return state in ("stable_sync", "online")


def wait_for_replicas(
    master: DragonflyProcess, replica_ports: Sequence[int], replicas: Sequence[DragonflyProcess]
) -> None:
    deadline = time.monotonic() + FULL_SYNC_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if master.process.poll() is not None:
            raise RuntimeError(
                f"master exited with {master.process.returncode}:\n{master.log_tail()}"
            )
        for replica in replicas:
            if replica.process.poll() is not None:
                raise RuntimeError(
                    f"{replica.role} exited with {replica.process.returncode}:\n{replica.log_tail()}"
                )
        try:
            if all(replica_is_stable(port) for port in replica_ports):
                return
        except (ConnectionError, OSError, RespError, TimeoutError):
            pass
        time.sleep(0.05)
    raise TimeoutError("replicas did not reach stable sync before the timeout")


def wait_for_master_without_replicas(master_port: int) -> None:
    """Wait until disconnected benchmark replicas are removed from the master."""

    deadline = time.monotonic() + 30
    last_reply: object = None
    while time.monotonic() < deadline:
        try:
            reply = command(master_port, "ROLE", timeout=2)
            last_reply = reply
            if (
                isinstance(reply, list)
                and len(reply) == 2
                and isinstance(reply[1], list)
                and not reply[1]
            ):
                return
        except (ConnectionError, OSError, RespError, TimeoutError):
            pass
        time.sleep(0.05)
    raise TimeoutError(f"master still lists detached benchmark replicas: {last_reply!r}")


def detach_replicas_from_master(replicas: Sequence[DragonflyProcess]) -> None:
    """Stop every benchmark replication link before terminating its replica process."""

    errors: List[Exception] = []
    errors_lock = threading.Lock()

    def detach(replica: DragonflyProcess) -> None:
        try:
            if command(replica.port, "REPLICAOF", "NO", "ONE", timeout=30) != "OK":
                raise RuntimeError(f"REPLICAOF NO ONE failed for {replica.role}")
        except Exception as error:
            with errors_lock:
                errors.append(error)

    threads = [
        threading.Thread(target=detach, args=(replica,), name=f"detach-replica-{replica.port}")
        for replica in replicas
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=35)
        if thread.is_alive():
            raise TimeoutError(f"{thread.name} did not detach from its master")
    if errors:
        raise RuntimeError("failed to detach benchmark replicas from the master") from errors[0]


def verify_replicas(
    master_port: int, replica_ports: Sequence[int], sample_keys: Sequence[str], marker: str
) -> None:
    if command(master_port, "SET", marker, "ok") != "OK":
        raise RuntimeError("failed to write post-sync marker")

    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        try:
            if all(command(port, "GET", marker, timeout=2) == b"ok" for port in replica_ports):
                break
        except (ConnectionError, OSError, RespError, TimeoutError):
            pass
        time.sleep(0.05)
    else:
        raise TimeoutError("post-sync marker did not reach every replica")

    master_size = command(master_port, "DBSIZE")
    expected_values = {key: command(master_port, "GET", key) for key in sample_keys}
    for port in replica_ports:
        if command(port, "DBSIZE") != master_size:
            raise AssertionError(f"DBSIZE mismatch on replica port {port}")
        for key, expected in expected_values.items():
            if command(port, "GET", key) != expected:
                raise AssertionError(f"sample key {key} differs on replica port {port}")


@dataclasses.dataclass
class CaseResult:
    replicas: int
    fanout: bool
    full_sync_seconds: float
    load_operations: int
    load_breakdown: Dict[str, int]

    def as_dict(self) -> Dict[str, object]:
        return {
            "replicas": self.replicas,
            "fanout": self.fanout,
            "full_sync_seconds": round(self.full_sync_seconds, 3),
            "load_operations": self.load_operations,
            "load_breakdown": dict(sorted(self.load_breakdown.items())),
        }


class FullSyncFanoutBenchmark:
    def __init__(self, binary: Path, data_size: int, run_dir: Path) -> None:
        self._binary = binary
        self._data_size = data_size
        self._run_dir = run_dir
        self._results: List[CaseResult] = []

    @property
    def results(self) -> Sequence[CaseResult]:
        return self._results

    def run(self) -> None:
        for fanout_enabled in (False, True):
            self._run_mode(fanout_enabled)

    def _run_mode(self, fanout_enabled: bool) -> None:
        mode_name = "with_fanout" if fanout_enabled else "without_fanout"
        mode_dir = self._run_dir / mode_name
        mode_dir.mkdir()
        port_plan = find_free_port_plan()
        master: Optional[DragonflyProcess] = None
        workload: Optional[RandomCommandLoad] = None
        failed = False

        try:
            print(f"\nStarting {mode_name.replace('_', ' ')} master...", flush=True)
            master = start_dragonfly(
                self._binary,
                mode_dir,
                "master",
                port_plan.data_port(0),
                port_plan.admin_port(0),
                fanout_enabled,
            )
            _, sample_keys = populate_master(master.port, self._data_size)
            seed_load_keyspaces(master.port)

            workload = RandomCommandLoad(master.port)
            workload.start()
            for replica_count in range(1, MAX_REPLICAS + 1):
                self._run_case(
                    master,
                    port_plan,
                    mode_dir,
                    fanout_enabled,
                    replica_count,
                    sample_keys,
                    workload,
                )
        except BaseException:
            failed = True
            raise
        finally:
            try:
                if workload is not None:
                    workload.stop()
            except Exception:
                if not failed:
                    raise
            finally:
                if master is not None:
                    master.stop()

    def _run_case(
        self,
        master: DragonflyProcess,
        port_plan: PortPlan,
        mode_dir: Path,
        fanout_enabled: bool,
        replica_count: int,
        sample_keys: Sequence[str],
        workload: RandomCommandLoad,
    ) -> None:
        case_dir = mode_dir / f"replicas_{replica_count}"
        case_dir.mkdir()
        replicas: List[DragonflyProcess] = []
        replica_ports: List[int] = []
        try:
            print(
                f"  {replica_count:2d} replica(s), {'fanout' if fanout_enabled else 'no fanout'}...",
                end=" ",
                flush=True,
            )
            for index in range(1, replica_count + 1):
                replica = start_dragonfly(
                    self._binary,
                    case_dir,
                    f"replica_{index}",
                    port_plan.data_port(index),
                    port_plan.admin_port(index),
                    fanout_enabled=False,
                )
                replicas.append(replica)
                replica_ports.append(replica.port)

            master_log_offset = master.log_path.stat().st_size
            before = workload.snapshot()
            started = time.monotonic()
            start_replication(replica_ports, master.port)
            wait_for_replicas(master, replica_ports, replicas)
            elapsed = time.monotonic() - started
            after = workload.snapshot()
            workload.raise_if_failed()

            if fanout_enabled:
                expected_batch = f"Started full-sync fanout batch with {replica_count} replica(s)"
                new_master_log = master.log_path.read_text(errors="replace")[master_log_offset:]
                if expected_batch not in new_master_log:
                    raise AssertionError(
                        f"master did not form the expected batch: {expected_batch}"
                    )

            marker = f"fanout:benchmark:marker:{int(fanout_enabled)}:{replica_count}"
            verify_replicas(master.port, replica_ports, sample_keys, marker)

            load_breakdown = Counter(after)
            load_breakdown.subtract(before)
            result = CaseResult(
                replicas=replica_count,
                fanout=fanout_enabled,
                full_sync_seconds=elapsed,
                load_operations=sum(load_breakdown.values()),
                load_breakdown={
                    command_name: load_breakdown[command_name] for command_name in LOAD_COMMANDS
                },
            )
            self._results.append(result)
            print(f"{elapsed:.2f} s; {result.load_operations:,} random commands", flush=True)
        finally:
            try:
                if replicas and master.process.poll() is None:
                    detach_replicas_from_master(replicas)
                    wait_for_master_without_replicas(master.port)
            finally:
                for replica in reversed(replicas):
                    replica.stop()


def render_markdown(data_size: int, proactor_backend: str, results: Sequence[CaseResult]) -> str:
    by_case = {(result.replicas, result.fanout): result for result in results}
    lines = [
        "## Full-sync fanout benchmark",
        "",
        f"Configuration: requested data size {format_bytes(data_size)}, one master, 1–{MAX_REPLICAS} replicas, "
        f"{PROACTOR_THREADS} proactors per Dragonfly process, {LOAD_THREADS} random-command client threads, "
        f"a {FANOUT_DELAY_SECONDS}-second fanout collection window, and {proactor_backend}.",
        "",
        "| Replicas | Without fanout | With fanout | Result | Random-command ops: without / with |",
        "| ---: | ---: | ---: | --- | ---: |",
    ]
    for replica_count in range(1, MAX_REPLICAS + 1):
        without = by_case[(replica_count, False)]
        with_fanout = by_case[(replica_count, True)]
        percentage = (
            without.full_sync_seconds - with_fanout.full_sync_seconds
        ) / without.full_sync_seconds
        if percentage >= 0:
            result = f"{percentage * 100:.1f}% faster ({without.full_sync_seconds / with_fanout.full_sync_seconds:.2f}×)"
        else:
            result = f"{-percentage * 100:.1f}% slower"
        lines.append(
            f"| {replica_count} | {without.full_sync_seconds:.2f} s | {with_fanout.full_sync_seconds:.2f} s "
            f"| {result} | {without.load_operations:,} / {with_fanout.load_operations:,} |"
        )
    return "\n".join(lines) + "\n"


def write_results(
    run_dir: Path, data_size: int, proactor_backend: str, results: Sequence[CaseResult]
) -> None:
    payload = {
        "requested_data_bytes": data_size,
        "proactor_threads_per_instance": PROACTOR_THREADS,
        "load_threads": LOAD_THREADS,
        "fanout_delay_seconds": FANOUT_DELAY_SECONDS,
        "proactor_backend": proactor_backend,
        "results": [result.as_dict() for result in results],
    }
    (run_dir / "results.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    (run_dir / "results.md").write_text(render_markdown(data_size, proactor_backend, results))


def find_binary() -> Path:
    repository_root = Path(__file__).resolve().parents[1]
    binary = Path(os.environ.get("DRAGONFLY_BIN", repository_root / "build-opt" / "dragonfly"))
    if not binary.is_file():
        raise FileNotFoundError(
            f"Dragonfly binary not found at {binary}. Build it with:\n"
            "  ./helio/blaze.sh -release\n"
            "  ninja -C build-opt dragonfly\n"
            "or set DRAGONFLY_BIN to the binary path."
        )
    if not os.access(binary, os.X_OK):
        raise PermissionError(f"Dragonfly binary is not executable: {binary}")
    return binary.resolve()


def create_run_directory() -> Path:
    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    run_dir = Path.cwd() / f"full-sync-fanout-benchmark-{timestamp}"
    suffix = 1
    while run_dir.exists():
        run_dir = Path.cwd() / f"full-sync-fanout-benchmark-{timestamp}-{suffix}"
        suffix += 1
    run_dir.mkdir()
    return run_dir


def print_hardware_guidance(data_size: int, proactor_backend: str) -> None:
    estimated_memory = data_size * (MAX_REPLICAS + 2)
    available_memory = available_memory_bytes()
    print(f"Requested data size: {format_bytes(data_size)}")
    print(
        f"Topology: 1 master, replica counts 1–{MAX_REPLICAS}, {PROACTOR_THREADS} proactors per "
        f"Dragonfly process, {LOAD_THREADS} random-command threads"
    )
    print(f"Proactor backend: {proactor_backend}")
    print(f"Estimated memory requirement: at least {format_bytes(estimated_memory)}")
    if available_memory is not None:
        print(f"Available memory: {format_bytes(available_memory)}")
        if available_memory < estimated_memory:
            print("WARNING: available memory is below the estimate; the benchmark may OOM.")
    logical_cores = os.cpu_count()
    if logical_cores is not None and logical_cores < PROACTOR_THREADS * (MAX_REPLICAS + 1):
        print(
            f"WARNING: {logical_cores} logical CPU(s) for up to "
            f"{PROACTOR_THREADS * (MAX_REPLICAS + 1)} Dragonfly proactors; results will be oversubscribed."
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare full-sync fanout for one master and one to ten replicas.",
        epilog="Example: ./tools/full_sync_fanout_benchmark.py 4GiB",
    )
    parser.add_argument("data_size", type=parse_size, help="requested data size, for example 4GiB")
    arguments = parser.parse_args()

    try:
        binary = find_binary()
        proactor_backend = describe_io_uring_memlock()
        run_dir = create_run_directory()
        print_hardware_guidance(arguments.data_size, proactor_backend)
        print(f"Results and logs: {run_dir}")
        benchmark = FullSyncFanoutBenchmark(binary, arguments.data_size, run_dir)
        benchmark.run()
        write_results(run_dir, arguments.data_size, proactor_backend, benchmark.results)
        print(
            "\n" + render_markdown(arguments.data_size, proactor_backend, benchmark.results), end=""
        )
        print(f"\nSaved Markdown and JSON results in: {run_dir}")
        return 0
    except KeyboardInterrupt:
        print(
            "\nInterrupted; stopped benchmark processes. Partial logs are retained.",
            file=sys.stderr,
        )
        return 130
    except BenchmarkSetupError as error:
        print(f"\nBenchmark cannot start: {error}", file=sys.stderr)
        return 2
    except Exception as error:
        print(f"\nBenchmark failed: {error}", file=sys.stderr)
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
