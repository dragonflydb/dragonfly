#!/usr/bin/env python3
"""Run four pinned Dragonfly nodes and preserve reproducible migration measurements.

Dependencies: redis, psutil, hdrhistogram; taskset and the bundled Go cluster client.
All server and load-generator processes are owned, tracked and reaped by this runner.
"""

import argparse
import binascii
import concurrent.futures
import hashlib
import itertools
import json
import os
from pathlib import Path
import shlex
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time
import traceback
import urllib.request

import psutil
import redis
from hdrh.histogram import HdrHistogram


NAMES = "ABCD"
OPTIONS = {
    "migration_buckets_cpu_budget": ["0.2", "0.35"],
    "migration_buckets_serialization_threshold": ["10", "25"],
    "migration_buckets_sleep_usec": ["500", "250"],
    "slot_migration_throttle_us": ["0", "20"],
    "snapshot_egress_limit_bytes": ["0", "100mb"],
    "replication_stream_output_limit": ["1048576", "4194304"],
}


class MigrationFailure(RuntimeError):
    """A measured server/client failure, retained as an ineligible configuration."""


def write_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def slot(key):
    encoded = key.encode()
    left = encoded.find(b"{")
    if left >= 0:
        right = encoded.find(b"}", left + 1)
        if right > left + 1:
            encoded = encoded[left + 1 : right]
    return binascii.crc_hqx(encoded, 0) % 16384


def resident_tag(index):
    for attempt in itertools.count():
        tag = f"resident{NAMES[index]}{attempt}"
        if index * 4096 + 2048 <= slot(tag) <= index * 4096 + 4095:
            return tag


def parameters(number):
    return {key: values[(number >> bit) & 1] for bit, (key, values) in enumerate(OPTIONS.items())}


class Audit:
    def __init__(self, root):
        self.path = root / "commands_audit.log"
        self.lock = threading.Lock()

    def log(self, command):
        with self.lock, self.path.open("a") as stream:
            stream.write(f"# {time.time():.6f}\n{command}\n")

    def command(self, argv):
        self.log(shlex.join([str(value) for value in argv]))


class Cluster:
    def __init__(self, args, run_dir, seed_dir, settings, audit):
        self.args = args
        self.run_dir = run_dir
        self.seed_dir = seed_dir
        self.settings = settings
        self.audit = audit
        self.processes = []
        self.logs = []
        self.admins = []
        self.readers = []
        self.ids = []
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)

    def rpc(self, index, *command):
        port = self.args.admin_port + index
        self.audit.command(["redis-cli", "-p", port, *command])
        return self.admins[index].execute_command(*command)

    def parallel(self, function):
        futures = [self.pool.submit(function, index) for index in range(4)]
        return [future.result() for future in futures]

    def start(self):
        self.run_dir.mkdir(parents=True, exist_ok=True)
        for index in range(4):
            data_dir = self.seed_dir / NAMES[index]
            data_dir.mkdir(parents=True, exist_ok=True)
            for port in (self.args.port + index, self.args.admin_port + index):
                with socket.socket() as probe:
                    probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    probe.bind(("127.0.0.1", port))
            argv = [
                "taskset",
                "-c",
                str(self.args.server_cpus[index]),
                str(self.args.binary),
                "--bind=127.0.0.1",
                f"--port={self.args.port + index}",
                f"--admin_port={self.args.admin_port + index}",
                "--cluster_mode=yes",
                "--proactor_threads=1",
                "--num_shards=1",
                "--maxmemory=6gb",
                f"--dir={data_dir}",
                "--dbfilename=initial",
                "--snapshot_cron=",
                "--version_check=false",
                "--logtostderr",
            ] + [f"--{key}={value}" for key, value in self.settings.items()]
            self.audit.command(argv)
            stream = (self.run_dir / f"server-{NAMES[index]}.log").open("w")
            self.logs.append(stream)
            self.processes.append(subprocess.Popen(argv, stdout=stream, stderr=stream))
            self.admins.append(
                redis.Redis(
                    host="127.0.0.1",
                    port=self.args.admin_port + index,
                    socket_timeout=120,
                    decode_responses=True,
                )
            )
            self.readers.append(
                redis.Redis(host="127.0.0.1", port=self.args.port + index, socket_timeout=20)
            )
        deadline = time.monotonic() + 180
        ready = [False] * 4
        while not all(ready):
            self.guard()
            for index, process in enumerate(self.processes):
                if ready[index]:
                    continue
                try:
                    self.rpc(index, "PING")
                    if not self.admins[index].info("persistence").get("loading", 0):
                        ready[index] = True
                except (redis.ConnectionError, redis.ResponseError, redis.TimeoutError):
                    pass
                if process.poll() is not None:
                    raise RuntimeError(f"Node {NAMES[index]} exited; see its server log")
            if time.monotonic() > deadline:
                raise TimeoutError("Server readiness timed out")
            time.sleep(0.1)
        self.ids = self.parallel(lambda index: self.rpc(index, "CLUSTER", "MYID"))
        identities = []
        for index, process in enumerate(self.processes):
            threads = []
            for task in Path(f"/proc/{process.pid}/task").iterdir():
                affinity = psutil.Process(int(task.name)).cpu_affinity()
                if affinity != [self.args.server_cpus[index]]:
                    raise RuntimeError(f"Wrong thread affinity on node {NAMES[index]}: {affinity}")
                threads.append(
                    {
                        "tid": int(task.name),
                        "cpu_affinity": affinity,
                        "name": (task / "comm").read_text().strip(),
                    }
                )
            info = self.admins[index].info("server")
            if "1.40.8" not in str(info.get("dragonfly_version", "")):
                raise RuntimeError(f"Unexpected running version: {info}")
            identities.append(
                {
                    "node": NAMES[index],
                    "pid": process.pid,
                    "threads": threads,
                    "server_info": info,
                    "cmd": psutil.Process(process.pid).cmdline(),
                }
            )
        write_json(self.run_dir / "processes.json", identities)
        self.configure("initial")

    def config(self, phase, case=None):
        pairs = [(0, 1), (1, 2), (2, 3), (3, 0)] if case == "ring" else [(0, 3), (1, 3), (2, 3)]
        config = []
        for index in range(4):
            ranges = [(4096 * index, 4096 * index + 4095)]
            migrations = []
            if phase == "opening":
                for source, destination in pairs:
                    if source == index:
                        migrations.append(
                            {
                                "slot_ranges": [
                                    {"start": source * 4096, "end": source * 4096 + 2047}
                                ],
                                "node_id": self.ids[destination],
                                "ip": "127.0.0.1",
                                "port": self.args.admin_port + destination,
                            }
                        )
            elif phase == "closing":
                if any(source == index for source, _ in pairs):
                    ranges = [(4096 * index + 2048, 4096 * index + 4095)]
                ranges += [
                    (4096 * source, 4096 * source + 2047)
                    for source, destination in pairs
                    if destination == index
                ]
            config.append(
                {
                    "slot_ranges": [{"start": start, "end": end} for start, end in ranges],
                    "master": {
                        "id": self.ids[index],
                        "ip": "127.0.0.1",
                        "port": self.args.port + index,
                    },
                    "replicas": [],
                    "migrations": migrations,
                }
            )
        return config

    def configure(self, phase, case=None):
        config = self.config(phase, case)
        write_json(self.run_dir / f"config-{phase}.json", config)
        encoded = json.dumps(config, separators=(",", ":"))
        started = time.time()
        responses = self.parallel(lambda index: self.rpc(index, "DFLYCLUSTER", "CONFIG", encoded))
        if responses != ["OK"] * 4:
            raise RuntimeError(f"Config rejected: {responses}")
        return started, time.time()

    def guard(self):
        if psutil.virtual_memory().available < 2 * 1024**3:
            raise RuntimeError("Host available memory fell below 2 GiB")
        for process in self.processes:
            if process.poll() is not None:
                raise RuntimeError(f"Owned server PID {process.pid} exited")

    def seed(self):
        def fill(index):
            self.rpc(
                index,
                "DEBUG",
                "POPULATE",
                self.args.keys,
                f"bulk{NAMES[index]}",
                self.args.value_size,
                "SLOTS",
                index * 4096,
                index * 4096 + 2047,
            )
            self.rpc(
                index,
                "DEBUG",
                "POPULATE",
                self.args.probe_keys,
                f"probe:{{{resident_tag(index)}}}",
                self.args.value_size,
            )
            if self.rpc(index, "DBSIZE") != self.args.keys + self.args.probe_keys:
                raise RuntimeError("Incorrect prefill key count")
            self.rpc(index, "SAVE", "DF", "initial")

        futures = [self.pool.submit(fill, index) for index in range(4)]
        while not all(future.done() for future in futures):
            self.guard()
            time.sleep(0.5)
        for future in futures:
            future.result()
        write_json(
            self.seed_dir / "dataset.json",
            {
                "keys_per_job": self.args.keys,
                "value_size": self.args.value_size,
                "probe_keys": self.args.probe_keys,
                "resident_tags": [resident_tag(index) for index in range(4)],
            },
        )

    def close(self):
        for index, process in enumerate(self.processes):
            if process.poll() is None:
                try:
                    self.rpc(index, "SHUTDOWN", "NOSAVE", "FORCE")
                except (redis.ConnectionError, redis.TimeoutError):
                    pass
        for process in self.processes:
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.audit.command(["kill", "-TERM", process.pid])
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.audit.command(["kill", "-KILL", process.pid])
                    process.kill()
                    process.wait(timeout=5)
        for index in range(len(self.processes)):
            for port in (self.args.port + index, self.args.admin_port + index):
                with socket.socket() as probe:
                    probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    probe.bind(("127.0.0.1", port))
        for client in self.admins + self.readers:
            client.close()
        for stream in self.logs:
            stream.close()
        self.pool.shutdown(wait=True)


class Load:
    def __init__(self, args, run_dir, rates, audit):
        self.args = args
        self.run_dir = run_dir
        self.rates = rates
        self.audit = audit
        self.processes = []
        self.logs = []

    def start(self):
        for index in range(4):
            stem = self.run_dir / f"client-{NAMES[index]}"
            argv = [
                "taskset",
                "-c",
                str(self.args.client_cpus[index]),
                str(self.args.client_binary),
                f"--seed=127.0.0.1:{self.args.port}",
                f"--prefix=probe:{{{resident_tag(index)}}}:",
                f"--keys={self.args.probe_keys}",
                f"--value-size={self.args.value_size}",
                f"--rate={self.rates[index] * 10}",
                "--connections=10",
                f"--control-port={self.args.control_port + index}",
                f"--output={stem}.json",
                f"--intervals={stem}-intervals.jsonl",
            ]
            self.audit.command(argv)
            stream = Path(f"{stem}.log").open("w")
            self.logs.append(stream)
            self.processes.append(subprocess.Popen(argv, stdout=stream, stderr=stream))
        deadline = time.monotonic() + 20
        while not all(
            "READY" in Path(stream.name).read_text(errors="replace") for stream in self.logs
        ):
            self.guard()
            if time.monotonic() > deadline:
                raise TimeoutError("Cluster GET client startup timed out")
            time.sleep(0.1)
        identities = []
        for index, process in enumerate(self.processes):
            for thread in psutil.Process(process.pid).threads():
                if psutil.Process(thread.id).cpu_affinity() != [self.args.client_cpus[index]]:
                    raise RuntimeError("Client thread affinity mismatch")
            identities.append(
                {
                    "node": NAMES[index],
                    "pid": process.pid,
                    "cpu_affinity": psutil.Process(process.pid).cpu_affinity(),
                    "rate_per_connection": self.rates[index],
                    "clients": 10,
                }
            )
        write_json(self.run_dir / "clients.json", identities)

    def phase(self, name):
        def set_phase(index):
            url = f"http://127.0.0.1:{self.args.control_port + index}/phase?name={name}"
            self.audit.command(["curl", "--fail", url])
            with urllib.request.urlopen(url, timeout=10) as response:
                return json.load(response)

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            return list(pool.map(set_phase, range(4)))

    def guard(self):
        for process in self.processes:
            if process.poll() is not None:
                raise RuntimeError(f"GET client PID {process.pid} exited before run ended")

    def close(self):
        for process in self.processes:
            if process.poll() is None:
                self.audit.command(["kill", "-INT", process.pid])
                process.send_signal(signal.SIGINT)
        for process in self.processes:
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.audit.command(["kill", "-TERM", process.pid])
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.audit.command(["kill", "-KILL", process.pid])
                    process.kill()
                    process.wait(timeout=5)
        for stream in self.logs:
            stream.close()


class Collector:
    def __init__(self, cluster, load):
        self.cluster = cluster
        self.load = load
        self.samples = []
        self.stop_event = threading.Event()
        self.error = None
        self.thread = threading.Thread(target=self.run, name="metrics")

    def start(self):
        self.thread.start()

    def run(self):
        previous = {}
        try:
            with (self.load.run_dir / "metrics.jsonl").open("w") as stream:
                while not self.stop_event.is_set():
                    self.cluster.guard()
                    self.load.guard()
                    now = time.time()
                    rows = []
                    for index, process in enumerate(self.cluster.processes):
                        server = psutil.Process(process.pid)
                        client = psutil.Process(self.load.processes[index].pid)
                        cpu = sum(server.cpu_times()[:2])
                        client_cpu = sum(client.cpu_times()[:2])
                        self.cluster.audit.command(
                            ["redis-cli", "-p", self.cluster.args.admin_port + index, "INFO"]
                        )
                        info = self.cluster.admins[index].info()
                        row = {
                            "node": NAMES[index],
                            "cpu_seconds": cpu,
                            "client_cpu_seconds": client_cpu,
                            "rss": server.memory_info().rss,
                            "used_memory": info.get("used_memory"),
                            "dbsize": info.get("db0", {}).get("keys", 0),
                            "evicted_keys": info.get("evicted_keys", 0),
                            "hard_evictions": info.get("hard_evictions", 0),
                            "expired_keys": info.get("expired_keys", 0),
                            "net_input_bytes": info.get("total_net_input_bytes", 0),
                            "net_output_bytes": info.get("total_net_output_bytes", 0),
                            "cpu_affinity": server.cpu_affinity(),
                            "snapshot_saving": info.get("rdb_bgsave_in_progress", 0),
                        }
                        if index in previous:
                            old_time, old_cpu, old_client_cpu = previous[index]
                            row["cpu_percent"] = 100 * (cpu - old_cpu) / (now - old_time)
                            row["client_cpu_percent"] = (
                                100 * (client_cpu - old_client_cpu) / (now - old_time)
                            )
                        previous[index] = (now, cpu, client_cpu)
                        if row["cpu_affinity"] != [self.cluster.args.server_cpus[index]]:
                            raise RuntimeError("Server affinity changed during benchmark")
                        if row["evicted_keys"] or row["hard_evictions"] or row["expired_keys"]:
                            raise RuntimeError("Unexpected eviction or expiry")
                        rows.append(row)
                    host = psutil.virtual_memory()
                    sample = {
                        "time": now,
                        "nodes": rows,
                        "host_available": host.available,
                        "host_used": host.used,
                        "host_swap_used": psutil.swap_memory().used,
                        "host_cpu_percent": psutil.cpu_percent(interval=None, percpu=True),
                    }
                    self.samples.append(sample)
                    stream.write(json.dumps(sample) + "\n")
                    stream.flush()
                    self.stop_event.wait(max(0, 1 - (time.time() - now)))
        except Exception as error:
            self.error = error

    def guard(self):
        if self.error:
            raise self.error
        self.cluster.guard()
        self.load.guard()

    def wait(self, duration):
        deadline = time.monotonic() + duration
        while time.monotonic() < deadline:
            self.guard()
            time.sleep(min(0.1, max(0, deadline - time.monotonic())))

    def close(self):
        self.stop_event.set()
        self.thread.join(timeout=30)
        if self.thread.is_alive():
            raise RuntimeError("Metrics collector did not stop")
        if self.error:
            raise self.error


def verify(cluster, case):
    result = {"dbsizes": [], "range_counts": [], "samples_per_original_dataset": 128}
    expected_value = b"x" * cluster.args.value_size
    for index in range(4):
        count = cluster.rpc(index, "DBSIZE")
        expected = cluster.args.keys + cluster.args.probe_keys
        if case == "fanin":
            expected = (4 * cluster.args.keys if index == 3 else 0) + cluster.args.probe_keys
        if count != expected:
            raise RuntimeError(f"Final node {NAMES[index]} has {count} keys; expected {expected}")
        result["dbsizes"].append(count)
        if cluster.rpc(index, "DFLYCLUSTER", "SLOT-MIGRATION-STATUS"):
            raise RuntimeError("Migration jobs remained after final config")
    for source in range(4):
        destination = (source + 1) % 4 if case == "ring" else 3
        if case == "fanin" and source == 3:
            destination = 3
        start, end = source * 4096, source * 4096 + 2047
        rows = cluster.rpc(destination, "DFLYCLUSTER", "GETSLOTINFO", "SLOTS", f"{start}-{end}")
        count = sum(row[2] for row in rows)
        if count != cluster.args.keys:
            raise RuntimeError(f"Transferred range {start}-{end} has {count} keys")
        result["range_counts"].append(
            {
                "source": NAMES[source],
                "destination": NAMES[destination],
                "keys": count,
                "range": [start, end],
            }
        )
        checked = 0
        for suffix in itertools.count():
            key = f"bulk{NAMES[source]}:{suffix}"
            if start <= slot(key) <= end:
                cluster.audit.command(
                    ["redis-cli", "-p", cluster.args.port + destination, "GET", key]
                )
                if cluster.readers[destination].get(key) != expected_value:
                    raise RuntimeError(f"Corrupt or missing migrated key {key}")
                checked += 1
                if checked == min(128, cluster.args.keys):
                    break
        key = f"probe:{{{resident_tag(source)}}}:0"
        if cluster.readers[source].get(key) != expected_value:
            raise RuntimeError("Resident GET key moved or was corrupted")
        ownership = cluster.readers[0].execute_command("CLUSTER", "SLOTS")
        owner_port = next(row[2][1] for row in ownership if row[0] <= start <= row[1])
        if owner_port != cluster.args.port + destination:
            raise RuntimeError("Final client-visible slot ownership is incorrect")
    result["ok"] = True
    return result


def latency_summary(run_dir, phases):
    """Validate and report per-phase merged HDR histograms and one-second peaks."""
    result = {}
    for name in NAMES:
        raw = json.loads((run_dir / f"client-{name}.json").read_text())
        intervals = []
        for line in (run_dir / f"client-{name}-intervals.jsonl").read_text().splitlines():
            row = json.loads(line)
            intervals.append(
                {
                    "start": row["start"],
                    "end": row["end"],
                    "count": row["stats"]["count"],
                    **{
                        field: row["stats"][field]
                        for field in ("p50_us", "p95_us", "p99_us", "max_us")
                    },
                }
            )
        write_json(run_dir / f"client-{name}-intervals.json", intervals)
        node = {}
        for phase in phases:
            stats = raw["phases"][phase]
            if stats["errors"] or stats["bad_values"]:
                raise MigrationFailure(f"GET error or corrupt value on {name}/{phase}: {stats}")
            if not stats["count"]:
                raise RuntimeError(f"No GET requests on {name}/{phase}")
            histogram = HdrHistogram(1, 60_000_000, 3)
            for bucket in stats["buckets"]:
                histogram.record_value(bucket["upper_us"], bucket["count"])
            if histogram.total_count != stats["count"]:
                raise RuntimeError("HDR sample count differs from completed GET count")
            for percentile in (95, 99):
                if histogram.get_value_at_percentile(percentile) != stats[f"p{percentile}_us"]:
                    raise RuntimeError("Independent HDR percentile validation failed")
            start, end = raw["phase_times"][phase]
            node[phase] = {
                "requests": stats["count"],
                "histogram_seconds": end - start,
                "qps": stats["count"] / (end - start),
                **{
                    field: stats[field]
                    for field in ("p50_us", "p95_us", "p99_us", "p999_us", "max_us")
                },
                "peak_interval_p99_us": max(
                    row["p99_us"] for row in intervals if row["end"] > start and row["start"] < end
                ),
                "errors": stats["errors"],
                "bad_values": stats["bad_values"],
            }
        result[name] = node
    return result


def cpu_summary(samples, phases):
    result = {}
    for index, name in enumerate(NAMES):
        node = {}
        for phase, (start, end) in phases.items():
            rows = [
                sample["nodes"][index]
                for sample in samples
                if start + 1 <= sample["time"] <= end and "cpu_percent" in sample["nodes"][index]
            ]
            node[phase] = {
                "cpu_mean": statistics.mean(row["cpu_percent"] for row in rows) if rows else None,
                "cpu_peak": max((row["cpu_percent"] for row in rows), default=None),
                "client_cpu_mean": (
                    statistics.mean(row["client_cpu_percent"] for row in rows) if rows else None
                ),
                "client_cpu_peak": max((row["client_cpu_percent"] for row in rows), default=None),
                "rss_peak": max((row["rss"] for row in rows), default=None),
            }
        result[name] = node
    return result


def run_one(args, root, case, number, repetition, stage, rates, audit):
    run_dir = root / "runs" / f"{stage}-{case}-{number:02d}-r{repetition}"
    if (run_dir / "result.json").exists():
        return json.loads((run_dir / "result.json").read_text())
    if (run_dir / "failure.json").exists():
        run_dir.rename(run_dir.with_name(f"{run_dir.name}-failed-{int(time.time())}"))
    run_dir.mkdir(parents=True, exist_ok=True)
    settings = parameters(number)
    cluster = Cluster(args, run_dir, root / "seed", settings, audit)
    load = Load(args, run_dir, rates, audit)
    collector = None
    try:
        print(f"START {stage} {case} config={number:02d} repeat={repetition}", flush=True)
        cluster.start()
        counts = cluster.parallel(lambda index: cluster.rpc(index, "DBSIZE"))
        if counts != [args.keys + args.probe_keys] * 4:
            raise RuntimeError(f"Initial snapshot data mismatch: {counts}")
        load.start()
        collector = Collector(cluster, load)
        collector.start()
        load.phase("before")
        before = time.time()
        collector.wait(args.window)
        load.phase("migration")
        opening_start, opening_end = cluster.configure("opening", case)
        pairs = [(0, 1), (1, 2), (2, 3), (3, 0)] if case == "ring" else [(0, 3), (1, 3), (2, 3)]
        completed = {}
        expected_rows = 2 * len(pairs)
        with (run_dir / "migration-status.jsonl").open("w") as stream:
            while True:
                collector.guard()
                statuses = cluster.parallel(
                    lambda index: cluster.rpc(index, "DFLYCLUSTER", "SLOT-MIGRATION-STATUS")
                )
                now = time.time()
                stream.write(json.dumps({"time": now, "statuses": statuses}) + "\n")
                stream.flush()
                flat = [(index, row) for index, rows in enumerate(statuses) for row in rows]
                for index, row in flat:
                    if row[4] != "0":
                        raise MigrationFailure(f"Migration failed on {NAMES[index]}: {row}")
                    if row[0] == "out" and row[2] == "FINISHED":
                        completed.setdefault(NAMES[index], now)
                if len(flat) == expected_rows and all(row[2] == "FINISHED" for _, row in flat):
                    finished = now
                    break
                if now - opening_start > args.timeout:
                    raise MigrationFailure("Migration batch did not finish within timeout")
                time.sleep(0.1)
        closing_start, closing_end = cluster.configure("closing", case)
        load.phase("after")
        collector.wait(args.window)
        after = time.time()
        load.phase("end")
        collector.close()
        collector = None
        load.close()
        phases = {
            "before": [before, opening_start],
            "migration": [opening_start, closing_end],
            "after": [closing_end, after],
        }
        write_json(run_dir / "phases.json", phases)
        correctness = verify(cluster, case)
        logical_bytes = args.keys * args.value_size * len(pairs)
        result = {
            "stage": stage,
            "case": case,
            "config": number,
            "repetition": repetition,
            "parameters": settings,
            "rates": rates,
            "phases": phases,
            "opening_publish_seconds": opening_end - opening_start,
            "closing_publish_seconds": closing_end - closing_start,
            "batch_seconds": finished - opening_start,
            "batch_including_closing_seconds": closing_end - opening_start,
            "logical_bytes": logical_bytes,
            "logical_mib_per_second": logical_bytes / 1024**2 / (finished - opening_start),
            "jobs": [
                {
                    "source": NAMES[source],
                    "destination": NAMES[destination],
                    "seconds": completed[NAMES[source]] - opening_start,
                }
                for source, destination in pairs
            ],
            "correctness": correctness,
            "latency": latency_summary(run_dir, phases),
            "cpu": cpu_summary(read_metrics(run_dir), phases),
            "path": str(run_dir),
        }
        write_json(run_dir / "result.json", result)
        print(
            f"DONE {stage} {case} config={number:02d} repeat={repetition} "
            f"batch={result['batch_seconds']:.3f}s "
            f"peak_p99_us={max(node['migration']['peak_interval_p99_us'] for node in result['latency'].values())}",
            flush=True,
        )
        return result
    except Exception as error:
        write_json(
            run_dir / "failure.json",
            {
                "error": str(error),
                "traceback": traceback.format_exc(),
                "case": case,
                "config": number,
                "stage": stage,
                "repetition": repetition,
                "parameters": settings,
                "time": time.time(),
                "measured_failure": isinstance(error, MigrationFailure),
            },
        )
        raise
    finally:
        try:
            if collector:
                collector.close()
        finally:
            try:
                load.close()
            finally:
                cluster.close()


def read_metrics(run_dir):
    return [json.loads(line) for line in (run_dir / "metrics.jsonl").read_text().splitlines()]


def calibrate(args, root, audit):
    calibration_path = root / "calibration.json"
    if calibration_path.exists():
        return json.loads(calibration_path.read_text())["rates_per_connection"]
    cluster = Cluster(args, root / "calibration-server", root / "seed", parameters(0), audit)
    rates = [5000] * 4
    history = []
    try:
        cluster.start()
        for iteration in range(10):
            run_dir = root / f"calibration-{iteration}"
            run_dir.mkdir(exist_ok=True)
            load = Load(args, run_dir, rates, audit)
            collector = None
            try:
                load.start()
                collector = Collector(cluster, load)
                collector.start()
                start = time.time()
                collector.wait(8)
                end = time.time()
                collector.close()
                samples = list(collector.samples)
                collector = None
                load.close()
                means = [
                    statistics.mean(
                        sample["nodes"][index]["cpu_percent"]
                        for sample in samples
                        if "cpu_percent" in sample["nodes"][index]
                    )
                    for index in range(4)
                ]
                history.append(
                    {
                        "iteration": iteration,
                        "rates": list(rates),
                        "cpu": means,
                        "start": start,
                        "end": end,
                    }
                )
                write_json(root / "calibration-history.json", history)
                print(
                    f"CALIBRATE iteration={iteration} rates={rates} "
                    f"CPU={[round(value, 1) for value in means]}",
                    flush=True,
                )
                if all(45 <= value <= 55 for value in means):
                    write_json(
                        calibration_path,
                        {"rates_per_connection": rates, "cpu_percent": means, "history": history},
                    )
                    return rates
                rates = [
                    max(100, round(rate * 50 / max(cpu, 1))) for rate, cpu in zip(rates, means)
                ]
            finally:
                if collector:
                    collector.close()
                load.close()
        raise RuntimeError("Could not calibrate all nodes to 45-55% CPU in ten iterations")
    finally:
        cluster.close()


def latency_ratio(result):
    return max(
        node[phase][percentile] / max(node["before"][percentile], 1)
        for node in result["latency"].values()
        for phase in ("migration", "after")
        for percentile in ("p95_us", "p99_us")
    )


def run_attempt(args, root, case, number, repetition, stage, rates, audit):
    """Keep configuration failures without stopping independent remaining tests."""
    run_dir = root / "runs" / f"{stage}-{case}-{number:02d}-r{repetition}"
    if (run_dir / "failure.json").exists():
        return None
    try:
        return run_one(args, root, case, number, repetition, stage, rates, audit)
    except MigrationFailure as error:
        print(f"FAILED {stage} {case} config={number:02d} repeat={repetition}: {error}", flush=True)
        return None


def execute(args):
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=True)
    audit = Audit(root)
    audit.command([sys.executable, *sys.argv])
    audit.log(
        "# Local equivalents: four independent PIDs/ports replace SSH single-server scripts; "
        "DEBUG POPULATE batches 32 replace network prefill; raw HDR/process time series "
        "replace dfly_bench-only charts and unavailable Docker monitoring. See report appendix."
    )
    version = subprocess.check_output([str(args.binary), "--version"], text=True)
    if "1.40.8" not in version:
        raise RuntimeError(f"Wrong binary: {version}")
    audit.command([str(args.binary), "--version"])
    environment = {
        "version": version,
        "binary": str(args.binary),
        "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
        "uname": list(os.uname()),
        "server_cpus": args.server_cpus,
        "client_cpus": args.client_cpus,
        "allowed_cpus": sorted(os.sched_getaffinity(0)),
        "memory": dict(psutil.virtual_memory()._asdict()),
        "client": "go-redis/v9 v9.12.1, HDR v1.1.2, Go 1.23.0",
        "client_binary": str(args.client_binary),
        "client_sha256": hashlib.sha256(args.client_binary.read_bytes()).hexdigest(),
        "dataset": {
            "keys_per_job": args.keys,
            "value_size": args.value_size,
            "probe_keys_per_node": args.probe_keys,
        },
        "created": time.time(),
    }
    write_json(root / "environment.json", environment)
    seed = root / "seed"
    if not (seed / "dataset.json").exists():
        print(
            f"PREFILL {args.keys} migrating keys per node, {args.value_size} bytes each", flush=True
        )
        cluster = Cluster(args, root / "prefill", seed, parameters(0), audit)
        try:
            cluster.start()
            cluster.seed()
        finally:
            cluster.close()
    else:
        dataset = json.loads((seed / "dataset.json").read_text())
        if (dataset["keys_per_job"], dataset["value_size"], dataset["probe_keys"]) != (
            args.keys,
            args.value_size,
            args.probe_keys,
        ):
            raise RuntimeError("Output directory contains a different dataset")
    if args.mode == "prepare":
        return
    rates = [5000] * 4 if args.mode == "pilot" else calibrate(args, root, audit)
    if args.mode == "calibrate":
        return
    if args.mode == "candidate":
        if not args.configs:
            raise ValueError("Candidate mode requires --configs")
        for repetition in range(1, args.repetitions + 1):
            for number in args.configs:
                for case in args.cases.split(","):
                    if case not in ("ring", "fanin"):
                        raise ValueError(f"Unknown case {case}")
                    run_attempt(args, root, case, number, repetition, args.stage, rates, audit)
        return
    if args.mode == "pilot":
        for case in ("ring", "fanin"):
            run_one(args, root, case, 0, 1, "pilot", rates, audit)
        return
    for repetition in range(1, 4):
        for case in ("ring", "fanin"):
            run_one(args, root, case, 0, repetition, "default", rates, audit)
    if args.mode == "baseline":
        return
    for number in range(64):
        for case in ("ring", "fanin"):
            run_attempt(args, root, case, number, 1, "grid", rates, audit)
    shortlist = {}
    for case in ("ring", "fanin"):
        rows = [
            json.loads(path.read_text())
            for path in (root / "runs").glob(f"grid-{case}-*/result.json")
        ]
        passing = [row for row in rows if latency_ratio(row) <= 1 + args.latency_tolerance / 100]
        candidates = sorted(passing or rows, key=lambda row: row["batch_seconds"])[:3]
        shortlist[case] = {
            "configs": [row["config"] for row in candidates],
            "passing_screen": bool(passing),
            "tolerance_percent": args.latency_tolerance,
        }
        for repetition in range(1, 4):
            for candidate in candidates:
                run_attempt(
                    args, root, case, candidate["config"], repetition, "finalist", rates, audit
                )
    write_json(root / "shortlist.json", shortlist)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--client-binary",
        type=Path,
        default=Path(__file__).resolve().parents[1]
        / "build-migration-results"
        / "slot-migration-client",
    )
    parser.add_argument(
        "--mode",
        choices=["prepare", "pilot", "calibrate", "baseline", "full", "candidate"],
        default="full",
    )
    parser.add_argument(
        "--configs", type=lambda value: [int(number) for number in value.split(",")]
    )
    parser.add_argument("--cases", default="ring,fanin")
    parser.add_argument("--stage", default="common")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--keys", type=int, default=1048576)
    parser.add_argument("--value-size", type=int, default=1024)
    parser.add_argument("--probe-keys", type=int, default=16384)
    parser.add_argument("--window", type=float, default=10)
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--port", type=int, default=6380)
    parser.add_argument("--admin-port", type=int, default=16380)
    parser.add_argument("--control-port", type=int, default=17380)
    parser.add_argument(
        "--server-cpus",
        type=lambda value: [int(cpu) for cpu in value.split(",")],
        default=[0, 1, 2, 3],
    )
    parser.add_argument(
        "--client-cpus",
        type=lambda value: [int(cpu) for cpu in value.split(",")],
        default=[4, 5, 6, 7],
    )
    parser.add_argument(
        "--latency-tolerance",
        type=float,
        default=0,
        help="Provisional screening tolerance; raw metrics remain available for another SLO",
    )
    args = parser.parse_args()
    args.binary = args.binary.resolve()
    args.client_binary = args.client_binary.resolve()
    if (
        len(args.server_cpus) != 4
        or len(args.client_cpus) != 4
        or len(set(args.server_cpus + args.client_cpus)) != 8
    ):
        parser.error("Specify four distinct server CPUs and four distinct client CPUs")
    allowed = os.sched_getaffinity(0)
    if not set(args.server_cpus + args.client_cpus) <= allowed:
        parser.error("Requested CPU affinity exceeds this process's allowed CPU set")
    os.sched_setaffinity(0, args.client_cpus)
    execute(args)


if __name__ == "__main__":
    main()
