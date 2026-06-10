#!/usr/bin/env python3
"""A/B benchmark two Dragonfly-compatible binaries with memtier PING.

Example:
  ./tools/benchmark/ab_ping_benchmark.py \
    --a build-opt/ok_backend \
    --b build-opt/ok_backend.main \
    --a-label ok_backend \
    --b-label ok_backend.main \
    --pairs 12 \
    --requests 20000000 \
    --warmup-requests 1000000 \
    --server-cpus 0-3 \
    --client-cpus 8-11 \
    --json-out /tmp/ok_backend_ab_ping.json

What it does:
  * Starts each binary on --server-cpus with taskset.
  * Runs memtier_benchmark on --client-cpus.
  * Randomizes A/B order inside each pair to reduce drift bias.
  * Reports per-binary confidence intervals and a paired t-test.
  * Optionally writes raw samples and summary stats to --json-out.

Dragonfly vs ok_backend example:
  ./tools/benchmark/ab_ping_benchmark.py \
    --a build-opt/dragonfly \
    --b build-opt/ok_backend \
    --a-label dragonfly \
    --b-label ok_backend \
    --a-server-arg=--dbfilename=

Dragonfly vs Valkey Docker example:
  ./tools/benchmark/ab_ping_benchmark.py \
    --a build-opt/dragonfly \
    --b-label valkey \
    --b-docker-image valkey/valkey \
    --a-server-arg=--dbfilename= \
    --b-server-arg=--io-threads \
    --b-server-arg=4

The default command benchmarks:
  memtier_benchmark -t 1 -c 1 --pipeline=100 --command=ping -d 2 -n 20000000
"""

import argparse
import json
import math
import random
import re
import socket
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path


PING_RE = re.compile(r"^Pings\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)")


@dataclass
class Sample:
    label: str
    pair: int
    order: int
    ops_sec: float
    avg_latency_ms: float
    p50_latency_ms: float
    p99_latency_ms: float
    p999_latency_ms: float


@dataclass
class ServerHandle:
    proc: subprocess.Popen | None = None
    docker_name: str | None = None
    log: object | None = None


def betacf(a: float, b: float, x: float) -> float:
    max_iter = 200
    eps = 3e-14
    fpmin = 1e-300
    qab = a + b
    qap = a + 1.0
    qam = a - 1.0
    c = 1.0
    d = 1.0 - qab * x / qap
    if abs(d) < fpmin:
        d = fpmin
    d = 1.0 / d
    h = d
    for m in range(1, max_iter + 1):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        if abs(d) < fpmin:
            d = fpmin
        c = 1.0 + aa / c
        if abs(c) < fpmin:
            c = fpmin
        d = 1.0 / d
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        if abs(d) < fpmin:
            d = fpmin
        c = 1.0 + aa / c
        if abs(c) < fpmin:
            c = fpmin
        d = 1.0 / d
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < eps:
            break
    return h


def betai(a: float, b: float, x: float) -> float:
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    bt = math.exp(math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b) +
                  a * math.log(x) + b * math.log1p(-x))
    if x < (a + 1.0) / (a + b + 2.0):
        return bt * betacf(a, b, x) / a
    return 1.0 - bt * betacf(b, a, 1.0 - x) / b


def student_t_cdf(t: float, df: float) -> float:
    if df <= 0:
        return math.nan
    x = df / (df + t * t)
    ib = betai(df / 2.0, 0.5, x)
    if t >= 0:
        return 1.0 - 0.5 * ib
    return 0.5 * ib


def student_t_ppf(p: float, df: float) -> float:
    if not 0.0 < p < 1.0:
        raise ValueError("p must be in (0, 1)")
    if p == 0.5:
        return 0.0
    sign = 1.0
    target = p
    if p < 0.5:
        sign = -1.0
        target = 1.0 - p
    lo, hi = 0.0, 1.0
    while student_t_cdf(hi, df) < target:
        hi *= 2.0
    for _ in range(80):
        mid = (lo + hi) / 2.0
        if student_t_cdf(mid, df) < target:
            lo = mid
        else:
            hi = mid
    return sign * (lo + hi) / 2.0


def mean_ci(values: list[float], confidence: float) -> tuple[float, float]:
    if len(values) < 2:
        return (math.nan, math.nan)
    mean = statistics.mean(values)
    stdev = statistics.stdev(values)
    crit = student_t_ppf((1.0 + confidence) / 2.0, len(values) - 1)
    half = crit * stdev / math.sqrt(len(values))
    return mean - half, mean + half


def two_sided_paired_t(diff: list[float]) -> tuple[float, float]:
    if len(diff) < 2:
        return math.nan, math.nan
    mean = statistics.mean(diff)
    stdev = statistics.stdev(diff)
    if stdev == 0:
        return math.inf if mean != 0 else 0.0, 0.0 if mean != 0 else 1.0
    t_stat = mean / (stdev / math.sqrt(len(diff)))
    p = 2.0 * (1.0 - student_t_cdf(abs(t_stat), len(diff) - 1))
    return t_stat, p


def wait_for_port(host: str, port: int, timeout_sec: float) -> None:
    deadline = time.monotonic() + timeout_sec
    last_error = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.2):
                return
        except OSError as e:
            last_error = e
            time.sleep(0.05)
    raise RuntimeError(f"port {host}:{port} did not open: {last_error}")


def ensure_port_free(host: str, port: int) -> None:
    try:
        with socket.create_connection((host, port), timeout=0.2):
            raise RuntimeError(f"port {host}:{port} is already accepting connections")
    except ConnectionRefusedError:
        return
    except OSError:
        return


def terminate(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def side_value(args: argparse.Namespace, side: str, name: str):
    return getattr(args, f"{side}_{name}")


def side_server_args(args: argparse.Namespace, side: str) -> list[str]:
    return args.server_arg + side_value(args, side, "server_arg")


def start_server(side: str, binary: Path, label: str, args: argparse.Namespace) -> ServerHandle:
    docker_image = side_value(args, side, "docker_image")
    if docker_image:
        name = f"ab-ping-{side}-{int(time.time() * 1000)}"
        subprocess.run(["docker", "rm", "-f", name], stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)
        cmd = [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            name,
            "--network=host",
            f"--cpuset-cpus={args.server_cpus}",
        ]
        cmd.extend(side_value(args, side, "docker_arg"))
        cmd.append(docker_image)
        cmd.extend(side_server_args(args, side))
        subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
        try:
            wait_for_port(args.host, args.port, args.startup_timeout)
        except Exception:
            subprocess.run(["docker", "rm", "-f", name], stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL)
            raise
        return ServerHandle(docker_name=name)

    cmd = [
        "taskset",
        "-c",
        args.server_cpus,
        str(binary),
        f"--proactor_threads={args.proactor_threads}",
        "--enable_resp_io_loop_v2=true",
        "--logtostderr",
        "--port",
        str(args.port),
    ]
    cmd.extend(side_server_args(args, side))
    log_path = Path(args.log_dir) / f"{label}.{int(time.time() * 1000)}.log"
    log = log_path.open("wb")
    proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    try:
        wait_for_port(args.host, args.port, args.startup_timeout)
    except Exception:
        terminate(proc)
        log.close()
        raise
    return ServerHandle(proc=proc, log=log)


def stop_server(handle: ServerHandle) -> None:
    if handle.docker_name:
        subprocess.run(["docker", "stop", handle.docker_name], stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)
    if handle.proc:
        terminate(handle.proc)
    if handle.log:
        handle.log.close()


def run_memtier(args: argparse.Namespace, requests: int) -> str:
    cmd = [
        "taskset",
        "-c",
        args.client_cpus,
        "memtier_benchmark",
        "-t",
        "1",
        "-c",
        "1",
        f"--pipeline={args.pipeline}",
        f"--command={args.command}",
        "-d",
        str(args.data_size),
        "-n",
        str(requests),
        "--hide-histogram",
        "-p",
        str(args.port),
        "--server",
        args.host,
    ]
    return subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)


def parse_ping_line(output: str) -> tuple[float, float, float, float, float]:
    for line in output.splitlines():
        match = PING_RE.match(line)
        if match:
            return tuple(float(x) for x in match.groups())
    raise RuntimeError(f"could not parse memtier Pings line from output:\n{output}")


def run_sample(side: str, label: str, binary: Path, pair: int, order: int,
               args: argparse.Namespace) -> Sample:
    ensure_port_free(args.host, args.port)
    handle = start_server(side, binary, label, args)
    try:
        if args.warmup_requests:
            run_memtier(args, args.warmup_requests)
        output = run_memtier(args, args.requests)
        ops, avg, p50, p99, p999 = parse_ping_line(output)
        print(f"{label:>12} pair={pair:02d} order={order} ops={ops:.2f} "
              f"avg_ms={avg:.5f} p99_ms={p99:.5f}", flush=True)
        return Sample(label, pair, order, ops, avg, p50, p99, p999)
    finally:
        stop_server(handle)
        time.sleep(args.cooldown_sec)


def summarize(label: str, samples: list[Sample], confidence: float) -> dict:
    ops = [s.ops_sec for s in samples]
    ci = mean_ci(ops, confidence)
    return {
        "label": label,
        "n": len(ops),
        "mean": statistics.mean(ops),
        "median": statistics.median(ops),
        "stdev": statistics.stdev(ops) if len(ops) > 1 else 0.0,
        "min": min(ops),
        "max": max(ops),
        "ci_low": ci[0],
        "ci_high": ci[1],
        "samples": ops,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--a", default="build-opt/ok_backend", type=Path)
    parser.add_argument("--b", default="build-opt/ok_backend.main", type=Path)
    parser.add_argument("--a-label", default="ok_backend")
    parser.add_argument("--b-label", default="ok_backend.main")
    parser.add_argument("--pairs", type=int, default=10)
    parser.add_argument("--requests", type=int, default=20_000_000)
    parser.add_argument("--warmup-requests", type=int, default=1_000_000)
    parser.add_argument("--pipeline", type=int, default=100)
    parser.add_argument("--command", default="ping")
    parser.add_argument("--data-size", type=int, default=2)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--server-cpus", default="0-3")
    parser.add_argument("--client-cpus", default="8-11")
    parser.add_argument("--proactor-threads", type=int, default=4)
    parser.add_argument("--startup-timeout", type=float, default=5.0)
    parser.add_argument("--cooldown-sec", type=float, default=0.5)
    parser.add_argument("--confidence", type=float, default=0.95)
    parser.add_argument("--seed", type=int, default=12345)
    parser.add_argument("--log-dir", default="/tmp/ab_ping_logs")
    parser.add_argument("--json-out", type=Path)
    parser.add_argument("--server-arg", action="append", default=[])
    parser.add_argument("--a-server-arg", action="append", default=[])
    parser.add_argument("--b-server-arg", action="append", default=[])
    parser.add_argument("--a-docker-image")
    parser.add_argument("--b-docker-image")
    parser.add_argument("--a-docker-arg", action="append", default=[])
    parser.add_argument("--b-docker-arg", action="append", default=[])
    args = parser.parse_args()

    if args.pairs < 2:
        raise SystemExit("--pairs must be at least 2")
    for side, binary in [("a", args.a), ("b", args.b)]:
        if not side_value(args, side, "docker_image") and not binary.exists():
            raise SystemExit(f"binary does not exist: {binary}")
    Path(args.log_dir).mkdir(parents=True, exist_ok=True)

    random.seed(args.seed)
    samples: list[Sample] = []
    print("A/B PING benchmark")
    print(f"A={args.a} ({args.a_label}) docker={args.a_docker_image or '-'}")
    print(f"B={args.b} ({args.b_label}) docker={args.b_docker_image or '-'}")
    print(f"pairs={args.pairs} requests={args.requests} warmup={args.warmup_requests}")
    print(f"server_cpus={args.server_cpus} client_cpus={args.client_cpus} port={args.port}")

    try:
        for pair in range(1, args.pairs + 1):
            order = [("a", args.a_label, args.a), ("b", args.b_label, args.b)]
            if random.randrange(2):
                order.reverse()
            for idx, (side, label, binary) in enumerate(order):
                samples.append(run_sample(side, label, binary, pair, idx, args))
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130

    by_label = {
        args.a_label: [s for s in samples if s.label == args.a_label],
        args.b_label: [s for s in samples if s.label == args.b_label],
    }
    a_summary = summarize(args.a_label, by_label[args.a_label], args.confidence)
    b_summary = summarize(args.b_label, by_label[args.b_label], args.confidence)

    pairs = []
    for pair in range(1, args.pairs + 1):
        a = next(s for s in samples if s.pair == pair and s.label == args.a_label)
        b = next(s for s in samples if s.pair == pair and s.label == args.b_label)
        pairs.append(b.ops_sec - a.ops_sec)
    diff_mean = statistics.mean(pairs)
    diff_ci = mean_ci(pairs, args.confidence)
    t_stat, p_value = two_sided_paired_t(pairs)
    rel = diff_mean / a_summary["mean"] * 100.0
    winner = args.b_label if diff_mean > 0 else args.a_label
    significant = diff_ci[0] > 0 or diff_ci[1] < 0

    print()
    print("Summary (ops/sec)")
    for s in [a_summary, b_summary]:
        print(f"{s['label']:>16}: n={s['n']} mean={s['mean']:.2f} "
              f"median={s['median']:.2f} stdev={s['stdev']:.2f} "
              f"{args.confidence:.0%}CI=[{s['ci_low']:.2f}, {s['ci_high']:.2f}] "
              f"min={s['min']:.2f} max={s['max']:.2f}")
    print()
    print(f"Paired difference ({args.b_label} - {args.a_label}):")
    print(f"  mean={diff_mean:.2f} ops/sec ({rel:+.2f}%)")
    print(f"  {args.confidence:.0%}CI=[{diff_ci[0]:.2f}, {diff_ci[1]:.2f}]")
    print(f"  paired t={t_stat:.4f}, two-sided p={p_value:.6g}")
    if significant:
        print(f"Decision: {winner} is faster at {args.confidence:.0%} confidence.")
    else:
        print(f"Decision: difference is not above noise at {args.confidence:.0%} confidence.")

    result = {
        "args": vars(args) | {"a": str(args.a), "b": str(args.b), "json_out": str(args.json_out)},
        "samples": [s.__dict__ for s in samples],
        "summary": {args.a_label: a_summary, args.b_label: b_summary},
        "paired_difference": {
            "label": f"{args.b_label} - {args.a_label}",
            "mean": diff_mean,
            "relative_percent": rel,
            "ci_low": diff_ci[0],
            "ci_high": diff_ci[1],
            "t_stat": t_stat,
            "p_value": p_value,
            "significant": significant,
            "winner": winner if significant else None,
        },
    }
    if args.json_out:
        args.json_out.write_text(json.dumps(result, indent=2) + "\n")
        print(f"Wrote JSON: {args.json_out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
