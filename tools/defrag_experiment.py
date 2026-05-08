#!/usr/bin/env python3
"""Run old/new defrag experiments from a clean Dragonfly process each time."""

import argparse
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def ping(host: str, port: int, timeout_s: float = 0.5) -> bool:
    request = b"*1\r\n$4\r\nPING\r\n"
    try:
        with socket.create_connection((host, port), timeout=timeout_s) as sock:
            sock.sendall(request)
            return sock.recv(16).startswith(b"+PONG")
    except OSError:
        return False


def wait_for_dragonfly(proc: subprocess.Popen, host: str, port: int, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"dragonfly exited during startup with code {proc.returncode}")
        if ping(host, port):
            return
        time.sleep(0.1)
    raise TimeoutError(f"dragonfly did not respond to PING on {host}:{port}")


def stop_process(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return

    proc.terminate()
    try:
        proc.wait(timeout=10)
        return
    except subprocess.TimeoutExpired:
        pass

    proc.kill()
    proc.wait(timeout=10)


def run_checked(cmd: list[str], *, cwd: Path) -> None:
    print(f"+ {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, cwd=cwd, check=True)


def dragonfly_cmd(args: argparse.Namespace, data_dir: Path, experimental_defrag: bool) -> list[str]:
    return [
        str(args.dragonfly),
        "--proactor_threads=1",
        "--alsologtostderr",
        f"--dir={data_dir}",
        f"--bind={args.bind}",
        f"--port={args.port}",
        f"--experimental_defrag={str(experimental_defrag).lower()}",
    ]


def run_one(label: str, experimental_defrag: bool, args: argparse.Namespace) -> None:
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    data_dir = Path(args.dfly_dir_base).with_name(f"{Path(args.dfly_dir_base).name}-{label}")
    if data_dir.exists():
        shutil.rmtree(data_dir)
    data_dir.mkdir(parents=True)

    log_path = out_dir / f"{label}.dragonfly.log"
    run_path = out_dir / f"{label}.jsonl"

    print(f"\n=== {label} experimental_defrag={experimental_defrag} ===", flush=True)
    print(f"dragonfly log: {log_path}", flush=True)
    with open(log_path, "w") as log_file:
        proc = subprocess.Popen(
            dragonfly_cmd(args, data_dir, experimental_defrag),
            cwd=REPO_ROOT,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
        try:
            wait_for_dragonfly(proc, args.host, args.port, args.startup_timeout_s)

            run_checked(
                [
                    sys.executable,
                    "tools/defrag_baseline.py",
                    "--workload",
                    args.workload,
                    "--mul",
                    str(args.mul),
                    "--server",
                    args.host,
                    "--port",
                    str(args.port),
                    "--quiet",
                ],
                cwd=REPO_ROOT,
            )

            run_checked(
                [
                    sys.executable,
                    "tools/defrag_drive.py",
                    "--cycles",
                    str(args.cycles),
                    "--target-waste",
                    str(args.target_waste),
                    "--host",
                    args.host,
                    "--port",
                    str(args.port),
                    "--output",
                    str(run_path),
                    label,
                ],
                cwd=REPO_ROOT,
            )
        finally:
            stop_process(proc)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--dragonfly", type=Path, default=Path("build-opt/dragonfly"))
    parser.add_argument("--host", default="127.0.0.1", help="host used by scripts to connect")
    parser.add_argument("--bind", default="0.0.0.0", help="Dragonfly bind address")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--workload", choices=("uniform", "wide"), default="wide")
    parser.add_argument("--mul", type=float, default=40.0)
    parser.add_argument("--cycles", type=int, default=500)
    parser.add_argument("--target-waste", type=float, default=5.0)
    parser.add_argument("--out-dir", default="runs")
    parser.add_argument("--dfly-dir-base", default="/tmp/dfly-defrag")
    parser.add_argument("--startup-timeout-s", type=float, default=30.0)
    args = parser.parse_args()

    run_one("old", False, args)
    run_one("new", True, args)

    compare_out = Path(args.out_dir) / "compare.png"
    run_checked(
        [
            sys.executable,
            "tools/defrag_compare.py",
            str(Path(args.out_dir) / "old.jsonl"),
            str(Path(args.out_dir) / "new.jsonl"),
            "--out",
            str(compare_out),
        ],
        cwd=REPO_ROOT,
    )
    print(f"\ncomparison plot: {compare_out}")


if __name__ == "__main__":
    main()
