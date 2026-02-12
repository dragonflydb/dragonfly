#!/usr/bin/env python3
"""Replays a crash from AFL++ persistent mode RECORD files.

In persistent mode, a crash depends on accumulated server state from all
previous iterations. AFL_PERSISTENT_RECORD saves these as RECORD files.
This script replays them in order against a running Dragonfly instance.

Usage:
    # Start dragonfly in another terminal:
    ./build-dbg/dragonfly --port 6379 --logtostderr --proactor_threads 1

    # Replay crash:
    python3 fuzz/replay_crash.py fuzz/artifacts/resp/default/crashes 000000
"""

import glob
import os
import socket
import sys


def send_input(host, port, data):
    """Send data over TCP. Mirrors SendFuzzInputToServer."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(0.2)
        s.connect((host, port))
    except ConnectionRefusedError:
        print("\033[0;31m[ERROR]\033[0m Connection refused — is Dragonfly running?")
        sys.exit(1)

    try:
        s.sendall(data)
    except Exception:
        pass

    try:
        s.recv(4096)
    except Exception:
        pass
    s.close()


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <crash_dir> <crash_id> [host] [port]")
        sys.exit(1)

    crash_dir = sys.argv[1]
    crash_id = sys.argv[2]
    host = sys.argv[3] if len(sys.argv) > 3 else "127.0.0.1"
    port = int(sys.argv[4]) if len(sys.argv) > 4 else 6379

    # Find RECORD files sorted by cnt
    pattern = os.path.join(crash_dir, f"RECORD:{crash_id},cnt:*")
    records = sorted(glob.glob(pattern))

    if not records:
        print(f"\033[0;31m[ERROR]\033[0m No RECORD files for crash {crash_id}")
        sys.exit(1)

    # Find crash input file
    crash_files = [
        f
        for f in glob.glob(os.path.join(crash_dir, f"id:{crash_id},*"))
        if not os.path.basename(f).startswith("RECORD:")
    ]
    if not crash_files:
        print(f"\033[0;31m[ERROR]\033[0m Crash input not found for id:{crash_id}")
        sys.exit(1)

    crash_file = crash_files[0]

    print(f"\033[0;32m[INFO]\033[0m Replaying crash {crash_id} against {host}:{port}")
    print(f"\033[0;32m[INFO]\033[0m RECORD files: {len(records)}")
    print(f"\033[0;32m[INFO]\033[0m Crash file: {crash_file}")
    print()

    # Replay all RECORD inputs
    for i, rec in enumerate(records):
        if i % 1000 == 0:
            print(f"\033[1;33m[REPLAY]\033[0m Progress: {i} / {len(records)}")
        with open(rec, "rb") as f:
            data = f.read()
        send_input(host, port, data)

    # Send the crash input
    print(f"\033[1;33m[REPLAY]\033[0m Sending crash input: {os.path.basename(crash_file)}")
    with open(crash_file, "rb") as f:
        data = f.read()
    send_input(host, port, data)

    print()
    print("\033[0;32m[INFO]\033[0m Replay complete. Check if the Dragonfly process crashed.")
    print(
        "\033[0;32m[INFO]\033[0m If not, the bug may depend on thread timing (non-deterministic)."
    )


if __name__ == "__main__":
    main()
