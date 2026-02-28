#!/usr/bin/env python3
"""Replays a hang input from AFL++ and verifies if it's a real server deadlock.

Unlike crashes, AFL++ does NOT save RECORD files for hangs — the process is killed
with SIGKILL when the timeout triggers, so no cleanup runs. This means we can't
replay accumulated server state the way replay_crash.py does.

Two replay strategies are available:
  1. Direct (default): send just the hang file and check if the server deadlocks.
     Works for hangs that don't depend on prior state (e.g. mutex/fiber deadlocks
     triggered by a single input).
  2. With queue (--queue-dir): replay the full corpus queue first to rebuild server
     state, then send the hang file. Use this if direct replay doesn't reproduce.

Usage:
    # Start dragonfly in another terminal:
    ./build-dbg/dragonfly --port 6379 --logtostderr --proactor_threads 1

    # Direct replay:
    python3 fuzz/replay_hang.py fuzz/artifacts/resp/default/hangs/id:000000,...

    # With accumulated state:
    python3 fuzz/replay_hang.py fuzz/artifacts/resp/default/hangs/id:000000,... \\
        --queue-dir fuzz/artifacts/resp/default/queue

    # Memcache target:
    python3 fuzz/replay_hang.py fuzz/artifacts/memcache/default/hangs/id:000000,... --memcache
"""

import argparse
import glob
import os
import socket
import sys
import threading
import time

POLL_INTERVAL = 0.1  # seconds between SET/GET liveness polls
HANG_TIMEOUT = 5.0  # seconds without response = confirmed hang


def send_input(host, port, data, timeout=0.2):
    """Send data over TCP and read the response. Mirrors SendFuzzInputToServer."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((host, port))
    except (ConnectionRefusedError, OSError):
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


def check_liveness(host, port, is_memcache, timeout):
    """Send a SET command and wait for response. Returns True if server is alive."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((host, port))
    except (ConnectionRefusedError, OSError):
        return False

    try:
        if is_memcache:
            cmd = b"set __afl_hc 0 0 2\r\nok\r\n"
            expected = b"STORED"
        else:
            cmd = b"*3\r\n$3\r\nSET\r\n$8\r\n__afl_hc\r\n$2\r\nok\r\n"
            expected = b"+OK"

        s.sendall(cmd)
        resp = s.recv(64)
        return expected in resp
    except Exception:
        return False
    finally:
        s.close()


def poll_liveness(host, port, is_memcache, stop_event, result):
    """Poll server liveness in a loop. Sets result['hung'] = True if server stops responding."""
    consecutive_failures = 0
    while not stop_event.is_set():
        alive = check_liveness(host, port, is_memcache, timeout=HANG_TIMEOUT)
        if not alive:
            consecutive_failures += 1
            if consecutive_failures >= 2:
                result["hung"] = True
                stop_event.set()
                return
        else:
            consecutive_failures = 0
        time.sleep(POLL_INTERVAL)


def replay_queue(host, port, queue_dir):
    """Replay all corpus queue files to rebuild accumulated server state."""
    queue_files = sorted(f for f in glob.glob(os.path.join(queue_dir, "id:*")) if os.path.isfile(f))
    if not queue_files:
        print(f"\033[1;33m[QUEUE]\033[0m No queue files found in {queue_dir}")
        return

    print(f"\033[1;33m[QUEUE]\033[0m Replaying {len(queue_files)} corpus files to rebuild state...")
    for i, path in enumerate(queue_files):
        if i % 500 == 0:
            print(f"\033[1;33m[QUEUE]\033[0m  Progress: {i} / {len(queue_files)}")
        with open(path, "rb") as f:
            data = f.read()
        send_input(host, port, data)
    print(f"\033[1;33m[QUEUE]\033[0m Done — {len(queue_files)} files replayed")


def main():
    parser = argparse.ArgumentParser(description="Replay AFL++ hang input against Dragonfly")
    parser.add_argument("hang_file", help="Path to the hang input file (e.g. hangs/id:000000,...)")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--memcache", action="store_true", help="Use memcache text protocol")
    parser.add_argument(
        "--queue-dir",
        metavar="DIR",
        help="Corpus queue directory to replay before the hang input (rebuilds accumulated state). "
        "Use this if direct replay does not reproduce the hang. "
        "Example: fuzz/artifacts/resp/default/queue",
    )
    parser.add_argument(
        "--poll-duration",
        type=float,
        default=10.0,
        help="How long to poll for liveness after sending the hang input (default: 10s)",
    )
    args = parser.parse_args()

    with open(args.hang_file, "rb") as f:
        hang_data = f.read()

    protocol = "memcache" if args.memcache else "resp"
    print(f"\033[0;32m[INFO]\033[0m Hang file:  {args.hang_file}")
    print(f"\033[0;32m[INFO]\033[0m Server:     {args.host}:{args.port} ({protocol})")
    print(f"\033[0;32m[INFO]\033[0m Input size: {len(hang_data)} bytes")
    if args.queue_dir:
        print(f"\033[0;32m[INFO]\033[0m Queue dir:  {args.queue_dir} (state rebuild enabled)")
    else:
        print(
            f"\033[0;32m[INFO]\033[0m Queue dir:  none (direct replay; use --queue-dir if hang doesn't reproduce)"
        )
    print()

    # Verify server is alive before sending
    if not check_liveness(args.host, args.port, args.memcache, timeout=2.0):
        print(
            "\033[0;31m[ERROR]\033[0m Server is not responding before replay — start Dragonfly first"
        )
        sys.exit(1)

    # Optionally rebuild state by replaying the corpus queue
    if args.queue_dir:
        replay_queue(args.host, args.port, args.queue_dir)
        print()
        if not check_liveness(args.host, args.port, args.memcache, timeout=2.0):
            print("\033[0;31m[ERROR]\033[0m Server died during queue replay — check for crashes")
            sys.exit(1)

    print("\033[1;33m[REPLAY]\033[0m Server is alive, starting liveness poller...")

    stop_event = threading.Event()
    result = {"hung": False}
    poller = threading.Thread(
        target=poll_liveness,
        args=(args.host, args.port, args.memcache, stop_event, result),
        daemon=True,
    )
    poller.start()

    print("\033[1;33m[REPLAY]\033[0m Sending hang input...")
    send_input(args.host, args.port, hang_data, timeout=0.2)
    print("\033[1;33m[REPLAY]\033[0m Input sent, polling for liveness...")

    # Wait for either: hang detected or poll duration elapsed
    poller.join(timeout=args.poll_duration)
    stop_event.set()

    print()
    if result["hung"]:
        print("\033[0;31m[RESULT]\033[0m HANG CONFIRMED — server stopped responding to SET/GET")
        sys.exit(1)
    else:
        print("\033[0;32m[RESULT]\033[0m Server remained responsive — hang did NOT reproduce")
        if not args.queue_dir:
            print(
                "\033[0;32m[HINT  ]\033[0m The hang may depend on accumulated server state.\n"
                "\033[0;32m[HINT  ]\033[0m Retry with: --queue-dir fuzz/artifacts/resp/default/queue"
            )


if __name__ == "__main__":
    main()
