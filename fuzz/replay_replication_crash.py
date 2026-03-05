#!/usr/bin/env python3
"""
Replay a replication fuzzing crash.

AFL++ saves the crashing input plus (if AFL_PERSISTENT_RECORD is set) the
sequence of inputs that built the server state before the crash.  This script
replays all RECORD inputs in order and then replays the crashing input so the
bug can be observed and debugged.

Usage:
  # 1. Start master and replica manually:
  ./build-dbg/dragonfly --port 7379 --logtostderr --proactor_threads 2 --dbfilename=""
  ./build-dbg/dragonfly --port 7380 --logtostderr --proactor_threads 2 --dbfilename=""

  # 2. Connect replica to master:
  redis-cli -p 7380 REPLICAOF 127.0.0.1 7379

  # 3. Replay:
  python3 fuzz/replay_replication_crash.py fuzz/artifacts/replication/default/crashes 0

Options:
  --master-port PORT   (default: 7379)
  --replica-port PORT  (default: 7380)
  --verbose / -v       Verbose logging
"""

import argparse
import logging
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

import replication_orchestrator as orch


def find_crash_input(crashes_dir: str, crash_id: int) -> "tuple[bytes, str] | tuple[None, None]":
    prefix = f"id:{crash_id:06d},"
    for fname in sorted(os.listdir(crashes_dir)):
        if fname.startswith(prefix):
            path = os.path.join(crashes_dir, fname)
            with open(path, "rb") as f:
                return f.read(), fname
    return None, None


def find_record_files(crashes_dir: str, crash_id: int) -> list:
    prefix = f"RECORD:{crash_id:06d},cnt:"
    files = sorted(f for f in os.listdir(crashes_dir) if f.startswith(prefix))
    return files


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay a replication fuzzing crash",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("crashes_dir", help="Path to AFL++ crashes directory")
    parser.add_argument("crash_id", type=int, help="Crash ID number (e.g. 0 for id:000000,...)")
    parser.add_argument("--master-port", type=int, default=None)
    parser.add_argument("--replica-port", type=int, default=None)
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    if args.master_port:
        orch.MASTER_PORT = args.master_port
    if args.replica_port:
        orch.REPLICA_PORT = args.replica_port

    # Verify instances are reachable
    if not orch.ping(orch.MASTER_HOST, orch.MASTER_PORT):
        print(
            f"ERROR: master not responding at {orch.MASTER_HOST}:{orch.MASTER_PORT}",
            file=sys.stderr,
        )
        sys.exit(1)
    if not orch.ping(orch.REPLICA_HOST, orch.REPLICA_PORT):
        print(
            f"ERROR: replica not responding at {orch.REPLICA_HOST}:{orch.REPLICA_PORT}",
            file=sys.stderr,
        )
        sys.exit(1)

    crashes_dir = args.crashes_dir
    crash_id = args.crash_id

    # Replay RECORD files to rebuild pre-crash state
    records = find_record_files(crashes_dir, crash_id)
    if records:
        print(f"Replaying {len(records)} RECORD input(s) to rebuild state...")
        for i, fname in enumerate(records):
            path = os.path.join(crashes_dir, fname)
            with open(path, "rb") as f:
                data = f.read()
            print(f"  [{i + 1}/{len(records)}] {fname}")
            orch.run_iteration(data)
            orch.reset_state()
    else:
        print("No RECORD files found – replaying crash input directly.")
        print("(Run AFL++ with AFL_PERSISTENT_RECORD=N to capture state history.)")

    # Replay the crashing input
    data, fname = find_crash_input(crashes_dir, crash_id)
    if data is None:
        print(
            f"ERROR: crash input id:{crash_id:06d},... not found in {crashes_dir}",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"\nReplaying crash input: {fname}")
    ok = orch.run_iteration(data)

    if not ok:
        print("\n[REPRODUCED] Bug confirmed.")
        sys.exit(1)
    else:
        print("\n[NOT REPRODUCED] No bug detected with this input.")
        print("The crash may be timing-dependent or require more state history.")
        print("Try running AFL++ with AFL_PERSISTENT_RECORD=N and replay again.")
        sys.exit(0)


if __name__ == "__main__":
    main()
