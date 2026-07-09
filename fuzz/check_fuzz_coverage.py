#!/usr/bin/env python3
"""CI guard: every command registered in Dragonfly must be known to the RESP fuzzer.

Extracts command registrations (CI{"NAME"...}) from src/server/**/*.cc and checks
that each one is either present in resp_mutator.COMMANDS or listed in EXCLUDED below
with a justification. Also flags stale mutator entries for commands that no longer
exist in the source tree.

This removes the need for reviewers to remember fuzz coverage when a PR adds a new
command: the PR author must either add the command to the mutator (plus, ideally, a
seed in fuzz/seeds/resp/ and a token in fuzz/dict/resp.dict) or consciously exclude
it here.

Run from anywhere: paths are resolved relative to this file.
"""

import re
import sys
from pathlib import Path

FUZZ_DIR = Path(__file__).resolve().parent
REPO_ROOT = FUZZ_DIR.parent
SERVER_SRC = REPO_ROOT / "src" / "server"

# Commands intentionally NOT fuzzed, with the reason. New entries require a
# justification — prefer adding the command to resp_mutator.COMMANDS instead.
EXCLUDED = {
    # Replication / topology: would turn the fuzzed instance into a replica or
    # tear down the fuzzing session entirely.
    "REPLICAOF": "changes replication topology mid-fuzz",
    "SLAVEOF": "alias of REPLICAOF",
    "ADDREPLICAOF": "changes replication topology mid-fuzz",
    "REPLTAKEOVER": "changes replication topology mid-fuzz",
    "REPLCONF": "internal replication handshake",
    # Cluster management / internal cluster protocol.
    "CLUSTER": "cluster management",
    "DFLYCLUSTER": "internal cluster protocol",
    "DFLYMIGRATE": "internal cluster protocol",
    "READONLY": "cluster no-op",
    "READWRITE": "cluster no-op",
    # Admin / process control: destroys state or kills the server.
    "SHUTDOWN": "kills the fuzzed process",
    "FLUSHALL": "wipes the dataset, destroying accumulated fuzz state",
    "FLUSHDB": "wipes the dataset, destroying accumulated fuzz state",
    "DEBUG": "admin: can crash/stall the process by design",
    "DFLY": "internal admin command",
    "MODULE": "admin-only stub",
    "AUTH": "can lock the fuzzing connection out of the session",
    # Not reachable / not useful over RESP.
    "GAT": "memcache-only; RESP path rejects immediately (covered by memcache fuzzer)",
    "_XGROUP_HELP": "hidden internal helper",
}

# Commands that predate this check and are still missing from the mutator.
# TODO: burn this list down (see resp_mutator.COMMANDS); do NOT add new entries —
# new commands must go into resp_mutator.COMMANDS or EXCLUDED above.
KNOWN_GAPS = {
    "BF.INFO",
    "RM",
    "ROLE",
    "LASTSAVE",
}

CI_RE = re.compile(r'CI\{"([A-Z0-9._]+)"')


def registered_commands():
    names = set()
    for path in sorted(SERVER_SRC.rglob("*.cc")):
        names.update(CI_RE.findall(path.read_text(encoding="utf-8", errors="replace")))
    return names


def mutator_commands():
    sys.path.insert(0, str(FUZZ_DIR))
    import resp_mutator

    return {name.decode() for name, _, _ in resp_mutator.COMMANDS}


def main():
    registered = registered_commands()
    if not registered:
        print(f"error: no CI{{...}} registrations found under {SERVER_SRC}", file=sys.stderr)
        return 2
    fuzzed = mutator_commands()

    errors = []

    missing = registered - fuzzed - EXCLUDED.keys() - KNOWN_GAPS
    for name in sorted(missing):
        errors.append(
            f"command {name} is registered but unknown to the fuzzer.\n"
            f"  Fix: add it to COMMANDS in fuzz/resp_mutator.py (plus a seed in\n"
            f"  fuzz/seeds/resp/ and a token in fuzz/dict/resp.dict), or exclude it\n"
            f"  with a justification in fuzz/check_fuzz_coverage.py."
        )

    for name in sorted(fuzzed - registered):
        errors.append(
            f"stale entry: {name} is in resp_mutator.COMMANDS but no longer registered\n"
            f"  in src/server. Remove it from fuzz/resp_mutator.py."
        )

    for name in sorted((EXCLUDED.keys() | KNOWN_GAPS) & fuzzed):
        errors.append(
            f"{name} is both fuzzed and listed in fuzz/check_fuzz_coverage.py.\n"
            f"  Remove it from EXCLUDED/KNOWN_GAPS."
        )

    for name in sorted((EXCLUDED.keys() | KNOWN_GAPS) - registered):
        errors.append(
            f"{name} is listed in fuzz/check_fuzz_coverage.py but not registered\n"
            f"  in src/server. Remove the dead entry."
        )

    if errors:
        print("fuzz command coverage check FAILED:\n", file=sys.stderr)
        for e in errors:
            print(f"* {e}\n", file=sys.stderr)
        return 1

    gaps = f", {len(KNOWN_GAPS)} known gaps" if KNOWN_GAPS else ""
    print(
        f"fuzz command coverage OK: {len(registered)} registered, "
        f"{len(fuzzed)} fuzzed, {len(EXCLUDED)} excluded{gaps}."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
