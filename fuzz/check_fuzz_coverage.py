#!/usr/bin/env python3
"""CI guard: every command registered in Dragonfly must be known to the RESP fuzzer.

Extracts command registrations (CI{"NAME", mask, arity, ...}) from src/server/**/*.cc
and checks that each one is either present in resp_mutator.COMMANDS or listed in
EXCLUDED below with a (non-empty) justification. Also flags stale mutator entries for
commands that no longer exist in the source tree, cross-checks each mutator
(min_args, max_args) tuple against the command's registered arity so the fuzzer never
emits arg counts that fail arity validation before reaching real command logic, and
requires every fuzzed command to appear in at least one fuzz/seeds/resp/*.resp seed.

This removes the need for reviewers to remember fuzz coverage when a PR adds a new
command: the PR author must either add the command to the mutator plus a seed in
fuzz/seeds/resp/ (and, ideally, a token in fuzz/dict/resp.dict) or consciously
exclude it here.

Run from anywhere: paths are resolved relative to this file.
"""

import re
import sys
from pathlib import Path

FUZZ_DIR = Path(__file__).resolve().parent
REPO_ROOT = FUZZ_DIR.parent
SERVER_SRC = REPO_ROOT / "src" / "server"
SEEDS_RESP_DIR = FUZZ_DIR / "seeds" / "resp"

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

# Matches `CI{"NAME", <mask expr, no top-level commas>, <arity>, ...}`. The mask
# expression is a `CO::FOO | CO::BAR`-style OR chain, so it never contains a comma,
# which lets `[^,]+` skip over it to reach the arity field.
CI_RE = re.compile(r'CI\{"([A-Z0-9._]+)",\s*[^,]+,\s*(-?\d+),')


def registered_commands():
    """Returns {name: arity} for every CI{...} registration in src/server."""
    arities = {}
    for path in sorted(SERVER_SRC.rglob("*.cc")):
        for name, arity in CI_RE.findall(path.read_text(encoding="utf-8", errors="replace")):
            arities.setdefault(name, int(arity))
    return arities


def mutator_commands():
    sys.path.insert(0, str(FUZZ_DIR))
    import resp_mutator

    return {name.decode(): (lo, hi) for name, lo, hi in resp_mutator.COMMANDS}


# A command name occupying its own line in a seed's RESP bulk-string encoding
# (e.g. the "GET" line in "$3\r\nGET\r\n"). Seeds mix CRLF and bare-LF line
# endings, so splitlines()+strip() is used to normalize both.
SEED_COMMAND_RE = re.compile(r"^[A-Z0-9._]+$")


def seeded_commands():
    """Returns the set of command names that appear in some fuzz/seeds/resp/*.resp file."""
    names = set()
    for path in sorted(SEEDS_RESP_DIR.glob("*.resp")):
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            token = line.strip()
            if SEED_COMMAND_RE.fullmatch(token):
                names.add(token)
    return names


def main():
    registered = registered_commands()
    if not registered:
        print(f"error: no CI{{...}} registrations found under {SERVER_SRC}", file=sys.stderr)
        return 2
    reg_names = registered.keys()
    fuzzed = mutator_commands()
    fuzzed_names = fuzzed.keys()
    seeded = seeded_commands()

    errors = []

    for name, reason in EXCLUDED.items():
        if not reason.strip():
            errors.append(f"EXCLUDED[{name!r}] has an empty justification. Add a reason.")

    missing = reg_names - fuzzed_names - EXCLUDED.keys() - KNOWN_GAPS
    for name in sorted(missing):
        errors.append(
            f"command {name} is registered but unknown to the fuzzer.\n"
            f"  Fix: add it to COMMANDS in fuzz/resp_mutator.py (plus a seed in\n"
            f"  fuzz/seeds/resp/ and a token in fuzz/dict/resp.dict), or exclude it\n"
            f"  with a justification in fuzz/check_fuzz_coverage.py."
        )

    for name in sorted(fuzzed_names - reg_names):
        errors.append(
            f"stale entry: {name} is in resp_mutator.COMMANDS but no longer registered\n"
            f"  in src/server. Remove it from fuzz/resp_mutator.py."
        )

    for name in sorted((EXCLUDED.keys() | KNOWN_GAPS) & fuzzed_names):
        errors.append(
            f"{name} is both fuzzed and listed in fuzz/check_fuzz_coverage.py.\n"
            f"  Remove it from EXCLUDED/KNOWN_GAPS."
        )

    for name in sorted((EXCLUDED.keys() | KNOWN_GAPS) - reg_names):
        errors.append(
            f"{name} is listed in fuzz/check_fuzz_coverage.py but not registered\n"
            f"  in src/server. Remove the dead entry."
        )

    # Cross-check the mutator's (min_args, max_args) against the real registered
    # arity (see CommandId::Validate in command_registry.cc): positive arity N
    # requires exactly N-1 args after the name; negative arity -N requires at
    # least N-1 (max_args may be larger, to cover documented option grammar);
    # arity 0 means unchecked (no minimum). min_args must be >= the required
    # minimum so the mutator never emits a command that fails arity validation
    # before reaching real command logic.
    for name in sorted(reg_names & fuzzed_names):
        arity = registered[name]
        lo, hi = fuzzed[name]
        expected_min = max(abs(arity) - 1, 0)
        if lo < expected_min:
            errors.append(
                f"{name}: resp_mutator.COMMANDS min_args={lo} is below the registered\n"
                f"  arity={arity} (requires at least {expected_min} args after the name).\n"
                f"  Fix the tuple in fuzz/resp_mutator.py."
            )
        elif arity > 0 and (lo != expected_min or hi != expected_min):
            errors.append(
                f"{name}: fixed arity={arity} takes exactly {expected_min} args after the\n"
                f"  name, but resp_mutator.COMMANDS has ({lo}, {hi}). Fix the tuple in\n"
                f"  fuzz/resp_mutator.py."
            )
        elif hi < lo:
            errors.append(f"{name}: resp_mutator.COMMANDS max_args={hi} < min_args={lo}.")

    # Every fuzzed command must actually be exercised by at least one seed, or the
    # fuzzer starts every run without a single valid example of that command to mutate.
    for name in sorted(fuzzed_names - seeded):
        errors.append(
            f"{name} is in resp_mutator.COMMANDS but no seed under fuzz/seeds/resp/\n"
            f"  contains it. Add an example invocation to an existing *.resp seed file\n"
            f"  (or a new one)."
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
