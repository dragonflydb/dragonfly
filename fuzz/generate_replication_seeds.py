#!/usr/bin/env python3
"""
Generate seed corpus for replication fuzzing.

Seeds are binary files in the multiplexed format understood by
replication_orchestrator.py.  Each file encodes a short sequence of actions
(master commands, replica commands, network drops, waits) that exercise a
specific replication scenario.

Usage:
  python3 fuzz/generate_replication_seeds.py
  python3 fuzz/generate_replication_seeds.py --output-dir fuzz/seeds/replication
"""

import argparse
import os
import struct

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUT = os.path.join(SCRIPT_DIR, "seeds", "replication")

ACTION_MASTER = 0
ACTION_REPLICA = 1
ACTION_NET_DROP = 2
ACTION_WAIT = 3


# ── Helpers ───────────────────────────────────────────────────────────────────


def _resp(*args) -> bytes:
    parts = [f"*{len(args)}\r\n".encode()]
    for a in args:
        if isinstance(a, str):
            a = a.encode()
        parts.append(f"${len(a)}\r\n".encode() + a + b"\r\n")
    return b"".join(parts)


def act(atype: int, *resp_args) -> bytes:
    """Encode a single action."""
    payload = _resp(*resp_args) if resp_args else b""
    return bytes([atype]) + struct.pack("<H", len(payload)) + payload


def seed(*actions: bytes) -> bytes:
    """Pack actions into a seed file."""
    return bytes([len(actions)]) + b"".join(actions)


M = ACTION_MASTER
R = ACTION_REPLICA
D = ACTION_NET_DROP
W = ACTION_WAIT


# ── Seed definitions ──────────────────────────────────────────────────────────

SEEDS = {
    # Basic write on master, read-back on replica
    "basic_set_get.repl": seed(
        act(M, "SET", "foo", "bar"),
        act(W),
        act(R, "GET", "foo"),
        act(R, "EXISTS", "foo"),
    ),
    # Multiple data types replicated
    "multi_type.repl": seed(
        act(M, "SET", "strkey", "hello"),
        act(M, "LPUSH", "mylist", "a", "b", "c"),
        act(M, "HSET", "myhash", "f1", "v1", "f2", "v2"),
        act(M, "SADD", "myset", "x", "y", "z"),
        act(M, "ZADD", "myzset", "1", "one", "2", "two"),
        act(W),
        act(R, "DBSIZE"),
        act(R, "GET", "strkey"),
        act(R, "LRANGE", "mylist", "0", "-1"),
        act(R, "SMEMBERS", "myset"),
    ),
    # Network drop then write, replica should catch up
    "net_drop_recovery.repl": seed(
        act(M, "SET", "before_drop", "v1"),
        act(D),
        act(M, "SET", "after_drop", "v2"),
        act(W),
        act(R, "GET", "before_drop"),
        act(R, "GET", "after_drop"),
    ),
    # Multiple drops in sequence
    "multi_drop.repl": seed(
        act(M, "SET", "k1", "v1"),
        act(D),
        act(M, "SET", "k2", "v2"),
        act(D),
        act(M, "SET", "k3", "v3"),
        act(D),
        act(M, "SET", "k4", "v4"),
        act(W),
    ),
    # MULTI/EXEC transaction replicated atomically
    "transaction_repl.repl": seed(
        act(M, "MULTI"),
        act(M, "SET", "tx_k1", "v1"),
        act(M, "INCR", "tx_cnt"),
        act(M, "LPUSH", "tx_list", "item"),
        act(M, "EXEC"),
        act(W),
        act(R, "GET", "tx_k1"),
        act(R, "GET", "tx_cnt"),
        act(R, "LLEN", "tx_list"),
    ),
    # Replica read-only operations (should never modify state)
    "replica_readonly.repl": seed(
        act(M, "MSET", "a", "1", "b", "2", "c", "3"),
        act(W),
        act(R, "GET", "a"),
        act(R, "MGET", "a", "b", "c"),
        act(R, "KEYS", "*"),
        act(R, "DBSIZE"),
        act(R, "TTL", "a"),
        act(R, "TYPE", "a"),
        act(R, "INFO", "replication"),
        act(R, "PING"),
    ),
    # DEL propagates to replica
    "delete_propagation.repl": seed(
        act(M, "MSET", "d1", "v1", "d2", "v2", "d3", "v3"),
        act(W),
        act(R, "EXISTS", "d1"),
        act(M, "DEL", "d1", "d2"),
        act(W),
        act(R, "EXISTS", "d1"),
        act(R, "EXISTS", "d3"),
    ),
    # EXPIRE / TTL replication
    "expiry_repl.repl": seed(
        act(M, "SET", "expkey", "val"),
        act(M, "EXPIRE", "expkey", "100"),
        act(W),
        act(R, "TTL", "expkey"),
        act(R, "GET", "expkey"),
    ),
    # FLUSHDB propagation
    "flush_propagation.repl": seed(
        act(M, "MSET", "x1", "1", "x2", "2"),
        act(W),
        act(R, "DBSIZE"),
        act(M, "FLUSHDB"),
        act(W),
        act(R, "DBSIZE"),
    ),
    # Drop during a transaction on master
    "drop_during_tx.repl": seed(
        act(M, "MULTI"),
        act(M, "SET", "tx_a", "1"),
        act(M, "SET", "tx_b", "2"),
        act(D),
        act(M, "EXEC"),
        act(W),
        act(R, "GET", "tx_a"),
        act(R, "GET", "tx_b"),
    ),
    # Interleaved writes to master and reads from replica
    "interleaved.repl": seed(
        act(M, "SET", "i1", "v1"),
        act(R, "DBSIZE"),
        act(M, "SET", "i2", "v2"),
        act(W),
        act(R, "GET", "i1"),
        act(M, "SET", "i3", "v3"),
        act(R, "GET", "i2"),
        act(W),
        act(R, "GET", "i3"),
    ),
    # Commands sent to both master and replica
    "master_and_replica_cmds.repl": seed(
        act(M, "SET", "shared", "master"),
        act(W),
        act(R, "GET", "shared"),
        act(R, "INFO", "server"),
        act(M, "APPEND", "shared", "_extra"),
        act(W),
        act(R, "GET", "shared"),
        act(R, "STRLEN", "shared"),
    ),
    # Large pipeline to master
    "large_pipeline.repl": seed(
        act(M, "MSET", "p1", "v1", "p2", "v2", "p3", "v3", "p4", "v4", "p5", "v5", "p6", "v6"),
        act(M, "LPUSH", "biglist", "a", "b", "c", "d", "e", "f", "g", "h"),
        act(M, "ZADD", "bigzset", "1", "a", "2", "b", "3", "c", "4", "d"),
        act(W),
        act(R, "DBSIZE"),
        act(R, "LLEN", "biglist"),
        act(R, "ZCARD", "bigzset"),
    ),
}


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate replication fuzzing seeds")
    parser.add_argument("--output-dir", default=DEFAULT_OUT)
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    for name, data in SEEDS.items():
        path = os.path.join(args.output_dir, name)
        with open(path, "wb") as f:
            f.write(data)
        print(f"  {name} ({len(data)} bytes)")

    print(f"\nGenerated {len(SEEDS)} seeds in {args.output_dir}")


if __name__ == "__main__":
    main()
