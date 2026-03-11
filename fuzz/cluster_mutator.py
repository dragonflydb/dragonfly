"""AFL++ custom mutator for Dragonfly cluster mode fuzzing.

Operates at the RESP command level, like resp_mutator.py, but with a mix
of cluster-specific commands (CLUSTER, DFLYCLUSTER) and regular data ops.

Node 1 (the fuzz target) owns slots 8192-16383; node 0 owns 0-8191.
Keys are chosen from both ranges so we exercise:
  - Commands handled locally (node 1's slots)
  - Commands that produce MOVED responses (node 0's slots)
  - Cluster metadata commands (CLUSTER INFO/MYID/SLOTS/NODES/...)
  - Slot migrations (outgoing: node1→node0, incoming: node0→node1)
  - Migration cancellation mid-flight (new CONFIG without migrations)
  - DFLYCLUSTER FLUSHSLOTS (deletes slot data)

Every mutated output ALWAYS starts with DFLYCLUSTER CONFIG so that node 1 is
in a known cluster state from the first calibration run.  This keeps AFL++
coverage stable (avoids the unconfigured-node vs configured-node coverage
split that would otherwise cause ~18% stability and AFL++ crashes).

Migration configs are a major focus because the migration code path
(OutgoingMigration::SyncFb, FinalizeMigration, Pause) is the most complex
and most likely to contain races.
"""

import json
import random

# ---------------------------------------------------------------------------
# Cluster config embedded in every fuzz output as the first command.
# Must match the IDs/ports used in run_fuzzer.sh / cluster_monitor.py.
# ---------------------------------------------------------------------------

_NODE0_ID = "0000000000000000000000000000000000000000"
_NODE1_ID = "1111111111111111111111111111111111111111"
_NODE0_PORT = 6380
_NODE1_PORT = 6379

# Base config: no active migrations; node0 owns 0-8191, node1 owns 8192-16383.
_CONFIG_JSON = json.dumps(
    [
        {
            "slot_ranges": [{"start": 0, "end": 8191}],
            "master": {"id": _NODE0_ID, "ip": "127.0.0.1", "port": _NODE0_PORT},
            "replicas": [],
        },
        {
            "slot_ranges": [{"start": 8192, "end": 16383}],
            "master": {"id": _NODE1_ID, "ip": "127.0.0.1", "port": _NODE1_PORT},
            "replicas": [],
        },
    ],
    separators=(",", ":"),
).encode()


# ---------------------------------------------------------------------------
# Keys: mix of slots owned by node 1 (8192-16383) and node 0 (0-8191).
# Hashtag format {N} deterministically maps to a slot.
# Slot of {N} = crc_hqx(N.encode(), 0) % 16384
# ---------------------------------------------------------------------------

# Keys guaranteed to land on node 1 (slots 8192+)
NODE1_KEYS = [b"foo", b"qux", b"{0}", b"{1}", b"hello", b"alpha", b"beta"]
# Keys guaranteed to land on node 0 (slots 0..8191)
NODE0_KEYS = [b"bar", b"baz", b"{2}", b"{3}", b"world", b"gamma", b"delta"]
# Mixed pool
ALL_KEYS = NODE1_KEYS + NODE0_KEYS

VALUES = [b"1", b"0", b"-1", b"hello", b"world", b"x" * 64, b"", b"123456789"]
FIELDS = [b"f1", b"f2", b"name", b"score", b"data"]

# ---------------------------------------------------------------------------
# CLUSTER subcommands
# ---------------------------------------------------------------------------

CLUSTER_SUBCMDS = [
    b"INFO",
    b"MYID",
    b"SLOTS",
    b"SHARDS",
    b"NODES",
    b"RESET",
    b"RESET HARD",
    b"RESET SOFT",
]


def _encode_resp(*args):
    parts = [b"*%d\r\n" % len(args)]
    for a in args:
        if not isinstance(a, bytes):
            a = str(a).encode()
        parts.append(b"$%d\r\n%s\r\n" % (len(a), a))
    return b"".join(parts)


def _config_cmd():
    return _encode_resp(b"DFLYCLUSTER", b"CONFIG", _CONFIG_JSON)


def _random_key():
    return random.choice(ALL_KEYS)


def _random_value():
    return random.choice(VALUES)


def _cluster_cmd():
    sub = random.choice(CLUSTER_SUBCMDS)
    parts = sub.split()
    return _encode_resp(b"CLUSTER", *parts)


def _keyslot_cmd():
    return _encode_resp(b"CLUSTER", b"KEYSLOT", _random_key())


def _getkeysinslot_cmd():
    slot = random.randint(0, 16383)
    count = random.randint(1, 10)
    return _encode_resp(b"CLUSTER", b"GETKEYSINSLOT", str(slot).encode(), str(count).encode())


def _countkeysinslot_cmd():
    slot = random.randint(0, 16383)
    return _encode_resp(b"CLUSTER", b"COUNTKEYSINSLOT", str(slot).encode())


def _dflycluster_getslotinfo():
    n = random.randint(1, 5)
    slots = [str(random.randint(0, 16383)).encode() for _ in range(n)]
    return _encode_resp(b"DFLYCLUSTER", b"GETSLOTINFO", b"SLOTS", *slots)


def _dflycluster_migration_status():
    return _encode_resp(b"DFLYCLUSTER", b"SLOT-MIGRATION-STATUS")


def _flushslots_cmd():
    """DFLYCLUSTER FLUSHSLOTS — deletes data for a slot range.

    Exercises the DeleteSlots path concurrently with data operations.
    Focus on slots owned by node1 (8192-16383) to hit the actual delete path.
    """
    start = random.randint(8192, 16383)
    end = random.randint(start, min(start + random.randint(1, 300), 16383))
    return _encode_resp(b"DFLYCLUSTER", b"FLUSHSLOTS", str(start).encode(), str(end).encode())


# ---------------------------------------------------------------------------
# Migration configs — the most complex and race-prone code paths.
#
# Outgoing (node1 → node0): node1's CONFIG declares it is migrating some of
# its slots to node0.  This starts OutgoingMigration::SyncFb which connects
# to node0, calls DFLYMIGRATE INIT/FLOW/ACK, and pauses clients during
# finalization.  Cancelling mid-flight by sending a clean CONFIG is especially
# interesting.
#
# Incoming (node0 → node1): node0's shard in the CONFIG declares a migration
# toward node1.  This starts IncomingSlotMigration on node1 which listens
# for the DFLYMIGRATE FLOW from node0.
# ---------------------------------------------------------------------------


def _outgoing_migration_config_cmd():
    """Config where node1 is pushing some of its slots to node0."""
    start = random.choice([8192, 8192, 8192, 8200, 8500, 8192])
    end = random.randint(start, min(start + random.randint(1, 500), 16383))
    cfg = json.dumps(
        [
            {
                "slot_ranges": [{"start": 0, "end": 8191}],
                "master": {"id": _NODE0_ID, "ip": "127.0.0.1", "port": _NODE0_PORT},
                "replicas": [],
            },
            {
                "slot_ranges": [{"start": 8192, "end": 16383}],
                "master": {"id": _NODE1_ID, "ip": "127.0.0.1", "port": _NODE1_PORT},
                "replicas": [],
                "migrations": [
                    {
                        "slot_ranges": [{"start": start, "end": end}],
                        "node_id": _NODE0_ID,
                        "ip": "127.0.0.1",
                        "port": _NODE0_PORT,
                    }
                ],
            },
        ],
        separators=(",", ":"),
    ).encode()
    return _encode_resp(b"DFLYCLUSTER", b"CONFIG", cfg)


def _incoming_migration_config_cmd():
    """Config where node0 is pushing some of its slots to node1."""
    end = random.choice([8191, 8191, 8191, 8100, 7900, 8191])
    start = random.randint(max(0, end - random.randint(1, 500)), end)
    cfg = json.dumps(
        [
            {
                "slot_ranges": [{"start": 0, "end": 8191}],
                "master": {"id": _NODE0_ID, "ip": "127.0.0.1", "port": _NODE0_PORT},
                "replicas": [],
                "migrations": [
                    {
                        "slot_ranges": [{"start": start, "end": end}],
                        "node_id": _NODE1_ID,
                        "ip": "127.0.0.1",
                        "port": _NODE1_PORT,
                    }
                ],
            },
            {
                "slot_ranges": [{"start": 8192, "end": 16383}],
                "master": {"id": _NODE1_ID, "ip": "127.0.0.1", "port": _NODE1_PORT},
                "replicas": [],
            },
        ],
        separators=(",", ":"),
    ).encode()
    return _encode_resp(b"DFLYCLUSTER", b"CONFIG", cfg)


def _data_cmd():
    r = random.random()
    key = _random_key()
    if r < 0.18:
        return _encode_resp(b"SET", key, _random_value())
    if r < 0.28:
        return _encode_resp(b"GET", key)
    if r < 0.33:
        return _encode_resp(b"DEL", key)
    if r < 0.38:
        return _encode_resp(b"INCR", key)
    if r < 0.43:
        return _encode_resp(b"EXISTS", key)
    if r < 0.48:
        return _encode_resp(b"EXPIRE", key, str(random.randint(1, 100)).encode())
    if r < 0.52:
        return _encode_resp(b"TTL", key)
    if r < 0.57:
        k2 = _random_key()
        return _encode_resp(b"MGET", key, k2)
    if r < 0.62:
        k2 = _random_key()
        return _encode_resp(b"MSET", key, _random_value(), k2, _random_value())
    # Hash ops
    if r < 0.68:
        field = random.choice(FIELDS)
        return _encode_resp(b"HSET", key, field, _random_value())
    if r < 0.73:
        field = random.choice(FIELDS)
        return _encode_resp(b"HGET", key, field)
    # List ops
    if r < 0.78:
        return _encode_resp(b"LPUSH", key, _random_value())
    if r < 0.82:
        return _encode_resp(b"LRANGE", key, b"0", b"-1")
    # Set ops
    if r < 0.87:
        return _encode_resp(b"SADD", key, _random_value())
    if r < 0.91:
        return _encode_resp(b"SMEMBERS", key)
    # ZSet ops
    if r < 0.96:
        score = str(random.randint(0, 1000)).encode()
        return _encode_resp(b"ZADD", key, score, _random_value())
    return _encode_resp(b"ZRANGE", key, b"0", b"-1")


# ---------------------------------------------------------------------------
# Weighted generator table: (generator_fn, weight)
#
# Weights are tuned so that:
#  - Migration configs (~22%) dominate — these are the highest-value paths
#  - Data ops (~32%) keep the server state interesting for migrations to stream
#  - Cluster info/slot queries (~20%) cover metadata paths
#  - FLUSHSLOTS (~6%) creates concurrent delete/write races
# ---------------------------------------------------------------------------

_GENERATORS = [
    (_data_cmd, 32),
    (_outgoing_migration_config_cmd, 12),
    (_incoming_migration_config_cmd, 10),
    (_config_cmd, 8),  # cancel any active migration
    (_cluster_cmd, 10),
    (_keyslot_cmd, 5),
    (_getkeysinslot_cmd, 5),
    (_countkeysinslot_cmd, 4),
    (_flushslots_cmd, 6),
    (_dflycluster_getslotinfo, 4),
    (_dflycluster_migration_status, 4),
]

_TOTAL_WEIGHT = sum(w for _, w in _GENERATORS)
_THRESHOLDS = []
_cum = 0
for _fn, _w in _GENERATORS:
    _cum += _w
    _THRESHOLDS.append((_cum / _TOTAL_WEIGHT, _fn))


def _pick_generator():
    r = random.random()
    for threshold, fn in _THRESHOLDS:
        if r < threshold:
            return fn
    return _GENERATORS[-1][0]


def _random_command():
    return _pick_generator()()


# ---------------------------------------------------------------------------
# RESP parser (best-effort)
# ---------------------------------------------------------------------------


def _parse_resp_commands(buf):
    """Parse RESP buffer into list of command arg-lists (each arg is bytes).

    Returns (commands, ok).  Skips the leading DFLYCLUSTER CONFIG command
    if present — the mutator always re-prepends a fresh valid one.
    """
    commands = []
    pos = 0
    data = bytes(buf)

    while pos < len(data):
        # Skip stray whitespace
        while pos < len(data) and data[pos : pos + 1] in (b"\r", b"\n", b" "):
            pos += 1
        if pos >= len(data):
            break

        if data[pos : pos + 1] != b"*":
            return [], False

        end = data.find(b"\r\n", pos)
        if end < 0:
            return [], False
        try:
            nargs = int(data[pos + 1 : end])
        except ValueError:
            return [], False
        pos = end + 2

        args = []
        ok = True
        for _ in range(nargs):
            if pos >= len(data) or data[pos : pos + 1] != b"$":
                ok = False
                break
            end2 = data.find(b"\r\n", pos)
            if end2 < 0:
                ok = False
                break
            try:
                alen = int(data[pos + 1 : end2])
            except ValueError:
                ok = False
                break
            pos = end2 + 2
            args.append(bytes(data[pos : pos + alen]))
            pos += alen + 2  # skip trailing \r\n

        if not ok:
            return [], False

        # Drop any existing DFLYCLUSTER CONFIG commands — we always re-prepend one.
        if len(args) >= 2 and args[0].upper() == b"DFLYCLUSTER" and args[1].upper() == b"CONFIG":
            continue

        commands.append(args)

    return commands, True


# ---------------------------------------------------------------------------
# AFL++ interface
# ---------------------------------------------------------------------------


def init(seed):
    random.seed(seed)


def fuzz(buf, add_buf, max_size):
    """Main mutation entry point called by AFL++ for each test case.

    Always prepends a valid DFLYCLUSTER CONFIG command so AFL++ sees
    consistent coverage from calibration run 1 onwards.
    """
    cmds, ok = _parse_resp_commands(buf)
    if not ok or not cmds:
        # Generate fresh sequence
        n = random.randint(1, 6)
        body = b"".join(_random_command() for _ in range(n))
    else:
        mutation = random.random()

        if mutation < 0.2:
            idx = random.randrange(len(cmds))
            cmds[idx] = None  # replaced below
        elif mutation < 0.4:
            ins = random.randint(0, len(cmds))
            cmds.insert(ins, None)
        elif mutation < 0.55 and len(cmds) > 1:
            cmds.pop(random.randrange(len(cmds)))
        elif mutation < 0.65 and len(cmds) > 1:
            i, j = random.sample(range(len(cmds)), 2)
            cmds[i], cmds[j] = cmds[j], cmds[i]
        elif mutation < 0.8 and cmds:
            cmd = random.choice([c for c in cmds if c is not None])
            if cmd and len(cmd) > 1:
                idx = random.randrange(1, len(cmd))
                cmd[idx] = _random_value() if random.random() < 0.5 else _random_key()
        else:
            cmds.append(None)

        parts = []
        for cmd in cmds:
            if cmd is None:
                parts.append(_random_command())
            else:
                parts.append(_encode_resp(*cmd))
        body = b"".join(parts)

    # Always lead with a valid cluster config command.
    result = _config_cmd() + body
    return bytearray(result[:max_size])


def havoc_mutation(buf, max_size):
    """Called during havoc stage — single small mutation."""
    cmds, ok = _parse_resp_commands(buf)
    if not ok or not cmds:
        result = _config_cmd() + _random_command()
        return bytearray(result[:max_size])

    # Pick one mutation to apply
    mutation = random.random()
    if mutation < 0.4 and len(cmds) > 1:
        cmds.pop(random.randrange(len(cmds)))
    elif mutation < 0.7:
        cmds.insert(random.randint(0, len(cmds)), None)
    else:
        idx = random.randrange(len(cmds))
        cmds[idx] = None

    parts = []
    for cmd in cmds:
        if cmd is None:
            parts.append(_random_command())
        else:
            parts.append(_encode_resp(*cmd))
    result = _config_cmd() + b"".join(parts)
    return bytearray(result[:max_size])


def havoc_mutation_probability():
    """How often our havoc_mutation is called vs AFL++'s built-in mutations."""
    return 50
