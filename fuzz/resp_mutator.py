"""AFL++ custom mutator for RESP protocol.

Instead of random byte-level mutations (which would break protocol framing and get
rejected by the parser), this mutator operates at the command level: it parses
the input into commands, then randomly replaces/inserts/removes/reorders commands and
arguments while keeping RESP encoding valid. This ensures mutated inputs actually
reach command execution code paths.

Focus commands (optional, set via FUZZ_FOCUS_COMMANDS env var):
    When running PR-targeted fuzzing, generate_targeted_seeds.py produces a list of
    command names affected by the code change. This mutator reads that list and
    picks those commands ~70% of the time, concentrating mutations on the changed code.
    Commands not already in the COMMANDS table are auto-registered with default arity.

Usage:
    export PYTHONPATH=/path/to/dragonfly/fuzz
    export AFL_PYTHON_MODULE=resp_mutator
    export AFL_CUSTOM_MUTATOR_ONLY=1
    afl-fuzz ...
"""

import json
import os
import random
import struct

# fmt: off
# Commands grouped by arity pattern: (name, min_args, max_args)
# min/max are argument counts AFTER the command name itself.
COMMANDS = [
    # String
    (b"GET", 1, 1), (b"SET", 2, 6), (b"MGET", 1, 5), (b"MSET", 2, 10),
    (b"SETNX", 2, 2), (b"SETEX", 3, 3), (b"PSETEX", 3, 3),
    (b"INCR", 1, 1), (b"DECR", 1, 1), (b"INCRBY", 2, 2), (b"DECRBY", 2, 2),
    (b"INCRBYFLOAT", 2, 2), (b"APPEND", 2, 2), (b"STRLEN", 1, 1),
    (b"GETRANGE", 3, 3), (b"SETRANGE", 3, 3), (b"GETSET", 2, 2),
    (b"GETDEL", 1, 1), (b"GETEX", 1, 3), (b"SUBSTR", 3, 3),
    (b"MSETNX", 2, 10),
    # Key
    (b"DEL", 1, 5), (b"UNLINK", 1, 5), (b"EXISTS", 1, 5),
    (b"EXPIRE", 2, 3), (b"EXPIREAT", 2, 3), (b"PEXPIRE", 2, 3),
    (b"PEXPIREAT", 2, 3), (b"PERSIST", 1, 1),
    (b"TTL", 1, 1), (b"PTTL", 1, 1), (b"EXPIRETIME", 1, 1), (b"PEXPIRETIME", 1, 1),
    (b"TYPE", 1, 1), (b"RENAME", 2, 2), (b"RENAMENX", 2, 2),
    (b"COPY", 2, 4), (b"DUMP", 1, 1), (b"TOUCH", 1, 5),
    (b"OBJECT", 2, 2), (b"RANDOMKEY", 0, 0), (b"KEYS", 1, 1),
    (b"SCAN", 1, 5), (b"SORT", 1, 7), (b"SORT_RO", 1, 7),
    # List
    (b"LPUSH", 2, 5), (b"RPUSH", 2, 5), (b"LPOP", 1, 2), (b"RPOP", 1, 2),
    (b"LLEN", 1, 1), (b"LINDEX", 2, 2), (b"LSET", 3, 3),
    (b"LRANGE", 3, 3), (b"LTRIM", 3, 3), (b"LREM", 3, 3),
    (b"LPOS", 2, 6), (b"LMOVE", 4, 4), (b"LMPOP", 2, 4),
    (b"LPUSHX", 2, 5), (b"RPUSHX", 2, 5), (b"RPOPLPUSH", 2, 2),
    (b"BLPOP", 2, 5), (b"BRPOP", 2, 5), (b"BLMOVE", 5, 5), (b"BLMPOP", 3, 5),
    # Hash
    (b"HSET", 3, 9), (b"HGET", 2, 2), (b"HDEL", 2, 5),
    (b"HEXISTS", 2, 2), (b"HLEN", 1, 1), (b"HKEYS", 1, 1),
    (b"HVALS", 1, 1), (b"HGETALL", 1, 1), (b"HINCRBY", 3, 3),
    (b"HINCRBYFLOAT", 3, 3), (b"HMSET", 3, 9), (b"HMGET", 2, 5),
    (b"HSETNX", 3, 3), (b"HSTRLEN", 2, 2), (b"HRANDFIELD", 1, 3),
    (b"HSCAN", 2, 6),
    # Set
    (b"SADD", 2, 5), (b"SREM", 2, 5), (b"SMEMBERS", 1, 1),
    (b"SISMEMBER", 2, 2), (b"SMISMEMBER", 2, 5), (b"SCARD", 1, 1),
    (b"SPOP", 1, 2), (b"SRANDMEMBER", 1, 2), (b"SMOVE", 3, 3),
    (b"SDIFF", 1, 3), (b"SINTER", 1, 3), (b"SUNION", 1, 3),
    (b"SDIFFSTORE", 2, 4), (b"SINTERSTORE", 2, 4), (b"SUNIONSTORE", 2, 4),
    (b"SINTERCARD", 2, 5), (b"SSCAN", 2, 6),
    # Sorted set
    (b"ZADD", 3, 9), (b"ZREM", 2, 5), (b"ZSCORE", 2, 2), (b"ZMSCORE", 2, 5),
    (b"ZRANK", 2, 2), (b"ZREVRANK", 2, 2), (b"ZCARD", 1, 1),
    (b"ZCOUNT", 3, 3), (b"ZLEXCOUNT", 3, 3),
    (b"ZRANGE", 3, 7), (b"ZRANGEBYLEX", 3, 7), (b"ZRANGEBYSCORE", 3, 7),
    (b"ZREVRANGE", 3, 5), (b"ZREVRANGEBYLEX", 3, 7), (b"ZREVRANGEBYSCORE", 3, 7),
    (b"ZRANGESTORE", 4, 8),
    (b"ZINCRBY", 3, 3), (b"ZRANDMEMBER", 1, 3),
    (b"ZPOPMIN", 1, 2), (b"ZPOPMAX", 1, 2),
    (b"BZPOPMIN", 2, 4), (b"BZPOPMAX", 2, 4),
    (b"ZDIFF", 2, 5), (b"ZDIFFSTORE", 3, 5),
    (b"ZMPOP", 2, 4), (b"BZMPOP", 3, 5),
    (b"ZREMRANGEBYRANK", 3, 3), (b"ZREMRANGEBYSCORE", 3, 3),
    (b"ZREMRANGEBYLEX", 3, 3),
    (b"ZSCAN", 2, 6),
    # Stream
    (b"XADD", 3, 9), (b"XLEN", 1, 1), (b"XRANGE", 3, 5),
    (b"XREVRANGE", 3, 5), (b"XREAD", 3, 7), (b"XTRIM", 2, 4),
    (b"XDEL", 2, 5), (b"XINFO", 2, 3), (b"XACK", 3, 5),
    (b"XGROUP", 3, 6), (b"XREADGROUP", 5, 9),
    (b"XAUTOCLAIM", 4, 6), (b"XCLAIM", 4, 8),
    # HyperLogLog
    (b"PFADD", 1, 5), (b"PFCOUNT", 1, 3), (b"PFMERGE", 2, 4),
    # Geo
    (b"GEOADD", 4, 10), (b"GEODIST", 3, 4), (b"GEOPOS", 2, 5),
    (b"GEOHASH", 2, 5), (b"GEOSEARCH", 4, 10), (b"GEOSEARCHSTORE", 5, 11),
    # Pub/Sub
    (b"SUBSCRIBE", 1, 3), (b"PUBLISH", 2, 2), (b"PSUBSCRIBE", 1, 3),
    # Transaction
    (b"MULTI", 0, 0), (b"EXEC", 0, 0), (b"DISCARD", 0, 0),
    (b"WATCH", 1, 3), (b"UNWATCH", 0, 0),
    # Script
    (b"EVAL", 2, 6), (b"EVALSHA", 2, 6), (b"EVALRO", 2, 6),
    # JSON
    (b"JSON.SET", 3, 4), (b"JSON.GET", 1, 4), (b"JSON.DEL", 1, 2),
    (b"JSON.TYPE", 1, 2), (b"JSON.NUMINCRBY", 3, 3),
    (b"JSON.ARRAPPEND", 3, 6), (b"JSON.ARRLEN", 1, 2),
    (b"JSON.ARRINSERT", 4, 6), (b"JSON.ARRTRIM", 4, 4),
    (b"JSON.ARRPOP", 1, 3), (b"JSON.ARRINDEX", 3, 5),
    (b"JSON.OBJKEYS", 1, 2), (b"JSON.OBJLEN", 1, 2),
    (b"JSON.STRAPPEND", 2, 3), (b"JSON.STRLEN", 1, 2),
    (b"JSON.TOGGLE", 2, 2), (b"JSON.CLEAR", 1, 2),
    (b"JSON.MERGE", 3, 3), (b"JSON.MGET", 2, 5),
    # Bloom filter
    (b"BF.ADD", 2, 2), (b"BF.EXISTS", 2, 2), (b"BF.MADD", 2, 5),
    (b"BF.MEXISTS", 2, 5), (b"BF.RESERVE", 3, 5),
    # Server
    (b"PING", 0, 1), (b"ECHO", 1, 1), (b"SELECT", 1, 1),
    (b"DBSIZE", 0, 0), (b"INFO", 0, 1),
    (b"CONFIG", 2, 3), (b"CLIENT", 1, 3), (b"COMMAND", 0, 2),
    (b"MEMORY", 1, 2), (b"ACL", 1, 5),
    (b"MONITOR", 0, 0), (b"RESET", 0, 0), (b"HELLO", 0, 5),
    (b"WAIT", 2, 2), (b"BGSAVE", 0, 1),
    (b"OBJECT", 2, 2), (b"LATENCY", 1, 2), (b"SLOWLOG", 1, 2),
    # Bitops
    (b"SETBIT", 3, 3), (b"GETBIT", 2, 2), (b"BITCOUNT", 1, 4),
    (b"BITOP", 3, 5), (b"BITPOS", 2, 5), (b"BITFIELD", 2, 8),
    # Search
    (b"FT.CREATE", 3, 15), (b"FT.SEARCH", 2, 10), (b"FT.DROPINDEX", 1, 2),
    (b"FT.INFO", 1, 1), (b"FT.ALTER", 3, 8),
    # Throttle
    (b"CL.THROTTLE", 5, 5),
]
# fmt: on

KEYS = [b"k", b"key", b"k1", b"k2", b"k3", b"src", b"dst", b"mylist", b"myset", b"myhash"]
VALUES = [b"v", b"val", b"hello", b"0", b"1", b"-1", b"100", b"3.14", b"", b"a b"]
SPECIAL = [b"*", b"?", b"[", b"NX", b"XX", b"EX", b"PX", b"GT", b"LT", b"KEEPTTL"]
JSON_VALUES = [b'{"a":1}', b"[1,2,3]", b'"str"', b"42", b"null", b"true"]
JSON_PATHS = [b"$", b"$.a", b"$.*", b"$.arr[0]", b"."]
SCORE_VALUES = [b"0", b"1", b"-inf", b"+inf", b"(1", b"(5", b"3.14"]
STREAM_IDS = [b"*", b"0-0", b"1-1", b"$", b">"]

# Fuzzy values: binary junk, edge cases
FUZZ_VALUES = [
    b"\x00",
    b"\xff" * 4,
    b"\r\n",
    b"$-1\r\n",
    b"*0\r\n",
    b"A" * 256,
    b"-1",
    b"99999999999",
    b"NaN",
    b"inf",
]

# Focus commands: when set via FUZZ_FOCUS_COMMANDS env var (JSON list of command names),
# the mutator will prefer these commands ~70% of the time. Used by PR fuzzing to
# concentrate mutations on commands affected by the code change.
_FOCUS_COMMANDS = []
_FOCUS_WEIGHT = 0.7

_focus_env = os.environ.get("FUZZ_FOCUS_COMMANDS", "")
if _focus_env:
    try:
        raw = json.loads(_focus_env)
        if isinstance(raw, str):
            raw = [raw]
        if isinstance(raw, list):
            _focus_names = {s.strip().upper() for s in raw if isinstance(s, str) and s.strip()}
        else:
            _focus_names = set()
        _FOCUS_COMMANDS = [c for c in COMMANDS if c[0].decode().upper() in _focus_names]
        # Add unknown commands (e.g. newly added in a PR) with default arity
        _known = {c[0].decode().upper() for c in COMMANDS}
        for name in _focus_names - _known:
            entry = (name.encode(), 1, 3)
            COMMANDS.append(entry)
            _FOCUS_COMMANDS.append(entry)
    except (json.JSONDecodeError, TypeError, ValueError):
        pass


def _pick_command():
    """Pick a command tuple, preferring focus commands when available."""
    if _FOCUS_COMMANDS and random.random() < _FOCUS_WEIGHT:
        return random.choice(_FOCUS_COMMANDS)
    return random.choice(COMMANDS)


def init(seed):
    random.seed(seed)


def _encode_resp(*args):
    """Encode a list of args into RESP array."""
    parts = [b"*%d\r\n" % len(args)]
    for a in args:
        if not isinstance(a, bytes):
            a = str(a).encode()
        parts.append(b"$%d\r\n%s\r\n" % (len(a), a))
    return b"".join(parts)


def _random_arg():
    """Generate a random argument value."""
    r = random.random()
    if r < 0.3:
        return random.choice(KEYS)
    if r < 0.55:
        return random.choice(VALUES)
    if r < 0.7:
        return random.choice(SPECIAL)
    if r < 0.8:
        return random.choice(FUZZ_VALUES)
    if r < 0.85:
        return random.choice(JSON_VALUES)
    if r < 0.9:
        return random.choice(JSON_PATHS)
    if r < 0.95:
        return random.choice(SCORE_VALUES)
    return random.choice(STREAM_IDS)


def _random_command():
    """Generate a single random RESP command."""
    cmd_name, min_args, max_args = _pick_command()
    nargs = random.randint(min_args, max_args)
    args = [cmd_name] + [_random_arg() for _ in range(nargs)]
    return _encode_resp(*args)


def _parse_resp_commands(buf):
    """Best-effort parse of RESP buffer into list of commands (each is list of bytes).
    Returns (commands, success). On parse failure returns ([], False)."""
    commands = []
    pos = 0
    data = bytes(buf)

    while pos < len(data):
        # Skip whitespace/newlines
        while pos < len(data) and data[pos : pos + 1] in (b"\r", b"\n", b" "):
            pos += 1
        if pos >= len(data):
            break

        if data[pos : pos + 1] != b"*":
            return ([], False)

        # Parse *N\r\n
        end = data.find(b"\r\n", pos)
        if end < 0:
            return ([], False)
        try:
            nargs = int(data[pos + 1 : end])
        except ValueError:
            return ([], False)
        pos = end + 2

        args = []
        for _ in range(nargs):
            if pos >= len(data) or data[pos : pos + 1] != b"$":
                return ([], False)
            end = data.find(b"\r\n", pos)
            if end < 0:
                return ([], False)
            try:
                slen = int(data[pos + 1 : end])
            except ValueError:
                return ([], False)
            pos = end + 2
            if slen < 0:
                args.append(b"")
                continue
            if pos + slen + 2 > len(data):
                return ([], False)
            args.append(data[pos : pos + slen])
            pos += slen + 2

        if args:
            commands.append(args)

    return (commands, True)


def _mutate_commands(commands):
    """Apply random mutations to a list of parsed commands."""
    result = list(commands)

    mutation = random.random()

    if mutation < 0.2 and len(result) > 0:
        # Replace a random command entirely
        idx = random.randint(0, len(result) - 1)
        cmd_name, min_args, max_args = _pick_command()
        nargs = random.randint(min_args, max_args)
        result[idx] = [cmd_name] + [_random_arg() for _ in range(nargs)]

    elif mutation < 0.4 and len(result) > 0:
        # Mutate an argument of a random command
        idx = random.randint(0, len(result) - 1)
        cmd = list(result[idx])
        if len(cmd) > 1:
            arg_idx = random.randint(1, len(cmd) - 1)
            cmd[arg_idx] = _random_arg()
            result[idx] = cmd

    elif mutation < 0.55:
        # Insert a new random command
        pos = random.randint(0, len(result))
        cmd_name, min_args, max_args = _pick_command()
        nargs = random.randint(min_args, max_args)
        result.insert(pos, [cmd_name] + [_random_arg() for _ in range(nargs)])

    elif mutation < 0.65 and len(result) > 1:
        # Remove a random command
        idx = random.randint(0, len(result) - 1)
        result.pop(idx)

    elif mutation < 0.75 and len(result) >= 2:
        # Swap two commands
        i, j = random.sample(range(len(result)), 2)
        result[i], result[j] = result[j], result[i]

    elif mutation < 0.85 and len(result) > 0:
        # Duplicate a command
        idx = random.randint(0, len(result) - 1)
        result.insert(idx + 1, list(result[idx]))

    elif mutation < 0.92 and len(result) > 0:
        # Wrap some commands in MULTI/EXEC
        start = random.randint(0, len(result) - 1)
        end = random.randint(start + 1, min(start + 5, len(result)))
        result.insert(start, [b"MULTI"])
        result.insert(end + 1, [b"EXEC"])

    else:
        # Add extra argument to a random command
        if len(result) > 0:
            idx = random.randint(0, len(result) - 1)
            result[idx] = list(result[idx]) + [_random_arg()]

    return result


def _commands_to_resp(commands):
    """Serialize list of commands back to RESP bytes."""
    parts = []
    for cmd in commands:
        parts.append(_encode_resp(*cmd))
    return b"".join(parts)


def fuzz(buf, add_buf, max_size):
    """Main mutation function called by AFL++."""
    # Try to parse the input as RESP
    commands, ok = _parse_resp_commands(buf)

    if ok and commands:
        # Parsed successfully — mutate at command level
        mutated = _mutate_commands(commands)
        result = _commands_to_resp(mutated)
    else:
        # Could not parse — generate random commands from scratch
        n = random.randint(1, 5)
        result = b"".join(_random_command() for _ in range(n))

    if len(result) > max_size:
        result = result[:max_size]

    return bytearray(result)


def havoc_mutation(buf, max_size):
    """Called during havoc stage — single small mutation."""
    commands, ok = _parse_resp_commands(buf)
    if not ok or not commands:
        return bytearray(_random_command()[:max_size])

    # Single small mutation
    mutated = _mutate_commands(commands)
    result = _commands_to_resp(mutated)
    if len(result) > max_size:
        result = result[:max_size]
    return bytearray(result)


def havoc_mutation_probability():
    """How often our havoc_mutation is called vs AFL++'s built-in mutations."""
    return 50
