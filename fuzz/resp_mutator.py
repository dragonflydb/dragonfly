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

# fmt: off
# Every command registered in Dragonfly (via CI{...}), grouped by family: (name, min_args, max_args).
# min/max are argument counts AFTER the command name. min_args satisfies the registered arity;
# max_args covers the documented option grammar (variadic lists capped to a practical bound).
# Admin/replication/cluster commands (REPLICAOF, CLUSTER, DEBUG, FLUSHALL, ...) are intentionally excluded.
COMMANDS = [
    # String
    (b"GET", 1, 1), (b"SET", 2, 8), (b"SETEX", 3, 3), (b"PSETEX", 3, 3),
    (b"SETNX", 2, 2), (b"APPEND", 2, 2), (b"PREPEND", 2, 2), (b"INCR", 1, 1),
    (b"DECR", 1, 1), (b"INCRBY", 2, 2), (b"DECRBY", 2, 2), (b"INCRBYFLOAT", 2, 2),
    (b"GETDEL", 1, 1), (b"GETEX", 1, 4), (b"GETSET", 2, 2), (b"MGET", 1, 6),
    (b"MSET", 2, 8), (b"MSETNX", 2, 8), (b"STRLEN", 1, 1), (b"GETRANGE", 3, 3),
    (b"SUBSTR", 3, 3), (b"SETRANGE", 3, 3), (b"DIGEST", 1, 1),
    # Key / generic
    (b"DEL", 1, 6), (b"UNLINK", 1, 6), (b"EXISTS", 1, 6), (b"EXPIRE", 2, 4),
    (b"EXPIREAT", 2, 4), (b"PEXPIRE", 2, 4), (b"PEXPIREAT", 2, 4), (b"EXPIRETIME", 1, 1),
    (b"PEXPIRETIME", 1, 1), (b"PERSIST", 1, 1), (b"TTL", 1, 1), (b"PTTL", 1, 1),
    (b"TYPE", 1, 1), (b"RENAME", 2, 2), (b"RENAMENX", 2, 2), (b"COPY", 2, 5),
    (b"DUMP", 1, 1), (b"RESTORE", 3, 7), (b"TOUCH", 1, 6), (b"RANDOMKEY", 0, 0),
    (b"KEYS", 1, 1), (b"SCAN", 1, 8), (b"SORT", 1, 10), (b"SORT_RO", 1, 9),
    (b"MOVE", 2, 2), (b"STICK", 1, 6), (b"DELEX", 1, 3), (b"FIELDEXPIRE", 3, 8),
    (b"FIELDTTL", 2, 2),
    # List
    (b"LPUSH", 2, 6), (b"RPUSH", 2, 6), (b"LPUSHX", 2, 6), (b"RPUSHX", 2, 6),
    (b"LPOP", 1, 2), (b"RPOP", 1, 2), (b"LLEN", 1, 1), (b"LINDEX", 2, 2),
    (b"LSET", 3, 3), (b"LRANGE", 3, 3), (b"LTRIM", 3, 3), (b"LREM", 3, 3),
    (b"LPOS", 2, 8), (b"LMOVE", 4, 4), (b"LMPOP", 3, 6), (b"LINSERT", 4, 4),
    (b"RPOPLPUSH", 2, 2), (b"BLPOP", 2, 5), (b"BRPOP", 2, 5), (b"BLMOVE", 5, 5),
    (b"BLMPOP", 4, 7), (b"BRPOPLPUSH", 3, 3),
    # Hash
    (b"HSET", 3, 9), (b"HSETNX", 3, 3), (b"HGET", 2, 2), (b"HMGET", 2, 6),
    (b"HMSET", 3, 9), (b"HDEL", 2, 6), (b"HLEN", 1, 1), (b"HEXISTS", 2, 2),
    (b"HKEYS", 1, 1), (b"HVALS", 1, 1), (b"HGETALL", 1, 1), (b"HINCRBY", 3, 3),
    (b"HINCRBYFLOAT", 3, 3), (b"HSTRLEN", 2, 2), (b"HRANDFIELD", 1, 3), (b"HSCAN", 2, 7),
    (b"HEXPIRE", 4, 9), (b"HTTL", 3, 8), (b"HSETEX", 4, 9), (b"HPEXPIRETIME", 3, 8),
    (b"HGETEX", 3, 9),
    # Set
    (b"SADD", 2, 6), (b"SREM", 2, 6), (b"SMEMBERS", 1, 1), (b"SISMEMBER", 2, 2),
    (b"SMISMEMBER", 2, 6), (b"SCARD", 1, 1), (b"SPOP", 1, 2), (b"SRANDMEMBER", 1, 2),
    (b"SMOVE", 3, 3), (b"SDIFF", 1, 4), (b"SINTER", 1, 4), (b"SUNION", 1, 4),
    (b"SDIFFSTORE", 2, 5), (b"SINTERSTORE", 2, 5), (b"SUNIONSTORE", 2, 5), (b"SINTERCARD", 2, 6),
    (b"SSCAN", 2, 6), (b"SADDEX", 3, 7),
    # Sorted set
    (b"ZADD", 3, 9), (b"ZREM", 2, 6), (b"ZSCORE", 2, 2), (b"ZMSCORE", 2, 6),
    (b"ZRANK", 2, 3), (b"ZREVRANK", 2, 3), (b"ZCARD", 1, 1), (b"ZCOUNT", 3, 3),
    (b"ZLEXCOUNT", 3, 3), (b"ZRANGE", 3, 9), (b"ZRANGEBYLEX", 3, 7), (b"ZRANGEBYSCORE", 3, 8),
    (b"ZREVRANGE", 3, 5), (b"ZREVRANGEBYLEX", 3, 7), (b"ZREVRANGEBYSCORE", 3, 8), (b"ZRANGESTORE", 4, 9),
    (b"ZINCRBY", 3, 3), (b"ZRANDMEMBER", 1, 3), (b"ZPOPMIN", 1, 2), (b"ZPOPMAX", 1, 2),
    (b"ZDIFF", 2, 4), (b"ZDIFFSTORE", 3, 5), (b"ZINTER", 2, 8), (b"ZUNION", 2, 8),
    (b"ZINTERSTORE", 3, 8), (b"ZUNIONSTORE", 3, 8), (b"ZINTERCARD", 2, 5), (b"ZMPOP", 3, 6),
    (b"ZREMRANGEBYRANK", 3, 3), (b"ZREMRANGEBYSCORE", 3, 3), (b"ZREMRANGEBYLEX", 3, 3), (b"ZSCAN", 2, 6),
    (b"BZPOPMIN", 2, 5), (b"BZPOPMAX", 2, 5), (b"BZMPOP", 4, 7),
    # Stream
    (b"XADD", 4, 10), (b"XLEN", 1, 1), (b"XRANGE", 3, 5), (b"XREVRANGE", 3, 5),
    (b"XREAD", 3, 9), (b"XREADGROUP", 5, 11), (b"XTRIM", 3, 6), (b"XDEL", 2, 6),
    (b"XINFO", 1, 3), (b"XACK", 3, 6), (b"XGROUP", 2, 6), (b"XAUTOCLAIM", 5, 7),
    (b"XCLAIM", 5, 10), (b"XPENDING", 2, 6), (b"XSETID", 2, 4),
    # HyperLogLog
    (b"PFADD", 2, 6), (b"PFCOUNT", 1, 4), (b"PFMERGE", 2, 5),
    # Geo
    (b"GEOADD", 4, 10), (b"GEOHASH", 2, 6), (b"GEOPOS", 2, 6), (b"GEODIST", 3, 4),
    (b"GEOSEARCH", 6, 12), (b"GEORADIUS", 5, 12), (b"GEORADIUS_RO", 5, 10), (b"GEORADIUSBYMEMBER", 4, 11),
    (b"GEORADIUSBYMEMBER_RO", 4, 9),
    # Bitops
    (b"SETBIT", 3, 3), (b"GETBIT", 2, 2), (b"BITCOUNT", 1, 4), (b"BITOP", 3, 6),
    (b"BITPOS", 2, 5), (b"BITFIELD", 1, 10), (b"BITFIELD_RO", 1, 7),
    # Pub/Sub
    (b"SUBSCRIBE", 1, 4), (b"UNSUBSCRIBE", 0, 4), (b"PSUBSCRIBE", 1, 4), (b"PUNSUBSCRIBE", 0, 4),
    (b"SSUBSCRIBE", 1, 4), (b"SUNSUBSCRIBE", 0, 4), (b"PUBLISH", 2, 2), (b"SPUBLISH", 2, 2),
    (b"PUBSUB", 0, 3),
    # Transaction
    (b"MULTI", 0, 0), (b"EXEC", 0, 0), (b"DISCARD", 0, 0), (b"WATCH", 1, 4),
    (b"UNWATCH", 0, 0),
    # Scripting
    (b"EVAL", 2, 6), (b"EVAL_RO", 2, 6), (b"EVALSHA", 2, 6), (b"EVALSHA_RO", 2, 6),
    (b"SCRIPT", 1, 3), (b"FUNCTION", 1, 1),
    # JSON
    (b"JSON.SET", 3, 4), (b"JSON.GET", 1, 5), (b"JSON.DEL", 1, 2), (b"JSON.FORGET", 1, 2),
    (b"JSON.TYPE", 1, 2), (b"JSON.NUMINCRBY", 3, 3), (b"JSON.NUMMULTBY", 3, 3), (b"JSON.ARRAPPEND", 3, 6),
    (b"JSON.ARRLEN", 1, 2), (b"JSON.ARRINSERT", 4, 6), (b"JSON.ARRTRIM", 4, 4), (b"JSON.ARRPOP", 1, 3),
    (b"JSON.ARRINDEX", 3, 5), (b"JSON.OBJKEYS", 1, 2), (b"JSON.OBJLEN", 1, 2), (b"JSON.STRAPPEND", 3, 3),
    (b"JSON.STRLEN", 1, 2), (b"JSON.TOGGLE", 2, 2), (b"JSON.CLEAR", 1, 2), (b"JSON.MERGE", 3, 3),
    (b"JSON.MGET", 2, 6), (b"JSON.MSET", 3, 9), (b"JSON.RESP", 1, 2), (b"JSON.DEBUG", 1, 3),
    # Bloom filter
    (b"BF.ADD", 2, 2), (b"BF.EXISTS", 2, 2), (b"BF.MADD", 2, 6), (b"BF.MEXISTS", 2, 6),
    (b"BF.RESERVE", 3, 5), (b"BF.SCANDUMP", 2, 2), (b"BF.LOADCHUNK", 3, 3),
    # Cuckoo filter
    (b"CF.RESERVE", 2, 8), (b"CF.ADD", 2, 2), (b"CF.ADDNX", 2, 2), (b"CF.EXISTS", 2, 2),
    (b"CF.MEXISTS", 2, 6), (b"CF.INFO", 1, 1), (b"CF.COUNT", 2, 2), (b"CF.DEL", 2, 2),
    (b"CF.INSERT", 3, 8), (b"CF.INSERTNX", 3, 8), (b"CF.COMPACT", 1, 1),
    # Count-Min Sketch
    (b"CMS.INITBYDIM", 3, 3), (b"CMS.INITBYPROB", 3, 3), (b"CMS.INCRBY", 3, 7), (b"CMS.QUERY", 2, 6),
    (b"CMS.INFO", 1, 1), (b"CMS.MERGE", 3, 7),
    # Top-K
    (b"TOPK.RESERVE", 2, 5), (b"TOPK.ADD", 2, 6), (b"TOPK.INCRBY", 3, 7), (b"TOPK.QUERY", 2, 6),
    (b"TOPK.COUNT", 2, 6), (b"TOPK.LIST", 1, 2), (b"TOPK.INFO", 1, 1),
    # Search
    (b"FT.CREATE", 3, 15), (b"FT.SEARCH", 2, 12), (b"FT.AGGREGATE", 2, 12), (b"FT.PROFILE", 3, 12),
    (b"FT.DROPINDEX", 1, 2), (b"FT.INFO", 1, 1), (b"FT.ALTER", 3, 8), (b"FT.TAGVALS", 2, 2),
    (b"FT.SYNDUMP", 1, 1), (b"FT.SYNUPDATE", 3, 6), (b"FT.CONFIG", 2, 4), (b"FT._LIST", 0, 0),
    (b"FT._DEBUG", 0, 4), (b"FT.HYBRID", 2, 12),
    # Server / introspection
    (b"PING", 0, 1), (b"ECHO", 1, 1), (b"SELECT", 1, 1), (b"DBSIZE", 0, 0),
    (b"INFO", 0, 1), (b"CONFIG", 1, 3), (b"CLIENT", 1, 4), (b"COMMAND", 0, 3),
    (b"MEMORY", 1, 3), (b"ACL", 1, 5), (b"MONITOR", 0, 0), (b"HELLO", 0, 5),
    (b"BGSAVE", 0, 4), (b"SAVE", 0, 3), (b"LATENCY", 1, 2), (b"SLOWLOG", 1, 2),
    (b"SHRINK", 1, 1), (b"QUIT", 0, 0), (b"TIME", 0, 0), (b"WAIT", 2, 2),
    # Throttle
    (b"CL.THROTTLE", 4, 5),
]
# fmt: on

KEYS = [b"k", b"key", b"k1", b"k2", b"k3", b"src", b"dst", b"mylist", b"myset", b"myhash"]
VALUES = [b"v", b"val", b"hello", b"0", b"1", b"-1", b"100", b"3.14", b"", b"a b"]
# Option keyword tokens parsed across the command grammar — feeding these as
# arguments steers mutations into option-parsing branches instead of dead-ending
# on unknown tokens. Grouped roughly by area but mixed uniformly when chosen.
SPECIAL = [
    b"*",
    b"?",
    b"[",
    # set / expire conditionals
    b"NX",
    b"XX",
    b"GT",
    b"LT",
    b"CH",
    b"INCR",
    b"KEEPTTL",
    b"PERSIST",
    b"GET",
    b"EX",
    b"PX",
    b"EXAT",
    b"PXAT",
    # range / sort / scan options
    b"WITHSCORES",
    b"WITHVALUES",
    b"WITHSCORE",
    b"LIMIT",
    b"COUNT",
    b"MATCH",
    b"TYPE",
    b"BYSCORE",
    b"BYLEX",
    b"REV",
    b"ASC",
    b"DESC",
    b"ALPHA",
    b"BY",
    b"STORE",
    b"NOVALUES",
    b"RANK",
    b"MAXLEN",
    b"MINID",
    b"ANY",
    # aggregate / store / pop direction
    b"WEIGHTS",
    b"AGGREGATE",
    b"SUM",
    b"MIN",
    b"MAX",
    b"LEFT",
    b"RIGHT",
    # stream / hash-field / geo
    b"STREAMS",
    b"GROUP",
    b"BLOCK",
    b"NOACK",
    b"FIELDS",
    b"JUSTID",
    b"IDLE",
    b"FROMMEMBER",
    b"FROMLONLAT",
    b"BYRADIUS",
    b"BYBOX",
    b"WITHCOORD",
    b"WITHDIST",
    b"m",
    b"km",
    # bitfield / restore / misc
    b"OVERFLOW",
    b"WRAP",
    b"SAT",
    b"FAIL",
    b"REPLACE",
    b"ABSTTL",
    b"u8",
    b"i64",
    # cuckoo filter options
    b"NOCREATE",
    b"ITEMS",
    b"BUCKETSIZE",
    b"MAXITERATIONS",
    b"EXPANSION",
    b"CAPACITY",
    # topk options
    b"WITHCOUNT",
]
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
