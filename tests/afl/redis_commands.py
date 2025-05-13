#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import random
import os
import string

# Constants
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DICT_FILE = os.path.join(SCRIPT_DIR, "redis.dict")
INPUT_DIR = os.path.join(SCRIPT_DIR, "input")
COMMANDS_LOG_FILE = os.path.join(SCRIPT_DIR, "commands.log")
CRASH_LOG_FILE = os.path.join(SCRIPT_DIR, "crash_commands.log")

# Redis connection defaults (from environment variables)
REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# Command sources
COMMAND_SOURCES = ["REDIS", "VALKEY", "DRAGONFLY"]

# Special characters to include in fuzzing
SPECIAL_CHARS = "!@#$%^&*()-_=+[]{}|;:'\",.<>/?\\"
ESCAPED_CHARS = [r"\\", r"\n", r"\r", r"\t", r"\"", r"\'", r"\0", r"\a", r"\b", r"\f", r"\v"]

# Mix ratio between dictionary values and generated values (0-1)
# 0: only generated values, 1: only dictionary values when available
DICT_MIX_RATIO = 0.7

# List of supported Redis commands with parameters
REDIS_COMMANDS = {
    # General commands
    "PING": {"args": [], "optional_args": []},
    "ECHO": {"args": ["message"], "optional_args": []},
    "INFO": {"args": [], "optional_args": ["section"]},
    "TIME": {"args": [], "optional_args": []},
    "QUIT": {"args": [], "optional_args": []},
    # Key operations commands
    "DEL": {"args": ["key"], "optional_args": ["key ..."]},
    "EXISTS": {"args": ["key"], "optional_args": ["key ..."]},
    "EXPIRE": {"args": ["key", "seconds"], "optional_args": ["NX|XX|GT|LT"]},
    "TTL": {"args": ["key"], "optional_args": []},
    "PERSIST": {"args": ["key"], "optional_args": []},
    "TYPE": {"args": ["key"], "optional_args": []},
    "RENAME": {"args": ["key", "newkey"], "optional_args": []},
    "RENAMENX": {"args": ["key", "newkey"], "optional_args": []},
    "KEYS": {"args": ["pattern"], "optional_args": []},
    "SCAN": {"args": ["cursor"], "optional_args": ["MATCH pattern", "COUNT count"]},
    # String operations commands
    "SET": {"args": ["key", "value"], "optional_args": ["EX seconds", "PX milliseconds", "NX|XX"]},
    "GET": {"args": ["key"], "optional_args": []},
    "MGET": {"args": ["key"], "optional_args": ["key ..."]},
    "MSET": {"args": ["key value"], "optional_args": ["key value ..."]},
    "INCR": {"args": ["key"], "optional_args": []},
    "INCRBY": {"args": ["key", "increment"], "optional_args": []},
    "DECR": {"args": ["key"], "optional_args": []},
    "DECRBY": {"args": ["key", "decrement"], "optional_args": []},
    "APPEND": {"args": ["key", "value"], "optional_args": []},
    "STRLEN": {"args": ["key"], "optional_args": []},
    "GETRANGE": {"args": ["key", "start", "end"], "optional_args": []},
    "SETRANGE": {"args": ["key", "offset", "value"], "optional_args": []},
    # List operations commands
    "LPUSH": {"args": ["key", "element"], "optional_args": ["element ..."]},
    "RPUSH": {"args": ["key", "element"], "optional_args": ["element ..."]},
    "LPOP": {"args": ["key"], "optional_args": ["count"]},
    "RPOP": {"args": ["key"], "optional_args": ["count"]},
    "LLEN": {"args": ["key"], "optional_args": []},
    "LRANGE": {"args": ["key", "start", "stop"], "optional_args": []},
    "LINDEX": {"args": ["key", "index"], "optional_args": []},
    "LSET": {"args": ["key", "index", "element"], "optional_args": []},
    "LTRIM": {"args": ["key", "start", "stop"], "optional_args": []},
    # Hash operations commands
    "HSET": {"args": ["key", "field", "value"], "optional_args": ["field value ..."]},
    "HSETNX": {"args": ["key", "field", "value"], "optional_args": []},
    "HGET": {"args": ["key", "field"], "optional_args": []},
    "HMGET": {"args": ["key", "field"], "optional_args": ["field ..."]},
    "HGETALL": {"args": ["key"], "optional_args": []},
    "HDEL": {"args": ["key", "field"], "optional_args": ["field ..."]},
    "HEXISTS": {"args": ["key", "field"], "optional_args": []},
    "HLEN": {"args": ["key"], "optional_args": []},
    "HKEYS": {"args": ["key"], "optional_args": []},
    "HVALS": {"args": ["key"], "optional_args": []},
    "HINCRBY": {"args": ["key", "field", "increment"], "optional_args": []},
    "HSCAN": {"args": ["key", "cursor"], "optional_args": ["MATCH pattern", "COUNT count"]},
    # Set operations commands
    "SADD": {"args": ["key", "member"], "optional_args": ["member ..."]},
    "SREM": {"args": ["key", "member"], "optional_args": ["member ..."]},
    "SISMEMBER": {"args": ["key", "member"], "optional_args": []},
    "SMEMBERS": {"args": ["key"], "optional_args": []},
    "SCARD": {"args": ["key"], "optional_args": []},
    "SPOP": {"args": ["key"], "optional_args": ["count"]},
    "SRANDMEMBER": {"args": ["key"], "optional_args": ["count"]},
    "SINTER": {"args": ["key"], "optional_args": ["key ..."]},
    "SUNION": {"args": ["key"], "optional_args": ["key ..."]},
    "SDIFF": {"args": ["key"], "optional_args": ["key ..."]},
    "SSCAN": {"args": ["key", "cursor"], "optional_args": ["MATCH pattern", "COUNT count"]},
    # Sorted set operations commands
    "ZADD": {"args": ["key", "score", "member"], "optional_args": ["NX|XX", "score member ..."]},
    "ZREM": {"args": ["key", "member"], "optional_args": ["member ..."]},
    "ZRANGE": {
        "args": ["key", "start", "stop"],
        "optional_args": ["WITHSCORES", "REV", "BYSCORE", "BYLEX", "LIMIT offset count"],
    },
    "ZCARD": {"args": ["key"], "optional_args": []},
    "ZSCORE": {"args": ["key", "member"], "optional_args": []},
    "ZRANK": {"args": ["key", "member"], "optional_args": []},
    "ZINCRBY": {"args": ["key", "increment", "member"], "optional_args": []},
    "ZCOUNT": {"args": ["key", "min", "max"], "optional_args": []},
    "ZSCAN": {"args": ["key", "cursor"], "optional_args": ["MATCH pattern", "COUNT count"]},
    # Pub/Sub commands
    "PUBLISH": {"args": ["channel", "message"], "optional_args": []},
    "SUBSCRIBE": {"args": ["channel"], "optional_args": ["channel ..."]},
    "UNSUBSCRIBE": {"args": ["channel"], "optional_args": ["channel ..."]},
    "PSUBSCRIBE": {"args": ["pattern"], "optional_args": ["pattern ..."]},
    "PUNSUBSCRIBE": {"args": ["pattern"], "optional_args": ["pattern ..."]},
    "PUBSUB": {"args": ["subcommand"], "optional_args": ["argument ..."]},
}

# Additional Valkey/Redis commands based on their documentation
ADDITIONAL_COMMANDS = {
    # Transaction commands
    "MULTI": {"args": [], "optional_args": []},
    "EXEC": {"args": [], "optional_args": []},
    "DISCARD": {"args": [], "optional_args": []},
    "WATCH": {"args": ["key"], "optional_args": ["key ..."]},
    "UNWATCH": {"args": [], "optional_args": []},
    # Scripting
    "EVAL": {"args": ["script", "numkeys", "key"], "optional_args": ["key ...", "arg", "arg ..."]},
    "EVALSHA": {"args": ["sha1", "numkeys", "key"], "optional_args": ["key ...", "arg", "arg ..."]},
    "SCRIPT": {"args": ["subcommand"], "optional_args": ["arg", "arg ..."]},
    # Connection
    "AUTH": {"args": ["password"], "optional_args": ["username"]},
    "SELECT": {"args": ["index"], "optional_args": []},
    "CLIENT": {"args": ["subcommand"], "optional_args": ["arg", "arg ..."]},
    # Server
    "FLUSHDB": {"args": [], "optional_args": ["ASYNC", "SYNC"]},
    "FLUSHALL": {"args": [], "optional_args": ["ASYNC", "SYNC"]},
    "DBSIZE": {"args": [], "optional_args": []},
    "CONFIG": {"args": ["GET", "pattern"], "optional_args": []},
    "MONITOR": {"args": [], "optional_args": []},
    "DEBUG": {"args": ["subcommand"], "optional_args": ["arg", "arg ..."]},
    # Bitmap operations
    "BITCOUNT": {"args": ["key"], "optional_args": ["start", "end", "BYTE|BIT"]},
    "BITOP": {"args": ["operation", "destkey", "key"], "optional_args": ["key ..."]},
    "BITPOS": {"args": ["key", "bit"], "optional_args": ["start", "end", "BYTE|BIT"]},
    "GETBIT": {"args": ["key", "offset"], "optional_args": []},
    "SETBIT": {"args": ["key", "offset", "value"], "optional_args": []},
    # HyperLogLog
    "PFADD": {"args": ["key", "element"], "optional_args": ["element ..."]},
    "PFCOUNT": {"args": ["key"], "optional_args": ["key ..."]},
    "PFMERGE": {"args": ["destkey", "sourcekey"], "optional_args": ["sourcekey ..."]},
    # GEO commands
    "GEOADD": {
        "args": ["key", "longitude", "latitude", "member"],
        "optional_args": ["longitude latitude member ..."],
    },
    "GEODIST": {"args": ["key", "member1", "member2"], "optional_args": ["unit"]},
    "GEOHASH": {"args": ["key", "member"], "optional_args": ["member ..."]},
    "GEOPOS": {"args": ["key", "member"], "optional_args": ["member ..."]},
    "GEORADIUS": {
        "args": ["key", "longitude", "latitude", "radius", "unit"],
        "optional_args": [
            "WITHDIST",
            "WITHCOORD",
            "WITHHASH",
            "COUNT",
            "count",
            "ASC|DESC",
            "STORE",
            "key",
            "STOREDIST",
            "key",
        ],
    },
    # Streams
    "XADD": {"args": ["key", "ID", "field", "value"], "optional_args": ["field value ..."]},
    "XRANGE": {"args": ["key", "start", "end"], "optional_args": ["COUNT", "count"]},
    "XREVRANGE": {"args": ["key", "end", "start"], "optional_args": ["COUNT", "count"]},
    "XLEN": {"args": ["key"], "optional_args": []},
    "XREAD": {
        "args": ["STREAMS", "key", "id"],
        "optional_args": ["key id ...", "COUNT", "count", "BLOCK", "milliseconds"],
    },
    # DragonFly specific commands
    "DF.STATS": {"args": [], "optional_args": []},
    "DF.INFO": {"args": [], "optional_args": []},
    "MEMORY": {"args": ["USAGE", "key"], "optional_args": ["SAMPLES", "count"]},
    "CAS": {"args": ["key", "oldval", "newval"], "optional_args": []},
}

# Merge commands
REDIS_COMMANDS.update(ADDITIONAL_COMMANDS)

# Data types for random value generation
DATA_TYPES = {
    "string": lambda: "".join(
        random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")
        for _ in range(random.randint(1, 20))
    ),
    "integer": lambda: str(random.randint(-1000000, 1000000)),
    "float": lambda: str(random.uniform(-1000000, 1000000)),
    "key": lambda: "key:"
    + "".join(
        random.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(random.randint(1, 10))
    ),
    "field": lambda: "field:"
    + "".join(
        random.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(random.randint(1, 10))
    ),
    "member": lambda: "member:"
    + "".join(
        random.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(random.randint(1, 10))
    ),
    "channel": lambda: "channel:"
    + "".join(
        random.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(random.randint(1, 10))
    ),
    "pattern": lambda: "*:"
    + "".join(
        random.choice("abcdefghijklmnopqrstuvwxyz0123456789*?[]")
        for _ in range(random.randint(1, 10))
    ),
    "score": lambda: str(random.uniform(-1000, 1000)),
    "index": lambda: str(random.randint(-100, 100)),
    "count": lambda: str(random.randint(1, 100)),
    "cursor": lambda: str(random.randint(0, 10000)),
    "increment": lambda: str(random.randint(-100, 100)),
    "decrement": lambda: str(random.randint(-100, 100)),
    "seconds": lambda: str(random.randint(1, 3600)),
    "milliseconds": lambda: str(random.randint(1, 3600000)),
    "offset": lambda: str(random.randint(0, 100)),
    "start": lambda: str(random.randint(-100, 100)),
    "end": lambda: str(random.randint(-100, 100)),
    "stop": lambda: str(random.randint(-100, 100)),
    "min": lambda: str(random.randint(-1000, 1000)),
    "max": lambda: str(random.randint(-1000, 1000)),
    "subcommand": lambda: random.choice(["CHANNELS", "NUMPAT", "NUMSUB"]),
    "section": lambda: random.choice(
        [
            "SERVER",
            "CLIENTS",
            "MEMORY",
            "PERSISTENCE",
            "STATS",
            "REPLICATION",
            "CPU",
            "COMMANDSTATS",
            "CLUSTER",
            "KEYSPACE",
        ]
    ),
    "message": lambda: "".join(
        random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")
        for _ in range(random.randint(1, 50))
    ),
    "value": lambda: "".join(
        random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")
        for _ in range(random.randint(1, 50))
    ),
    "element": lambda: "".join(
        random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")
        for _ in range(random.randint(1, 50))
    ),
    "script": lambda: "return {KEYS[1],ARGV[1]}",
    "numkeys": lambda: str(random.randint(0, 3)),
    "sha1": lambda: "".join(random.choice("0123456789abcdef") for _ in range(40)),
    "password": lambda: "".join(
        random.choice(string.ascii_letters + string.digits) for _ in range(random.randint(4, 12))
    ),
    "username": lambda: "".join(
        random.choice(string.ascii_letters) for _ in range(random.randint(3, 8))
    ),
    "longitude": lambda: str(random.uniform(-180, 180)),
    "latitude": lambda: str(random.uniform(-90, 90)),
    "radius": lambda: str(random.uniform(0, 100)),
    "unit": lambda: random.choice(["m", "km", "ft", "mi"]),
    "ID": lambda: f"{random.randint(0, 1000)}-{random.randint(0, 1000)}",
    "operation": lambda: random.choice(["AND", "OR", "XOR", "NOT"]),
    "destkey": lambda: "key",
    "sourcekey": lambda: "key",
    "arg": lambda: "string",
    "bit": lambda: random.choice(["0", "1"]),
}

# Mapping arguments to data types
ARG_TYPE_MAP = {
    "key": "key",
    "newkey": "key",
    "field": "field",
    "member": "member",
    "channel": "channel",
    "pattern": "pattern",
    "value": "value",
    "element": "element",
    "score": "score",
    "index": "index",
    "count": "count",
    "cursor": "cursor",
    "increment": "increment",
    "decrement": "decrement",
    "seconds": "seconds",
    "milliseconds": "milliseconds",
    "offset": "offset",
    "start": "start",
    "end": "end",
    "stop": "stop",
    "min": "min",
    "max": "max",
    "subcommand": "subcommand",
    "section": "section",
    "message": "message",
    "script": "script",
    "numkeys": "numkeys",
    "sha1": "sha1",
    "password": "password",
    "username": "username",
    "longitude": "longitude",
    "latitude": "latitude",
    "radius": "radius",
    "unit": "unit",
    "ID": "ID",
    "operation": "operation",
    "destkey": "key",
    "sourcekey": "key",
    "arg": "string",
    "bit": lambda: random.choice(["0", "1"]),
}


# Enhanced DATA_TYPES with special characters and escaped sequences
def enhance_data_types():
    global DATA_TYPES

    # Add functions to generate special characters and escaped strings
    DATA_TYPES.update(
        {
            "special_string": lambda: "".join(
                random.choice(string.ascii_letters + string.digits + SPECIAL_CHARS)
                for _ in range(random.randint(1, 20))
            ),
            "escaped_string": lambda: random.choice(ESCAPED_CHARS)
            + "".join(
                random.choice(string.ascii_letters + string.digits)
                for _ in range(random.randint(1, 10))
            ),
            "mixed_string": lambda: "".join(
                random.choice(
                    [
                        lambda: random.choice(string.ascii_letters + string.digits),
                        lambda: random.choice(SPECIAL_CHARS),
                        lambda: random.choice(ESCAPED_CHARS),
                    ]
                )()
                for _ in range(random.randint(5, 20))
            ),
            "binary_string": lambda: "\\x"
            + "".join(format(random.randint(0, 255), "02x") for _ in range(random.randint(1, 10))),
        }
    )

    # Create enhanced versions of existing types
    enhanced_types = {}
    for key, func in DATA_TYPES.items():
        if key.endswith("string") or key in [
            "value",
            "message",
            "element",
            "key",
            "field",
            "member",
            "pattern",
        ]:
            enhanced_types[f"special_{key}"] = lambda k=key: DATA_TYPES[k]() + random.choice(
                SPECIAL_CHARS
            )
            enhanced_types[f"escaped_{key}"] = lambda k=key: DATA_TYPES[k]() + random.choice(
                ESCAPED_CHARS
            )

    DATA_TYPES.update(enhanced_types)


# Function to load previous input cases as dictionary values
def load_input_dict():
    """Load all input files as dictionary values"""
    input_values = []
    if os.path.exists(INPUT_DIR):
        for filename in os.listdir(INPUT_DIR):
            if filename.endswith(".txt"):
                try:
                    with open(os.path.join(INPUT_DIR, filename), "r") as f:
                        for line in f:
                            line = line.strip()
                            if line:
                                parts = line.split(" ", 1)
                                if len(parts) > 1:
                                    input_values.append(parts[1])
                except Exception as e:
                    print(f"Error loading input file {filename}: {e}")
    return input_values
