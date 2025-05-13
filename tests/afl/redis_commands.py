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

# Special characters to include in fuzzing
SPECIAL_CHARS = "!@#$%^&*()-_=+[]{}|;:'\",.<>/?\\"
ESCAPED_CHARS = [r"\\", r"\n", r"\r", r"\t", r"\"", r"\'", r"\0", r"\a", r"\b", r"\f", r"\v"]

# Mix ratio between dictionary values and generated values (0-1)
# 0: only generated values, 1: only dictionary values when available
DICT_MIX_RATIO = 0.9

# List of supported Redis commands with parameters
REDIS_COMMANDS = {
    # ACL commands
    "ACL CAT": {"args": [], "optional_args": ["categoryname"]},
    "ACL DELUSER": {"args": ["username"], "optional_args": ["username ..."]},
    "ACL DRYRUN": {"args": ["username", "command"], "optional_args": ["arg", "arg ..."]},
    "ACL GENPASS": {"args": [], "optional_args": ["bits"]},
    "ACL GETUSER": {"args": ["username"], "optional_args": []},
    "ACL LIST": {"args": [], "optional_args": []},
    "ACL LOAD": {"args": [], "optional_args": []},
    "ACL LOG": {"args": [], "optional_args": ["count", "RESET"]},
    "ACL SAVE": {"args": [], "optional_args": []},
    "ACL SETUSER": {"args": ["username"], "optional_args": ["rule", "rule ..."]},
    "ACL USERS": {"args": [], "optional_args": []},
    "ACL WHOAMI": {"args": [], "optional_args": []},
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
    # Додаткові команди для Sorted Sets
    "ZDIFF": {"args": ["numkeys", "key"], "optional_args": ["key ...", "WITHSCORES"]},
    "ZDIFFSTORE": {"args": ["destination", "numkeys", "key"], "optional_args": ["key ..."]},
    "ZINTER": {
        "args": ["numkeys", "key"],
        "optional_args": ["key ...", "WEIGHTS", "weight", "AGGREGATE", "SUM|MIN|MAX", "WITHSCORES"],
    },
    "ZINTERCARD": {"args": ["numkeys", "key"], "optional_args": ["key ...", "LIMIT", "limit"]},
    "ZINTERSTORE": {
        "args": ["destination", "numkeys", "key"],
        "optional_args": ["key ...", "WEIGHTS", "weight", "AGGREGATE", "SUM|MIN|MAX"],
    },
    "ZLEXCOUNT": {"args": ["key", "min", "max"], "optional_args": []},
    "ZMPOP": {
        "args": ["numkeys", "key", "MIN|MAX"],
        "optional_args": ["key ...", "COUNT", "count"],
    },
    "ZMSCORE": {"args": ["key", "member"], "optional_args": ["member ..."]},
    "ZPOPMAX": {"args": ["key"], "optional_args": ["count"]},
    "ZPOPMIN": {"args": ["key"], "optional_args": ["count"]},
    "ZRANDMEMBER": {"args": ["key"], "optional_args": ["count", "WITHSCORES"]},
    "ZRANGEBYLEX": {"args": ["key", "min", "max"], "optional_args": ["LIMIT", "offset", "count"]},
    "ZRANGEBYSCORE": {
        "args": ["key", "min", "max"],
        "optional_args": ["WITHSCORES", "LIMIT", "offset", "count"],
    },
    "ZRANGESTORE": {
        "args": ["dst", "src", "min", "max"],
        "optional_args": ["BYSCORE", "BYLEX", "REV", "LIMIT", "offset", "count"],
    },
    "ZREVRANGE": {"args": ["key", "start", "stop"], "optional_args": ["WITHSCORES"]},
    "ZREVRANGEBYLEX": {
        "args": ["key", "max", "min"],
        "optional_args": ["LIMIT", "offset", "count"],
    },
    "ZREVRANGEBYSCORE": {
        "args": ["key", "max", "min"],
        "optional_args": ["WITHSCORES", "LIMIT", "offset", "count"],
    },
    "ZREVRANK": {"args": ["key", "member"], "optional_args": ["WITHSCORE"]},
    "ZREMRANGEBYLEX": {"args": ["key", "min", "max"], "optional_args": []},
    "ZREMRANGEBYRANK": {"args": ["key", "start", "stop"], "optional_args": []},
    "ZREMRANGEBYSCORE": {"args": ["key", "min", "max"], "optional_args": []},
    "ZUNION": {
        "args": ["numkeys", "key"],
        "optional_args": ["key ...", "WEIGHTS", "weight", "AGGREGATE", "SUM|MIN|MAX", "WITHSCORES"],
    },
    "ZUNIONSTORE": {
        "args": ["destination", "numkeys", "key"],
        "optional_args": ["key ...", "WEIGHTS", "weight", "AGGREGATE", "SUM|MIN|MAX"],
    },
    # Pub/Sub commands
    "PUBLISH": {"args": ["channel", "message"], "optional_args": []},
    "SUBSCRIBE": {"args": ["channel"], "optional_args": ["channel ..."]},
    "UNSUBSCRIBE": {"args": ["channel"], "optional_args": ["channel ..."]},
    "PSUBSCRIBE": {"args": ["pattern"], "optional_args": ["pattern ..."]},
    "PUNSUBSCRIBE": {"args": ["pattern"], "optional_args": ["pattern ..."]},
    "PUBSUB": {"args": ["subcommand"], "optional_args": ["argument ..."]},
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
    # Stream commands
    "XACK": {"args": ["key", "group", "ID"], "optional_args": ["ID ..."]},
    "XAUTOCLAIM": {
        "args": ["key", "group", "consumer", "min-idle-time", "start"],
        "optional_args": ["COUNT", "count", "JUSTID"],
    },
    "XCLAIM": {
        "args": ["key", "group", "consumer", "min-idle-time", "ID"],
        "optional_args": [
            "ID ...",
            "IDLE",
            "ms",
            "TIME",
            "ms-unix-time",
            "RETRYCOUNT",
            "count",
            "FORCE",
            "JUSTID",
        ],
    },
    "XDEL": {"args": ["key", "ID"], "optional_args": ["ID ..."]},
    "XGROUP CREATE": {"args": ["key", "groupname", "ID"], "optional_args": ["MKSTREAM"]},
    "XGROUP CREATECONSUMER": {"args": ["key", "groupname", "consumername"], "optional_args": []},
    "XGROUP DELCONSUMER": {"args": ["key", "groupname", "consumername"], "optional_args": []},
    "XGROUP DESTROY": {"args": ["key", "groupname"], "optional_args": []},
    "XGROUP SETID": {"args": ["key", "groupname", "ID"], "optional_args": []},
    "XINFO CONSUMERS": {"args": ["key", "groupname"], "optional_args": []},
    "XINFO GROUPS": {"args": ["key"], "optional_args": []},
    "XINFO STREAM": {"args": ["key"], "optional_args": ["FULL", "COUNT", "count"]},
    "XPENDING": {"args": ["key", "group"], "optional_args": ["start", "end", "count", "consumer"]},
    "XREADGROUP": {
        "args": ["GROUP", "group", "consumer", "STREAMS", "key", "ID"],
        "optional_args": ["key ID ...", "COUNT", "count", "BLOCK", "milliseconds", "NOACK"],
    },
    "XSETID": {"args": ["key", "last-id"], "optional_args": []},
    "XTRIM": {
        "args": ["key", "MAXLEN", "count"],
        "optional_args": ["MINID", "id", "LIMIT", "count", "APPROX", "EXACT"],
    },
    # DragonFly specific commands
    "DF.STATS": {"args": [], "optional_args": []},
    "DF.INFO": {"args": [], "optional_args": []},
    "MEMORY": {"args": ["USAGE", "key"], "optional_args": ["SAMPLES", "count"]},
    "CAS": {"args": ["key", "oldval", "newval"], "optional_args": []},
    # Dragonfly hash-specific commands
    "HEXPIRE": {"args": ["key", "seconds"], "optional_args": []},
    "HSETEX": {
        "args": ["key", "seconds", "field", "value"],
        "optional_args": ["field", "value ..."],
    },
    # Rate Limiter commands from Dragonfly
    "CL.THROTTLE": {
        "args": ["key", "max_burst", "count_per_period", "period"],
        "optional_args": ["quantity"],
    },
    # Search commands from Dragonfly
    "FT._LIST": {"args": [], "optional_args": []},
    "FT.CREATE": {
        "args": ["index", "SCHEMA"],
        "optional_args": [
            "ON",
            "HASH|JSON",
            "PREFIX",
            "count",
            "prefix",
            "prefix ...",
            "FILTER",
            "filter",
            "LANGUAGE",
            "default_lang",
            "LANGUAGE_FIELD",
            "lang_field",
            "SCORE",
            "default_score",
            "SCORE_FIELD",
            "score_field",
            "MAXTEXTFIELDS",
            "TEMPORARY",
            "seconds",
            "NOOFFSETS",
            "NOHL",
            "NOFIELDS",
            "NOFREQS",
            "STOPWORDS",
            "count",
            "stopword",
            "stopword ...",
        ],
    },
    "FT.DROPINDEX": {"args": ["index"], "optional_args": ["DD"]},
    "FT.INFO": {"args": ["index"], "optional_args": []},
    "FT.PROFILE": {
        "args": ["index", "SEARCH", "query"],
        "optional_args": ["NOCONTENT", "LIMIT", "offset", "num"],
    },
    "FT.SEARCH": {
        "args": ["index", "query"],
        "optional_args": [
            "NOCONTENT",
            "VERBATIM",
            "NOSTOPWORDS",
            "WITHSCORES",
            "WITHPAYLOADS",
            "WITHSORTKEYS",
            "FILTER",
            "numeric_field",
            "min",
            "max",
            "GEOFILTER",
            "geo_field",
            "lon",
            "lat",
            "radius",
            "m|km|mi|ft",
            "INKEYS",
            "count",
            "key",
            "key ...",
            "INFIELDS",
            "count",
            "field",
            "field ...",
            "RETURN",
            "count",
            "identifier",
            "AS",
            "property",
            "identifier",
            "AS",
            "property ...",
            "SUMMARIZE",
            "FIELDS",
            "count",
            "field",
            "field ...",
            "FRAGS",
            "num",
            "LEN",
            "fragsize",
            "SEPARATOR",
            "separator",
            "HIGHLIGHT",
            "FIELDS",
            "count",
            "field",
            "field ...",
            "TAGS",
            "open",
            "close",
            "SLOP",
            "slop",
            "TIMEOUT",
            "timeout",
            "INORDER",
            "LANGUAGE",
            "language",
            "EXPANDER",
            "expander",
            "SCORER",
            "scorer",
            "EXPLAINSCORE",
            "PAYLOAD",
            "payload",
            "SORTBY",
            "sort_field",
            "ASC|DESC",
            "LIMIT",
            "offset",
            "num",
        ],
    },
    "FT.SYNDUMP": {"args": ["index"], "optional_args": []},
    "FT.SYNUPDATE": {
        "args": ["index", "synonym_group_id", "term", "term ..."],
        "optional_args": ["SKIPINITIALSCAN"],
    },
    # Bloom filter commands
    "BF.ADD": {"args": ["key", "item"], "optional_args": []},
    "BF.CARD": {"args": ["key"], "optional_args": []},
    "BF.EXISTS": {"args": ["key", "item"], "optional_args": []},
    "BF.INFO": {"args": ["key"], "optional_args": []},
    "BF.INSERT": {
        "args": ["key", "item"],
        "optional_args": [
            "CAPACITY",
            "capacity",
            "ERROR",
            "error",
            "EXPANSION",
            "expansion",
            "NOCREATE",
            "NONSCALING",
            "item ...",
        ],
    },
    "BF.LOADCHUNK": {"args": ["key", "iterator", "data"], "optional_args": []},
    "BF.MADD": {"args": ["key", "item"], "optional_args": ["item ..."]},
    "BF.MEXISTS": {"args": ["key", "item"], "optional_args": ["item ..."]},
    "BF.RESERVE": {
        "args": ["key", "error_rate", "capacity"],
        "optional_args": ["EXPANSION", "expansion", "NONSCALING"],
    },
    "BF.SCANDUMP": {"args": ["key", "iterator"], "optional_args": []},
    # Cuckoo filter commands
    "CF.ADD": {"args": ["key", "item"], "optional_args": []},
    "CF.ADDNX": {"args": ["key", "item"], "optional_args": []},
    "CF.COUNT": {"args": ["key", "item"], "optional_args": []},
    "CF.DEL": {"args": ["key", "item"], "optional_args": []},
    "CF.EXISTS": {"args": ["key", "item"], "optional_args": []},
    "CF.INFO": {"args": ["key"], "optional_args": []},
    "CF.INSERT": {
        "args": ["key", "item"],
        "optional_args": ["CAPACITY", "capacity", "NOCREATE", "item ..."],
    },
    "CF.INSERTNX": {
        "args": ["key", "item"],
        "optional_args": ["CAPACITY", "capacity", "NOCREATE", "item ..."],
    },
    "CF.LOADCHUNK": {"args": ["key", "iterator", "data"], "optional_args": []},
    "CF.MEXISTS": {"args": ["key", "item"], "optional_args": ["item ..."]},
    "CF.RESERVE": {
        "args": ["key", "capacity"],
        "optional_args": [
            "BUCKETSIZE",
            "bucketsize",
            "MAXITERATIONS",
            "maxiterations",
            "EXPANSION",
            "expansion",
        ],
    },
    "CF.SCANDUMP": {"args": ["key", "iterator"], "optional_args": []},
    # Count-Min Sketch commands
    "CMS.INCRBY": {"args": ["key", "item", "increment"], "optional_args": ["item increment ..."]},
    "CMS.INFO": {"args": ["key"], "optional_args": []},
    "CMS.INITBYDIM": {"args": ["key", "width", "depth"], "optional_args": []},
    "CMS.INITBYPROB": {"args": ["key", "error", "probability"], "optional_args": []},
    "CMS.MERGE": {
        "args": ["dest", "numkeys", "source"],
        "optional_args": ["source ...", "WEIGHTS", "weight", "weight ..."],
    },
    "CMS.QUERY": {"args": ["key", "item"], "optional_args": ["item ..."]},
    # JSON commands
    "JSON.ARRAPPEND": {"args": ["key", "path", "value"], "optional_args": ["value ..."]},
    "JSON.ARRINDEX": {"args": ["key", "path", "value"], "optional_args": ["start", "stop"]},
    "JSON.ARRINSERT": {"args": ["key", "path", "index", "value"], "optional_args": ["value ..."]},
    "JSON.ARRLEN": {"args": ["key"], "optional_args": ["path"]},
    "JSON.ARRPOP": {"args": ["key"], "optional_args": ["path", "index"]},
    "JSON.ARRTRIM": {"args": ["key", "path", "start", "stop"], "optional_args": []},
    "JSON.CLEAR": {"args": ["key"], "optional_args": ["path"]},
    "JSON.DEBUG": {"args": ["subcommand", "key"], "optional_args": ["path"]},
    "JSON.DEL": {"args": ["key"], "optional_args": ["path"]},
    "JSON.FORGET": {"args": ["key"], "optional_args": ["path"]},
    "JSON.GET": {
        "args": ["key"],
        "optional_args": [
            "INDENT",
            "indent",
            "NEWLINE",
            "newline",
            "SPACE",
            "space",
            "path",
            "path ...",
        ],
    },
    "JSON.MGET": {"args": ["key", "path"], "optional_args": ["key ..."]},
    "JSON.NUMINCRBY": {"args": ["key", "path", "number"], "optional_args": []},
    "JSON.NUMMULTBY": {"args": ["key", "path", "number"], "optional_args": []},
    "JSON.OBJKEYS": {"args": ["key"], "optional_args": ["path"]},
    "JSON.OBJLEN": {"args": ["key"], "optional_args": ["path"]},
    "JSON.RESP": {"args": ["key"], "optional_args": ["path"]},
    "JSON.SET": {"args": ["key", "path", "value"], "optional_args": ["NX|XX"]},
    "JSON.STRAPPEND": {"args": ["key"], "optional_args": ["path", "value"]},
    "JSON.STRLEN": {"args": ["key"], "optional_args": ["path"]},
    "JSON.TOGGLE": {"args": ["key", "path"], "optional_args": []},
    "JSON.TYPE": {"args": ["key"], "optional_args": ["path"]},
    # Vector commands
    "VADD": {
        "args": ["key", "id", "vector"],
        "optional_args": ["id vector ...", "DIMENSIONS", "dimensions"],
    },
    "VADDONCE": {"args": ["key", "id", "vector"], "optional_args": ["DIMENSIONS", "dimensions"]},
    "VCREATE": {
        "args": ["key", "dimensions"],
        "optional_args": [
            "ALGORITHM",
            "algorithm",
            "M",
            "m",
            "EF_CONSTRUCTION",
            "ef_construction",
            "DISTANCE_METRIC",
            "distance_metric",
            "INITIAL_CAP",
            "initial_cap",
            "DATA_TYPE",
            "data_type",
        ],
    },
    "VDEL": {"args": ["key", "id"], "optional_args": ["id ..."]},
    "VDIM": {"args": ["key"], "optional_args": []},
    "VEXISTS": {"args": ["key", "id"], "optional_args": []},
    "VGET": {"args": ["key", "id"], "optional_args": []},
    "VGETALL": {"args": ["key"], "optional_args": []},
    "VGETATTR": {"args": ["key", "id"], "optional_args": []},
    "VINFO": {"args": ["key"], "optional_args": []},
    "VLINKS": {"args": ["key", "id"], "optional_args": []},
    "VRANDMEMBER": {"args": ["key"], "optional_args": ["count"]},
    "VREM": {"args": ["key", "id"], "optional_args": ["id ..."]},
    "VSETATTR": {"args": ["key", "id", "attributes"], "optional_args": []},
    "VSIM": {
        "args": ["key", "vector"],
        "optional_args": [
            "K",
            "k",
            "BYID",
            "id",
            "EFRUNTIME",
            "ef_runtime",
            "RADIUS",
            "radius",
            "RETURNED_ATTRIBUTES",
            "attributes",
            "RETURN_ATTRS",
        ],
    },
    # Server management commands
    "ASKING": {"args": [], "optional_args": []},
    "BGREWRITEAOF": {"args": [], "optional_args": []},
    "BGSAVE": {"args": [], "optional_args": ["SCHEDULE"]},
    "COMMAND": {
        "args": [],
        "optional_args": [
            "DOCS",
            "INFO",
            "command",
            "GETKEYS",
            "GETKEYSANDFLAGS",
            "COUNT",
            "LIST",
            "HELP",
        ],
    },
    "FAILOVER": {
        "args": [],
        "optional_args": ["TO", "host", "port", "ABORT", "TIMEOUT", "milliseconds", "FORCE"],
    },
    "LASTSAVE": {"args": [], "optional_args": []},
    "LOLWUT": {"args": [], "optional_args": ["VERSION", "version"]},
    "ROLE": {"args": [], "optional_args": []},
    "SAVE": {"args": [], "optional_args": []},
    "SHUTDOWN": {"args": [], "optional_args": ["NOSAVE", "SAVE", "NOW", "FORCE", "ABORT"]},
    "SLAVEOF": {"args": ["host", "port"], "optional_args": []},
    "REPLICAOF": {"args": ["host", "port"], "optional_args": []},
    "SWAPDB": {"args": ["index1", "index2"], "optional_args": []},
    "SYNC": {"args": [], "optional_args": []},
    "WAIT": {"args": ["numreplicas", "timeout"], "optional_args": []},
    "WAITAOF": {"args": ["numlocal", "numreplicas", "timeout"], "optional_args": []},
    "RESET": {"args": [], "optional_args": []},
    # Add commands from Valkey
    "COMMANDLOG": {"args": [], "optional_args": []},
    "COMMANDLOG GET": {"args": [], "optional_args": ["count"]},
    "COMMANDLOG HELP": {"args": [], "optional_args": []},
    "COMMANDLOG LEN": {"args": [], "optional_args": []},
    "COMMANDLOG RESET": {"args": [], "optional_args": []},
    "EVALSHA_RO": {
        "args": ["sha1", "numkeys", "key"],
        "optional_args": ["key ...", "arg", "arg ..."],
    },
    "EVAL_RO": {
        "args": ["script", "numkeys", "key"],
        "optional_args": ["key ...", "arg", "arg ..."],
    },
    "FCALL": {
        "args": ["function", "numkeys", "key"],
        "optional_args": ["key ...", "arg", "arg ..."],
    },
    "FCALL_RO": {
        "args": ["function", "numkeys", "key"],
        "optional_args": ["key ...", "arg", "arg ..."],
    },
    "FUNCTION": {"args": ["subcommand"], "optional_args": ["arg", "arg ..."]},
    "FUNCTION DELETE": {"args": ["library"], "optional_args": []},
    "FUNCTION DUMP": {"args": [], "optional_args": []},
    "FUNCTION FLUSH": {"args": [], "optional_args": ["ASYNC", "SYNC"]},
    "FUNCTION HELP": {"args": [], "optional_args": []},
    "FUNCTION KILL": {"args": [], "optional_args": []},
    "FUNCTION LIST": {"args": [], "optional_args": ["LIBRARYNAME", "pattern"]},
    "FUNCTION LOAD": {"args": ["code"], "optional_args": ["REPLACE"]},
    "FUNCTION RESTORE": {"args": ["payload"], "optional_args": ["FLUSH", "APPEND", "REPLACE"]},
    "FUNCTION STATS": {"args": [], "optional_args": []},
    "GEORADIUSBYMEMBER_RO": {
        "args": ["key", "member", "radius", "unit"],
        "optional_args": ["WITHDIST", "WITHCOORD", "WITHHASH", "COUNT", "count", "ASC|DESC"],
    },
    "GEORADIUS_RO": {
        "args": ["key", "longitude", "latitude", "radius", "unit"],
        "optional_args": ["WITHDIST", "WITHCOORD", "WITHHASH", "COUNT", "count", "ASC|DESC"],
    },
    "GEOSEARCH": {
        "args": [
            "key",
            "FROMLONLAT|FROMMEMBER",
            "longitude latitude|member",
            "BYRADIUS|BYBOX",
            "radius|width",
            "unit",
        ],
        "optional_args": ["WITHDIST", "WITHCOORD", "WITHHASH", "COUNT", "count", "ASC|DESC"],
    },
    "GEOSEARCHSTORE": {
        "args": [
            "destination",
            "source",
            "FROMLONLAT|FROMMEMBER",
            "longitude latitude|member",
            "BYRADIUS|BYBOX",
            "radius|width",
            "unit",
        ],
        "optional_args": [
            "WITHDIST",
            "WITHCOORD",
            "WITHHASH",
            "COUNT",
            "count",
            "ASC|DESC",
            "STOREDIST",
        ],
    },
    "JSON.MSET": {"args": ["key", "path", "value"], "optional_args": ["key path value ..."]},
    "LATENCY": {"args": ["subcommand"], "optional_args": ["arg", "arg ..."]},
    "LATENCY DOCTOR": {"args": [], "optional_args": []},
    "LATENCY GRAPH": {"args": ["event"], "optional_args": []},
    "LATENCY HELP": {"args": [], "optional_args": []},
    "LATENCY HISTOGRAM": {"args": ["event"], "optional_args": ["event ..."]},
    "LATENCY HISTORY": {"args": ["event"], "optional_args": []},
    "LATENCY LATEST": {"args": [], "optional_args": []},
    "LATENCY RESET": {"args": [], "optional_args": ["event", "event ..."]},
    "LCS": {
        "args": ["key1", "key2"],
        "optional_args": ["LEN", "IDX", "MINMATCHLEN", "len", "WITHMATCHLEN"],
    },
    "MEMORY DOCTOR": {"args": [], "optional_args": []},
    "MEMORY HELP": {"args": [], "optional_args": []},
    "MEMORY MALLOC-STATS": {"args": [], "optional_args": []},
    "MEMORY PURGE": {"args": [], "optional_args": []},
    "MEMORY STATS": {"args": [], "optional_args": []},
    "MODULE": {"args": ["subcommand"], "optional_args": ["arg", "arg ..."]},
    "MODULE HELP": {"args": [], "optional_args": []},
    "MODULE LIST": {"args": [], "optional_args": []},
    "MODULE LOAD": {"args": ["path"], "optional_args": ["arg", "arg ..."]},
    "MODULE LOADEX": {"args": ["path"], "optional_args": ["CONFIG", "name", "value"]},
    "MODULE UNLOAD": {"args": ["name"], "optional_args": []},
    "PFDEBUG": {"args": ["key"], "optional_args": []},
    "PFSELFTEST": {"args": [], "optional_args": []},
    "PUBSUB SHARDCHANNELS": {"args": [], "optional_args": ["pattern"]},
    "PUBSUB SHARDNUMSUB": {"args": [], "optional_args": ["shardchannel", "shardchannel ..."]},
    "REPLCONF": {"args": ["option", "value"], "optional_args": []},
    "RESTORE-ASKING": {
        "args": ["key", "ttl", "serialized-value"],
        "optional_args": ["REPLACE", "ABSTTL", "IDLETIME", "seconds", "FREQ", "frequency"],
    },
    "SCRIPT SHOW": {"args": [], "optional_args": []},
    "SORT_RO": {
        "args": ["key"],
        "optional_args": [
            "BY",
            "pattern",
            "LIMIT",
            "offset",
            "count",
            "GET",
            "pattern",
            "ASC|DESC",
            "ALPHA",
        ],
    },
    "SPUBLISH": {"args": ["shardchannel", "message"], "optional_args": []},
    "SSUBSCRIBE": {"args": ["shardchannel"], "optional_args": ["shardchannel ..."]},
    "SUNSUBSCRIBE": {"args": ["shardchannel"], "optional_args": ["shardchannel ..."]},
    # Add cluster commands from Valkey
    "CLUSTER": {"args": [], "optional_args": []},
    "CLUSTER ADDSLOTS": {"args": ["slot"], "optional_args": ["slot ..."]},
    "CLUSTER ADDSLOTSRANGE": {"args": ["start", "end"], "optional_args": ["start end ..."]},
    "CLUSTER BUMPEPOCH": {"args": [], "optional_args": []},
    "CLUSTER COUNT-FAILURE-REPORTS": {"args": ["node-id"], "optional_args": []},
    "CLUSTER COUNTKEYSINSLOT": {"args": ["slot"], "optional_args": []},
    "CLUSTER DELSLOTS": {"args": ["slot"], "optional_args": ["slot ..."]},
    "CLUSTER DELSLOTSRANGE": {"args": ["start", "end"], "optional_args": ["start end ..."]},
    "CLUSTER FAILOVER": {"args": [], "optional_args": ["FORCE", "TAKEOVER"]},
    "CLUSTER FLUSHSLOTS": {"args": [], "optional_args": []},
    "CLUSTER FORGET": {"args": ["node-id"], "optional_args": []},
    "CLUSTER GETKEYSINSLOT": {"args": ["slot", "count"], "optional_args": []},
    "CLUSTER HELP": {"args": [], "optional_args": []},
    "CLUSTER INFO": {"args": [], "optional_args": []},
    "CLUSTER KEYSLOT": {"args": ["key"], "optional_args": []},
    "CLUSTER LINKS": {"args": [], "optional_args": []},
    "CLUSTER MEET": {"args": ["ip", "port"], "optional_args": ["cluster-bus-port"]},
    "CLUSTER MYID": {"args": [], "optional_args": []},
    "CLUSTER MYSHARDID": {"args": [], "optional_args": []},
    "CLUSTER NODES": {"args": [], "optional_args": []},
    "CLUSTER REPLICAS": {"args": ["node-id"], "optional_args": []},
    "CLUSTER REPLICATE": {"args": ["node-id"], "optional_args": []},
    "CLUSTER RESET": {"args": [], "optional_args": ["HARD", "SOFT"]},
    "CLUSTER SAVECONFIG": {"args": [], "optional_args": []},
    "CLUSTER SET-CONFIG-EPOCH": {"args": ["epoch"], "optional_args": []},
    "CLUSTER SETSLOT": {"args": ["slot", "subcommand"], "optional_args": ["node-id"]},
    "CLUSTER SHARDS": {"args": [], "optional_args": []},
    "CLUSTER SLAVES": {"args": ["node-id"], "optional_args": []},
    "CLUSTER SLOT-STATS": {"args": [], "optional_args": []},
    "CLUSTER SLOTS": {"args": [], "optional_args": []},
    "READONLY": {"args": [], "optional_args": []},
    "READWRITE": {"args": [], "optional_args": []},
}

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
    # New data types
    "categoryname": lambda: random.choice(
        [
            "string",
            "list",
            "set",
            "sorted_set",
            "hash",
            "pubsub",
            "transaction",
            "connection",
            "server",
            "scripting",
        ]
    ),
    "command": lambda: random.choice(list(REDIS_COMMANDS.keys())),
    "bits": lambda: str(random.randint(1, 256)),
    "rule": lambda: random.choice(
        [
            "on",
            "off",
            "nopass",
            "+@all",
            "-@all",
            "+@string",
            "-@dangerous",
            ">password",
            "<password",
        ]
    ),
    "groupname": lambda: "group:"
    + "".join(random.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(random.randint(3, 8))),
    "consumername": lambda: "consumer:"
    + "".join(random.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(random.randint(3, 8))),
    "min-idle-time": lambda: str(random.randint(1, 10000)),
    "ms-unix-time": lambda: str(random.randint(1000000000, 2000000000)),
    "last-id": lambda: f"{random.randint(0, 1000)}-{random.randint(0, 1000)}",
    "weight": lambda: str(random.uniform(0.1, 10.0)),
    "limit": lambda: str(random.randint(1, 100)),
    "destination": lambda: "dest:"
    + "".join(random.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(random.randint(3, 8))),
    "dst": lambda: "dst:"
    + "".join(random.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(random.randint(3, 8))),
    "src": lambda: "src:"
    + "".join(random.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(random.randint(3, 8))),
    "item": lambda: "item:"
    + "".join(
        random.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(random.randint(1, 10))
    ),
    "error_rate": lambda: str(random.uniform(0.001, 0.1)),
    "capacity": lambda: str(random.randint(100, 10000)),
    "expansion": lambda: str(random.randint(1, 5)),
    "iterator": lambda: str(random.randint(0, 100)),
    "data": lambda: "".join(
        random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")
        for _ in range(random.randint(10, 50))
    ),
    "bucketsize": lambda: str(random.randint(1, 10)),
    "maxiterations": lambda: str(random.randint(10, 100)),
    "error": lambda: str(random.uniform(0.01, 0.1)),
    "probability": lambda: str(random.uniform(0.01, 0.1)),
    "width": lambda: str(random.randint(10, 100)),
    "depth": lambda: str(random.randint(5, 20)),
    "path": lambda: random.choice(["$", "$[0]", "$.field", "$.nested.field", "$..field"]),
    "indent": lambda: str(random.randint(0, 4)),
    "newline": lambda: random.choice(["\\n", "\\r\\n"]),
    "space": lambda: " ",
    "number": lambda: str(random.uniform(-100, 100)),
    "id": lambda: "".join(
        random.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(random.randint(3, 8))
    ),
    "vector": lambda: "["
    + ",".join(str(random.uniform(-1, 1)) for _ in range(random.randint(2, 10)))
    + "]",
    "dimensions": lambda: str(random.randint(2, 128)),
    "algorithm": lambda: random.choice(["FLAT", "HNSW"]),
    "m": lambda: str(random.randint(5, 50)),
    "ef_construction": lambda: str(random.randint(10, 500)),
    "distance_metric": lambda: random.choice(["L2", "IP", "COSINE"]),
    "initial_cap": lambda: str(random.randint(1000, 10000)),
    "data_type": lambda: random.choice(["FLOAT32", "FLOAT64"]),
    "ef_runtime": lambda: str(random.randint(10, 1000)),
    "attributes": lambda: '{"'
    + "".join(random.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(random.randint(3, 8)))
    + '":"'
    + "".join(random.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(random.randint(3, 8)))
    + '"}',
    "host": lambda: random.choice(["localhost", "127.0.0.1", "redis-server"]),
    "port": lambda: str(random.randint(1024, 65535)),
    "numreplicas": lambda: str(random.randint(0, 5)),
    "numlocal": lambda: str(random.randint(0, 5)),
    "index1": lambda: str(random.randint(0, 15)),
    "index2": lambda: str(random.randint(0, 15)),
    "timeout": lambda: str(random.randint(100, 10000)),
    "group": lambda: "group:"
    + "".join(random.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(random.randint(3, 8))),
    # New data types
    "slot": lambda: str(random.randint(0, 16383)),
    "node-id": lambda: "".join(random.choice("0123456789abcdef") for _ in range(40)),
    "epoch": lambda: str(random.randint(1, 10000)),
    "subcommand": lambda: random.choice(["IMPORTING", "MIGRATING", "NODE", "STABLE"]),
    "cluster-bus-port": lambda: str(random.randint(10000, 30000)),
    "event": lambda: random.choice(
        [
            "command",
            "fast-command",
            "fork",
            "aof-fsync-always",
            "aof-write",
            "expire-cycle",
            "eviction",
        ]
    ),
    "library": lambda: "lib:"
    + "".join(random.choice(string.ascii_lowercase) for _ in range(random.randint(3, 10))),
    "function": lambda: "myfunc",
    "code": lambda: "redis.register_function('myfunc', function() return 'hello' end)",
    "payload": lambda: "".join(
        random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/")
        for _ in range(random.randint(20, 100))
    ),
    "shardchannel": lambda: "shard:"
    + "".join(
        random.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(random.randint(3, 8))
    ),
    "option": lambda: random.choice(["ACK", "GETACK", "CAPA", "LISTENING-PORT"]),
    "key1": lambda: "key1:"
    + "".join(
        random.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(random.randint(1, 10))
    ),
    "key2": lambda: "key2:"
    + "".join(
        random.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(random.randint(1, 10))
    ),
    "len": lambda: str(random.randint(1, 10)),
    "path": lambda: random.choice(["/path/to/module.so", "./module.so"]),
    "name": lambda: "".join(
        random.choice(string.ascii_lowercase) for _ in range(random.randint(3, 10))
    ),
    "ip": lambda: ".".join(str(random.randint(0, 255)) for _ in range(4)),
    "frequency": lambda: str(random.randint(1, 100)),
    "serialized-value": lambda: "".join(
        random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/")
        for _ in range(random.randint(20, 100))
    ),
    # Rate Limiter parameters
    "index": lambda: "idx:"
    + "".join(random.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(random.randint(3, 8))),
    "filter": lambda: "@field:{tag}",
    "default_lang": lambda: random.choice(
        ["english", "spanish", "french", "chinese", "japanese", "arabic"]
    ),
    "lang_field": lambda: "language",
    "default_score": lambda: str(random.uniform(0.1, 10.0)),
    "score_field": lambda: "score",
    "stopword": lambda: random.choice(["a", "an", "the", "and", "or", "but", "in", "on", "at"]),
    "query": lambda: random.choice(["@title:hello", "*", "@tags:{important}", "@num:[0 100]"]),
    "synonym_group_id": lambda: "group" + str(random.randint(1, 1000)),
    "term": lambda: random.choice(["word", "term", "phrase", "concept"]),
    "max_burst": lambda: str(random.randint(0, 30)),
    "count_per_period": lambda: str(random.randint(10, 1000)),
    "period": lambda: str(random.randint(1, 3600)),
    "quantity": lambda: str(random.randint(1, 10)),
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
    # New data types
    "categoryname": lambda: random.choice(
        [
            "string",
            "list",
            "set",
            "sorted_set",
            "hash",
            "pubsub",
            "transaction",
            "connection",
            "server",
            "scripting",
        ]
    ),
    "command": lambda: random.choice(list(REDIS_COMMANDS.keys())),
    "bits": lambda: str(random.randint(1, 256)),
    "rule": lambda: random.choice(
        [
            "on",
            "off",
            "nopass",
            "+@all",
            "-@all",
            "+@string",
            "-@dangerous",
            ">password",
            "<password",
        ]
    ),
    "groupname": lambda: "group:"
    + "".join(random.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(random.randint(3, 8))),
    "consumername": lambda: "consumer:"
    + "".join(random.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(random.randint(3, 8))),
    "min-idle-time": lambda: str(random.randint(1, 10000)),
    "ms-unix-time": lambda: str(random.randint(1000000000, 2000000000)),
    "last-id": lambda: f"{random.randint(0, 1000)}-{random.randint(0, 1000)}",
    "weight": lambda: str(random.uniform(0.1, 10.0)),
    "limit": lambda: str(random.randint(1, 100)),
    "destination": "key",
    "dst": "key",
    "src": "key",
    "item": lambda: "item:"
    + "".join(
        random.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(random.randint(1, 10))
    ),
    "error_rate": lambda: str(random.uniform(0.001, 0.1)),
    "capacity": lambda: str(random.randint(100, 10000)),
    "expansion": lambda: str(random.randint(1, 5)),
    "iterator": lambda: str(random.randint(0, 100)),
    "data": lambda: "".join(
        random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")
        for _ in range(random.randint(10, 50))
    ),
    "bucketsize": lambda: str(random.randint(1, 10)),
    "maxiterations": lambda: str(random.randint(10, 100)),
    "error": lambda: str(random.uniform(0.01, 0.1)),
    "probability": lambda: str(random.uniform(0.01, 0.1)),
    "width": lambda: str(random.randint(10, 100)),
    "depth": lambda: str(random.randint(5, 20)),
    "path": lambda: random.choice(["$", "$[0]", "$.field", "$.nested.field", "$..field"]),
    "indent": lambda: str(random.randint(0, 4)),
    "newline": lambda: random.choice(["\\n", "\\r\\n"]),
    "space": lambda: " ",
    "number": lambda: str(random.uniform(-100, 100)),
    "id": lambda: "".join(
        random.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(random.randint(3, 8))
    ),
    "vector": lambda: "["
    + ",".join(str(random.uniform(-1, 1)) for _ in range(random.randint(2, 10)))
    + "]",
    "dimensions": lambda: str(random.randint(2, 128)),
    "algorithm": lambda: random.choice(["FLAT", "HNSW"]),
    "m": lambda: str(random.randint(5, 50)),
    "ef_construction": lambda: str(random.randint(10, 500)),
    "distance_metric": lambda: random.choice(["L2", "IP", "COSINE"]),
    "initial_cap": lambda: str(random.randint(1000, 10000)),
    "data_type": lambda: random.choice(["FLOAT32", "FLOAT64"]),
    "ef_runtime": lambda: str(random.randint(10, 1000)),
    "attributes": lambda: '{"'
    + "".join(random.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(random.randint(3, 8)))
    + '":"'
    + "".join(random.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(random.randint(3, 8)))
    + '"}',
    "host": lambda: random.choice(["localhost", "127.0.0.1", "redis-server"]),
    "port": lambda: str(random.randint(1024, 65535)),
    "numreplicas": lambda: str(random.randint(0, 5)),
    "numlocal": lambda: str(random.randint(0, 5)),
    "index1": lambda: str(random.randint(0, 15)),
    "index2": lambda: str(random.randint(0, 15)),
    "timeout": lambda: str(random.randint(100, 10000)),
    "group": "groupname",
    # New data types
    "slot": lambda: str(random.randint(0, 16383)),
    "node-id": lambda: "".join(random.choice("0123456789abcdef") for _ in range(40)),
    "epoch": lambda: str(random.randint(1, 10000)),
    "subcommand": lambda: random.choice(["IMPORTING", "MIGRATING", "NODE", "STABLE"]),
    "cluster-bus-port": lambda: str(random.randint(10000, 30000)),
    "event": lambda: random.choice(
        [
            "command",
            "fast-command",
            "fork",
            "aof-fsync-always",
            "aof-write",
            "expire-cycle",
            "eviction",
        ]
    ),
    "library": lambda: "lib:"
    + "".join(random.choice(string.ascii_lowercase) for _ in range(random.randint(3, 10))),
    "function": lambda: "myfunc",
    "code": lambda: "redis.register_function('myfunc', function() return 'hello' end)",
    "payload": lambda: "".join(
        random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/")
        for _ in range(random.randint(20, 100))
    ),
    "shardchannel": lambda: "shard:"
    + "".join(
        random.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(random.randint(3, 8))
    ),
    "option": lambda: random.choice(["ACK", "GETACK", "CAPA", "LISTENING-PORT"]),
    "key1": lambda: "key1:"
    + "".join(
        random.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(random.randint(1, 10))
    ),
    "key2": lambda: "key2:"
    + "".join(
        random.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(random.randint(1, 10))
    ),
    "len": lambda: str(random.randint(1, 10)),
    "path": lambda: random.choice(["/path/to/module.so", "./module.so"]),
    "name": lambda: "".join(
        random.choice(string.ascii_lowercase) for _ in range(random.randint(3, 10))
    ),
    "ip": lambda: ".".join(str(random.randint(0, 255)) for _ in range(4)),
    "frequency": lambda: str(random.randint(1, 100)),
    "serialized-value": lambda: "".join(
        random.choice("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/")
        for _ in range(random.randint(20, 100))
    ),
    # Rate Limiter parameters
    "index": lambda: "idx:"
    + "".join(random.choice("abcdefghijklmnopqrstuvwxyz") for _ in range(random.randint(3, 8))),
    "filter": lambda: "@field:{tag}",
    "default_lang": lambda: random.choice(
        ["english", "spanish", "french", "chinese", "japanese", "arabic"]
    ),
    "lang_field": lambda: "language",
    "default_score": lambda: str(random.uniform(0.1, 10.0)),
    "score_field": lambda: "score",
    "stopword": lambda: random.choice(["a", "an", "the", "and", "or", "but", "in", "on", "at"]),
    "query": lambda: random.choice(["@title:hello", "*", "@tags:{important}", "@num:[0 100]"]),
    "synonym_group_id": lambda: "group" + str(random.randint(1, 1000)),
    "term": lambda: random.choice(["word", "term", "phrase", "concept"]),
    "max_burst": lambda: str(random.randint(0, 30)),
    "count_per_period": lambda: str(random.randint(10, 1000)),
    "period": lambda: str(random.randint(1, 3600)),
    "quantity": lambda: str(random.randint(1, 10)),
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
