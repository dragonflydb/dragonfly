const globalStats = {
    "cpu": [],
    "qps": [],
    "used_memory_bytes": [],
    "max_memory_bytes": [],
    "num_connected_clients": [],
    "uptime": ["6 minutes, 25 seconds"],
    "total_commands_num": [],
    "hit_rate": [],
    "shards_stats": {
        "used_memory": [],
        "key_count": [],
        "expire_count": [],
        "key_reads": []
    }
};

async function updateStats() {

    const limit = 20;
    const range = [1, 100];

    update(globalStats);
    return globalStats;

    function update(stats) {
        Object.keys(stats).forEach(key => {
            if (key == "uptime") return;
            if (stats[key].constructor === Array) {
                if (stats[key].length >= limit) {
                    stats[key].shift();
                }
                const num = Math.floor(Math.random() * (range[1] - range[0] + 1) + range[0]);
                stats[key].push(num);
            } else update(stats[key])
        });
    }

}

async function fetchCommands() {
    return ["BRPOP",
        "GEORADIUSBYMEMBER", "SSCAN", "LTRIM", "LPOS", "MSET", "HRANDFIELD", "SINTER", "GEOHASH", "INFO", "SREM", "MODULE", "SUBSTR", "SUNIONSTORE", "ADDREPLICAOF", "SET", "TIME", "EXEC", "GEOPOS", "BGSAVE", "ACL", "WHOAMI", "CL", "THROTTLE", "GETEX", "WATCH", "XSETID", "XINFO", "HLEN", "ZUNIONSTORE", "HSETNX", "BITFIELD", "PING", "APPEND", "PSETEX", "PFCOUNT", "RPOP", "SMOVE", "SUNION", "SLAVEOF", "INCR", "JSON", "MGET", "REPLICAOF", "ZREVRANK", "RPUSH", "BITCOUNT", "FT", "DROPINDEX", "ACL", "SETUSER", "ACL", "CAT", "FT", "LIST", "SMEMBERS", "BLPOP", "HINCRBYFLOAT", "RENAMENX", "ZCOUNT", "GEOSEARCH", "FLUSHDB", "ZINTER", "SISMEMBER", "ZREMRANGEBYLEX", "LINDEX", "ACL", "LOAD", "MOVE", "ZMSCORE", "RPOPLPUSH", "XREVRANGE", "RENAME",
        "ACL", "USERS", "HKEYS", "JSON", "DEL", "ZSCAN", "MSETNX", "CONFIG", "SHUTDOWN", "JSON", "ARRLEN", "FUNCTION", "XREADGROUP", "HSETEX", "INCRBY", "TYPE", "HEXISTS", "XRANGE", "GET", "ZINTERSTORE", "XCLAIM", "EXPIREAT", "LSET", "EXISTS", "UNWATCH", "QUIT", "JSON", "DEBUG", "BZPOPMIN", "LPUSH", "JSON", "ARRINSERT", "PFADD", "RESTORE", "HSET", "JSON", "NUMINCRBY", "XREAD", "XACK", "KEYS", "SETEX", "EXPIRE", "HELLO", "ZREM", "ROLE", "GETDEL", "ZPOPMAX", "HSCAN", "JSON", "ARRINDEX", "SDIFFSTORE", "SRANDMEMBER", "HMGET", "SAVE", "LLEN", "ZRANGEBYSCORE", "HGET", "PFMERGE", "COMMAND", "DBSIZE", "JSON", "NUMMULTBY", "ZREVRANGEBYSCORE", "DUMP", "JSON", "TYPE", "ZDIFF", "XPENDING", "ZREVRANGE", "ACL", "BZPOPMAX", "SINTERSTORE", "JSON", "ARRPOP", "ZRANDMEMBER", "SCARD", "ZPOPMIN", "ACL", "LIST", "HDEL", "JSON", "GET", "ACL", "LOG", "JSON", "ARRAPPEND", "SLOWLOG", "XAUTOCLAIM", "DEBUG", "HSTRLEN", "REPLCONF", "JSON", "SET", "LMOVE", "PUBLISH", "DECR", "JSON", "OBJLEN", "MGET", "LRANGE", "ZADD", "SORT", "LPUSHX", "ZINTERCARD", "ZRANGE", "FT", "AGGREGATE", "CLIENT", "SELECT", "SPOP", "PEXPIREAT", "BRPOPLPUSH", "LATENCY", "ZCARD", "ECHO", "BITOP", "SADDEX", "RANDOMKEY", "SCAN", "TTL", "GETSET", "ACL", "GETUSER", "ACL", "DELUSER", "XADD", "XTRIM", "UNLINK", "ZINCRBY", "HGETALL", "ACL", "DRYRUN", "HVALS", "SADD", "GETRANGE",
        "PREPEND", "DISCARD", "SETRANGE", "LPOP", "HMSET", "JSON", "ARRTRIM", "TOUCH", "SINTERCARD", "JSON", "RESP", "ZRANK", "GETBIT", "REPLTAKEOVER", "ACL", "GENPASS", "FLUSHALL", "LREM", "MONITOR", "MULTI", "INCRBYFLOAT", "BLMOVE", "SMISMEMBER", "GEOADD", "SETNX", "GEODIST", "XLEN", "CLUSTER", "XGROUP", "DECRBY", "JSON", "CLEAR", "FT", "PROFILE", "ZSCORE", "XDEL", "SCRIPT", "STRLEN", "SUBSCRIBE", "ZRANGEBYLEX", "JSON", "STRAPPEND", "SETBIT", "UNSUBSCRIBE", "ZREVRANGEBYLEX", "FT", "SEARCH", "READWRITE", "PUNSUBSCRIBE", "ZREMRANGEBYRANK", "RPUSHX", "PERSIST", "JSON", "FORGET", "DEL", "ZLEXCOUNT", "ZUNION", "JSON", "TOGGLE", "JSON", "OBJKEYS", "FT",
        "CREATE", "HINCRBY", "BITPOS", "AUTH", "LASTSAVE", "SDIFF", "PEXPIRE", "STICK", "EVAL", "ACL", "SAVE", "FT", "INFO", "EVALSHA", "PSUBSCRIBE", "JSON", "STRLEN", "BITFIELD", "RO", "READONLY", "PUBSUB", "MEMORY", "LINSERT", "ZREMRANGEBYSCORE", "PTTL", "FIELDTTL"
    ];
}