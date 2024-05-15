async function fetchData(cmd) {
   try {
       const response = await fetch(API_URL, {
           method: 'POST',
           mode: 'cors',
           cache: 'no-cache',
           credentials: 'same-origin',
           headers: {
               'Content-Type': 'application/json'
           },
           body: JSON.stringify(formatCommands([cmd]))
       });

       const reply = await response.json();
       return reply;
   } catch (e) {
     console.log("cannot fetch data for command ", cmd, e);
   }
   return null;
}

function formatTime(seconds) {
   if (isNaN(seconds) || seconds < 0) {
     return "Invalid input";
   }

   const years = Math.floor(seconds / (365.25 * 24 * 60 * 60)); // Approximate a year as 365.25 days
   seconds %= 365.25 * 24 * 60 * 60;

   const months = Math.floor(seconds / (30.44 * 24 * 60 * 60)); // Approximate a month as 30.44 days
   seconds %= 30.44 * 24 * 60 * 60;

   const days = Math.floor(seconds / (24 * 60 * 60));
   seconds %= 24 * 60 * 60;

   const hours = Math.floor(seconds / (60 * 60));
   seconds %= 60 * 60;

   const minutes = Math.floor(seconds / 60);
   seconds %= 60;

   const result = [];
   if (years > 0) result.push(`${years} year${years > 1 ? "s" : ""}`);
   if (months > 0) result.push(`${months} month${months > 1 ? "s" : ""}`);
   if (days > 0) result.push(`${days} day${days > 1 ? "s" : ""}`);
   if (hours > 0) result.push(`${hours} hour${hours > 1 ? "s" : ""}`);
   if (minutes > 0) result.push(`${minutes} minute${minutes > 1 ? "s" : ""}`);
   if (seconds > 0) result.push(`${seconds} second${seconds > 1 ? "s" : ""}`);

   return result.join(", ");
}

/**
* Functions to extract the list of commands from Dragonfly.
*/


function parseCommands(input) {
 const regex = /([A-Z]+)/g;
 const matches = input.match(regex);
 return matches || [];
}

async function fetchCommands() {
  try {
   const response = await fetch(API_URL, {
         method: 'POST',
         mode: 'cors',
         cache: 'no-cache',
         credentials: 'same-origin',
         body: JSON.stringify(formatCommands(["command"]))
   });


   const text = await response.text();
   commands = parseCommands(text);
   console.log(commands);
   // TODO: delete the following line
   commands = [ "BRPOP",
       "GEORADIUSBYMEMBER","SSCAN","LTRIM","LPOS","MSET","HRANDFIELD","SINTER","GEOHASH","INFO","SREM","MODULE","SUBSTR","SUNIONSTORE","ADDREPLICAOF","SET","TIME","EXEC","GEOPOS","BGSAVE","ACL","WHOAMI","CL","THROTTLE","GETEX","WATCH","XSETID","XINFO","HLEN","ZUNIONSTORE","HSETNX","BITFIELD","PING","APPEND","PSETEX","PFCOUNT","RPOP","SMOVE","SUNION","SLAVEOF","INCR","JSON","MGET","REPLICAOF","ZREVRANK","RPUSH","BITCOUNT","FT","DROPINDEX","ACL","SETUSER","ACL","CAT","FT","LIST","SMEMBERS","BLPOP","HINCRBYFLOAT","RENAMENX","ZCOUNT","GEOSEARCH","FLUSHDB","ZINTER","SISMEMBER","ZREMRANGEBYLEX","LINDEX","ACL","LOAD","MOVE","ZMSCORE","RPOPLPUSH","XREVRANGE","RENAME",
       "ACL","USERS","HKEYS","JSON","DEL","ZSCAN","MSETNX","CONFIG","SHUTDOWN","JSON","ARRLEN","FUNCTION","XREADGROUP","HSETEX","INCRBY","TYPE","HEXISTS","XRANGE","GET","ZINTERSTORE","XCLAIM","EXPIREAT","LSET","EXISTS","UNWATCH","QUIT","JSON","DEBUG","BZPOPMIN","LPUSH","JSON","ARRINSERT","PFADD","RESTORE","HSET","JSON","NUMINCRBY","XREAD","XACK","KEYS","SETEX","EXPIRE","HELLO","ZREM","ROLE","GETDEL","ZPOPMAX","HSCAN","JSON","ARRINDEX","SDIFFSTORE","SRANDMEMBER","HMGET","SAVE","LLEN","ZRANGEBYSCORE","HGET","PFMERGE","COMMAND","DBSIZE","JSON","NUMMULTBY","ZREVRANGEBYSCORE","DUMP","JSON","TYPE","ZDIFF","XPENDING","ZREVRANGE","ACL","BZPOPMAX","SINTERSTORE","JSON","ARRPOP","ZRANDMEMBER","SCARD","ZPOPMIN","ACL","LIST","HDEL","JSON","GET","ACL","LOG","JSON","ARRAPPEND","SLOWLOG","XAUTOCLAIM","DEBUG","HSTRLEN","REPLCONF","JSON","SET","LMOVE","PUBLISH","DECR","JSON","OBJLEN","MGET","LRANGE","ZADD","SORT","LPUSHX","ZINTERCARD","ZRANGE","FT","AGGREGATE","CLIENT","SELECT","SPOP","PEXPIREAT","BRPOPLPUSH","LATENCY","ZCARD","ECHO","BITOP","SADDEX","RANDOMKEY","SCAN","TTL","GETSET","ACL","GETUSER","ACL","DELUSER","XADD","XTRIM","UNLINK","ZINCRBY","HGETALL","ACL","DRYRUN","HVALS","SADD","GETRANGE",
       "PREPEND","DISCARD","SETRANGE","LPOP","HMSET","JSON","ARRTRIM","TOUCH","SINTERCARD","JSON","RESP","ZRANK","GETBIT","REPLTAKEOVER","ACL","GENPASS","FLUSHALL","LREM","MONITOR","MULTI","INCRBYFLOAT","BLMOVE","SMISMEMBER","GEOADD","SETNX","GEODIST","XLEN","CLUSTER","XGROUP","DECRBY","JSON","CLEAR","FT","PROFILE","ZSCORE","XDEL","SCRIPT","STRLEN","SUBSCRIBE","ZRANGEBYLEX","JSON","STRAPPEND","SETBIT","UNSUBSCRIBE","ZREVRANGEBYLEX","FT","SEARCH","READWRITE","PUNSUBSCRIBE","ZREMRANGEBYRANK","RPUSHX","PERSIST","JSON","FORGET","DEL","ZLEXCOUNT","ZUNION","JSON","TOGGLE","JSON","OBJKEYS","FT",
       "CREATE","HINCRBY","BITPOS","AUTH","LASTSAVE","SDIFF","PEXPIRE","STICK","EVAL","ACL","SAVE","FT","INFO","EVALSHA","PSUBSCRIBE","JSON","STRLEN","BITFIELD","RO","READONLY","PUBSUB","MEMORY","LINSERT","ZREMRANGEBYSCORE","PTTL","FIELDTTL"
   ];

   return commands;
 } catch (e) {
   console.error('Error fetching commands:', e);
 }
}


/**
* Functions to extract stats from the different Dragonfly commands.
* Info all
* debug memory
*/
// holds all the stats data;
globalStats = {
   "cpu": [],
   "qps": [],
   "used_memory_bytes": [],
   "max_memory_bytes": [],
   "num_connected_clients": [],
   "uptime": [],
   "total_commands_num": [],
   "hit_rate": [],
   "shards_stats": []
};

function parseRedisInfo(text) {
   const lines = text.split('\r\n');
   const result = {};

   let currentSection = null;
   for (const line of lines) {
       if (line.startsWith('#')) {
           currentSection = line.substring(2).trim(); // Extract section name
           result[currentSection] = {};
       } else if (line.trim()) {
           const [key, value] = line.split(':');
           result[currentSection][key] = value;
       }
   }

   return result;
 }

async function updateStats() {
   const info = await fetchData("info all");
   const data = parseRedisInfo(info.result);

   globalStats.qps.push(data.Stats.instantaneous_ops_per_sec);
   globalStats.num_connected_clients.push(data.Clients.connected_clients);
   globalStats.used_memory_bytes.push(data.Memory.used_memory);
   globalStats.max_memory_bytes.push(data.Memory.maxmemory);
   globalStats.uptime.push(formatTime(data.Server.uptime_in_seconds));
   globalStats.cpu.push(data.Cpu.used_cpu_sys);
   globalStats.total_commands_num.push(data.Stats.total_commands_processed);
   if (data.Stats.keyspace_hits > 0) {
     globalStats.hit_rate.push((data.Stats.keyspace_hits / (data.Stats.keyspace_hits + data.Stats.keyspace_misses)) * 100);
   } else {
     globalStats.hit_rate.push(0);
   }
}

/**
* Shards related functions
*/

function parseShardInfo(text) {
 const lines = text.split('\n');
 const data = {};

 // Create a regular expression to match shard lines
 const shardRegex = /^shard(\d+)_(\w+): (\d+)$/;

 for (const line of lines) {
   const match = line.match(shardRegex);
   if (match) {
     const shardNum = match[1];
     const property = match[2];
     const value = parseInt(match[3], 10); // Parse value as a number

     // Create a nested structure for shards
     if (!data[shardNum]) {
       data[shardNum] = {};
     }
     data[shardNum][property] = value;
   } else {
     // Handle non-shard properties
     const [key, value] = line.split(': ');
     data[key] = parseInt(value, 10) || value; // Parse as number if possible
   }
 }
 return data;
}

async function updateShardStats() {
 const stats = await fetchData("debug shards");
 shards = parseShardInfo(stats.result);
 globalStats.shards_stats["used_memory"] = [];
 globalStats.shards_stats["key_count"] = [];
 globalStats.shards_stats["expire_count"] = [];
 globalStats.shards_stats["key_reads"] = [];

 for (i=0; i < shards.num_shards; i++) {
   globalStats.shards_stats["used_memory"][i] = shards[i].used_memory;
   globalStats.shards_stats["key_count"][i] = shards[i].key_count;
   globalStats.shards_stats["expire_count"][i] = shards[i].expire_count;
   globalStats.shards_stats["key_reads"][i] = shards[i].key_reads;
 }
}