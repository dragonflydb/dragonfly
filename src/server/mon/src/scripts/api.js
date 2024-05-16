async function fetchData(cmd, isJson = true) {
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
  
      let reply = null;
      if (isJson) {
        reply = await response.json();
      } else {
        reply = await response.text();
      }
      return reply;
  } catch (e) {
    console.log("cannot fetch data for command ", cmd, isJson, e);
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
  const text = await fetchData("command", false);
  return parseCommands(text);  
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
  "shards_stats": {
    "used_memory": [],
    "key_count": [],
    "expire_count": [],
    "key_reads": []
  }
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
    let hit = parseInt(data.Stats.keyspace_hits, 10);
    let miss = parseInt(data.Stats.keyspace_misses, 10);
    let tmp = (hit / (hit + miss)) * 100;
    tmp = Math.round(100 * (tmp)) / 100;
    globalStats.hit_rate.push(tmp);
  } else {
    globalStats.hit_rate.push(0);
  }

  for (const [key, value] of Object.entries(globalStats)) {
    if (key != "shards_stats" && globalStats[key].length > HISTORY_WINDOW) {
      globalStats[key].shift();
    }
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

  for (i = 0; i < shards.num_shards; i++) {
    globalStats.shards_stats["used_memory"].push(shards[i].used_memory);
    globalStats.shards_stats["key_count"].push(shards[i].key_count);
    globalStats.shards_stats["expire_count"].push(shards[i].expire_count);
    globalStats.shards_stats["key_reads"].push(shards[i].key_reads);
  }
}

/**
* Slowlog related functions
*/

// Example of the str: "1510171586325711220"
function extractTimeAndDuration(str) {
  const index = str.indexOf("17");
  const timeStamp = str.slice(index, index + 10);
  const duration = str.slice(index+10);
  return [timeStamp, duration];
}
  
/**
* Return an Array of slow commands in the following format
* [] if empty
* [{time, duration, command, key}, {time, duration, command, key} ...]
*/ 
function parseSlowLog(text) {
  const str = text.slice(11, -4); // remove {"result":[ and the """} from the end
  const lines = str.replaceAll('[','').split('"""'); // remove [ and split to lines according to """
  const result = [];
  lines.forEach(line => {
      const parts = line.replaceAll('""','"').split('"');
      const timeDuration = extractTimeAndDuration(parts[0]);
      result.push({
        "time" : timeDuration[0],
        "duration" : timeDuration[1],
        "command" : parts[1],
        "key" : parts[2]
      });
  });
  return result;
}
  
async function fetchSlowLog() {
  const res = await fetchData("slowlog get 50", false);
  return parseSlowLog(res);
}