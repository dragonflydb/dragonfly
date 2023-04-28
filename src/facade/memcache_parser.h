// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <string_view>
#include <vector>

namespace facade {

// Memcache parser does not parse value blobs, only the commands.
// The expectation is that the caller will parse the command and
// then will follow up with reading the blob data directly from source.
class MemcacheParser {
 public:
  enum CmdType {
    INVALID = 0,
    SET = 1,
    ADD = 2,
    REPLACE = 3,
    APPEND = 4,
    PREPEND = 5,
    CAS = 6,

    // Retrieval
    GET = 10,
    GETS = 11,
    GAT = 12,
    GATS = 13,
    STATS = 14,

    QUIT = 20,
    VERSION = 21,

    // The rest of write commands.
    DELETE = 31,
    INCR = 32,
    DECR = 33,
    FLUSHALL = 34,
  };

  // According to https://github.com/memcached/memcached/wiki/Commands#standard-protocol
  struct Command {
    CmdType type = INVALID;
    std::string_view key;
    std::vector<std::string_view> keys_ext;

    union {
      uint64_t cas_unique = 0;  // for CAS COMMAND
      uint64_t delta;          // for DECR/INCR commands.
    };

    uint32_t expire_ts = 0;  // relative time in seconds.
    uint32_t bytes_len = 0;
    uint32_t flags = 0;
    bool no_reply = false;
  };

  enum Result {
    OK,
    INPUT_PENDING,
    UNKNOWN_CMD,
    BAD_INT,
    PARSE_ERROR,
    BAD_DELTA,
  };

  static bool IsStoreCmd(CmdType type) {
    return type >= SET && type <= CAS;
  }

  Result Parse(std::string_view str, uint32_t* consumed, Command* res);

 private:
};

}  // namespace dfly
