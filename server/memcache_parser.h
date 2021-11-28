// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string_view>
#include <vector>

namespace dfly {

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

    // Delete and INCR
    DELETE = 21,
    INCR = 22,
    DECR = 23,
  };

  struct Command {
    CmdType type = INVALID;
    std::string_view key;
    std::vector<std::string_view> keys_ext;

    uint64_t cas_unique = 0;
    uint32_t expire_ts = 0;
    uint32_t bytes_len = 0;
    uint16_t flags = 0;
    bool no_reply = false;
  };

  enum Result {
    OK,
    INPUT_PENDING,
    UNKNOWN_CMD,
    BAD_INT,
    PARSE_ERROR,
  };

  static bool IsStoreCmd(CmdType type) {
    return type >= SET && type <= CAS;
  }

  Result Parse(std::string_view str, uint32_t* consumed, Command* res);

 private:
};

}  // namespace dfly