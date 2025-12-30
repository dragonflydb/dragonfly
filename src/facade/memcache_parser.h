// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "common/backed_args.h"
#include "facade/facade_types.h"

namespace facade {

// Memcache parser does not parse value blobs, only the commands.
// The expectation is that the caller will parse the command and
// then will follow up with reading the blob data directly from source.
class MemcacheParser {
 public:
  enum CmdType : uint8_t {
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

    // META_COMMANDS
    META_NOOP = 50,
    META_SET = 51,
    META_DEL = 52,
    META_ARITHM = 53,
    META_GET = 54,
    META_DEBUG = 55,
  };

  // According to https://github.com/memcached/memcached/wiki/Commands#standard-protocol
  struct Command {
    Command() = default;
    Command(const Command&) = delete;
    Command(Command&&) noexcept = default;

    CmdType type = INVALID;

    std::string_view key() const {
      return backed_args->empty() ? std::string_view{} : backed_args->Front();
    }

    // For STORE commands, value is at index 1.
    // For both key and value we provide convenience accessors that return empty string_view
    // if not present.
    std::string_view value() const {
      return backed_args->size() < 2 ? std::string_view{} : backed_args->at(1);
    }

    size_t size() const {
      return backed_args->size();
    }

    char* value_ptr() {  // NOLINT
      return backed_args->data(1);
    }

    union {
      uint64_t cas_unique = 0;  // for CAS COMMAND
      uint64_t delta;           // for DECR/INCR commands.
    };

    int64_t expire_ts = 0;  // unix time (expire_ts > month) in seconds

    // flags for STORE commands
    uint32_t flags = 0;

    MemcacheCmdFlags cmd_flags;

    // Does not own this object, only references it.
    cmn::BackedArguments* backed_args = nullptr;
  };

  static_assert(sizeof(Command) == 40);

  enum Result : uint8_t {
    OK,
    INPUT_PENDING,
    UNKNOWN_CMD,
    BAD_INT,
    PARSE_ERROR,  // request parse error, but can continue parsing within the same connection.
    BAD_DELTA,
  };

  static bool IsStoreCmd(CmdType type) {
    return type >= SET && type <= CAS;
  }

  size_t UsedMemory() const {
    return tmp_buf_.capacity();
  }

  void Reset() {
    val_len_to_read_ = 0;
    tmp_buf_.clear();
  }

  Result Parse(std::string_view str, uint32_t* consumed, Command* res);

  void set_last_unix_time(int64_t t) {
    last_unix_time_ = t;
  }

 private:
  Result ConsumeValue(std::string_view str, uint32_t* consumed, Command* dest);
  Result ParseInternal(ArgSlice tokens_view, Command* cmd);

  uint32_t val_len_to_read_ = 0;
  std::string tmp_buf_;
  int64_t last_unix_time_ = 0;
};

}  // namespace facade
