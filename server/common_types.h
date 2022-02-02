// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/strings/ascii.h>
#include <absl/types/span.h>

#include <string_view>
#include <vector>

namespace dfly {

enum class ListDir : uint8_t { LEFT, RIGHT };

enum class Protocol : uint8_t {
  MEMCACHE = 1,
  REDIS = 2
};

using DbIndex = uint16_t;
using ShardId = uint16_t;
using TxId = uint64_t;
using TxClock = uint64_t;

using ArgSlice = absl::Span<const std::string_view>;
using MutableSlice = absl::Span<char>;
using CmdArgList = absl::Span<MutableSlice>;
using CmdArgVec = std::vector<MutableSlice>;

constexpr DbIndex kInvalidDbId = DbIndex(-1);
constexpr ShardId kInvalidSid = ShardId(-1);
constexpr DbIndex kMaxDbId = 1024;  // Reasonable starting point.

class CommandId;
class Transaction;
class EngineShard;

struct KeyLockArgs {
  DbIndex db_index;
  ArgSlice args;
  unsigned key_step;
};


struct ConnectionStats {
  uint32_t num_conns = 0;
  uint32_t num_replicas = 0;
  size_t read_buf_capacity = 0;
  size_t io_reads_cnt = 0;
  size_t command_cnt = 0;
  size_t pipelined_cmd_cnt = 0;

  ConnectionStats& operator+=(const ConnectionStats& o);
};

struct OpArgs {
  EngineShard* shard;
  DbIndex db_ind;
};

constexpr inline unsigned long long operator""_MB(unsigned long long x) {
  return 1024L * 1024L * x;
}

constexpr inline unsigned long long operator""_KB(unsigned long long x) {
  return 1024L * x;
}

inline std::string_view ArgS(CmdArgList args, size_t i) {
  auto arg = args[i];
  return std::string_view(arg.data(), arg.size());
}

inline MutableSlice ToMSS(absl::Span<uint8_t> span) {
  return MutableSlice{reinterpret_cast<char*>(span.data()), span.size()};
}

inline void ToUpper(const MutableSlice* val) {
  for (auto& c : *val) {
    c = absl::ascii_toupper(c);
  }
}

}  // namespace dfly

namespace std {
ostream& operator<<(ostream& os, dfly::CmdArgList args);

}  // namespace std
