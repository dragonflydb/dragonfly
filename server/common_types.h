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
using MutableStrSpan = absl::Span<char>;
using CmdArgList = absl::Span<MutableStrSpan>;
using CmdArgVec = std::vector<MutableStrSpan>;

constexpr DbIndex kInvalidDbId = DbIndex(-1);
constexpr ShardId kInvalidSid = ShardId(-1);

class CommandId;
class Transaction;
class EngineShard;

struct KeyLockArgs {
  DbIndex db_index;
  ArgSlice args;
  unsigned key_step;
};

struct OpArgs {
  EngineShard* shard;
  DbIndex db_ind;
};

inline std::string_view ArgS(CmdArgList args, size_t i) {
  auto arg = args[i];
  return std::string_view(arg.data(), arg.size());
}

inline MutableStrSpan ToMSS(absl::Span<uint8_t> span) {
  return MutableStrSpan{reinterpret_cast<char*>(span.data()), span.size()};
}

inline void ToUpper(const MutableStrSpan* val) {
  for (auto& c : *val) {
    c = absl::ascii_toupper(c);
  }
}

}  // namespace dfly

namespace std {
ostream& operator<<(ostream& os, dfly::CmdArgList args);

}  // namespace std
