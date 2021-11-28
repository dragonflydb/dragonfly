// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/strings/ascii.h>
#include <absl/types/span.h>
#include <xxhash.h>

#include <string_view>
#include <vector>

namespace dfly {

using DbIndex = uint16_t;
using ShardId = uint16_t;

using MutableStrSpan = absl::Span<char>;
using CmdArgList = absl::Span<MutableStrSpan>;
using CmdArgVec = std::vector<MutableStrSpan>;

constexpr DbIndex kInvalidDbId = DbIndex(-1);
constexpr ShardId kInvalidSid = ShardId(-1);

struct ConnectionState {
  enum Mask : uint32_t {
    ASYNC_DISPATCH = 1,  // whether a command is handled via async dispatch.
    CONN_CLOSING = 2,    // could be because of unrecoverable error or planned action.
  };

  uint32_t mask = 0;  // A bitmask of Mask values.

  bool IsClosing() const {
    return mask & CONN_CLOSING;
  }

  bool IsRunViaDispatch() const {
    return mask & ASYNC_DISPATCH;
  }
};

template <typename View> inline ShardId Shard(const View& v, ShardId shard_num) {
  XXH64_hash_t hash = XXH64(v.data(), v.size(), 120577);
  return hash % shard_num;
}

using MainValue = std::string;
using MainTable = absl::flat_hash_map<std::string, MainValue>;
using MainIterator = MainTable::iterator;

class EngineShard;

inline std::string_view ArgS(CmdArgList args, size_t i) {
  auto arg = args[i];
  return std::string_view(arg.data(), arg.size());
}

inline void ToUpper(const MutableStrSpan* val) {
  for (auto& c : *val) {
    c = absl::ascii_toupper(c);
  }
}

inline MutableStrSpan ToMSS(absl::Span<uint8_t> span) {
  return MutableStrSpan{reinterpret_cast<char*>(span.data()), span.size()};
}

std::string WrongNumArgsError(std::string_view cmd);

}  // namespace dfly

namespace std {
ostream& operator<<(ostream& os, dfly::CmdArgList args);

}  // namespace std