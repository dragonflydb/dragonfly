// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/strings/ascii.h>
#include <absl/types/span.h>

#include <string_view>
#include <vector>

#include "facade/facade_types.h"

namespace dfly {

enum class ListDir : uint8_t { LEFT, RIGHT };

// Dependent on ExpirePeriod representation of the value.
constexpr int64_t kMaxExpireDeadlineSec = (1u << 27) - 1;

using DbIndex = uint16_t;
using ShardId = uint16_t;
using TxId = uint64_t;
using TxClock = uint64_t;

using facade::MutableSlice;
using facade::CmdArgList;
using facade::CmdArgVec;
using facade::ArgS;

using ArgSlice = absl::Span<const std::string_view>;
using StringVec = std::vector<std::string>;

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

// Describes key indices.
struct KeyIndex {
  unsigned start;
  unsigned end;  // does not include this index (open limit).
  unsigned step; // 1 for commands like mget. 2 for commands like mset.
};

struct OpArgs {
  EngineShard* shard;
  DbIndex db_ind;
};


struct TieredStats {
  size_t external_reads = 0;
  size_t external_writes = 0;

  TieredStats& operator+=(const TieredStats&);
};


inline void ToUpper(const MutableSlice* val) {
  for (auto& c : *val) {
    c = absl::ascii_toupper(c);
  }
}

inline void ToLower(const MutableSlice* val) {
  for (auto& c : *val) {
    c = absl::ascii_tolower(c);
  }
}

bool ParseHumanReadableBytes(std::string_view str, int64_t* num_bytes);

// Cached values, updated frequently to represent the correct state of the system.
extern std::atomic_uint64_t used_mem_peak;
extern std::atomic_uint64_t used_mem_current;

// version 5.11 maps to 511 etc.
// set upon server start.
extern unsigned kernel_version;

}  // namespace dfly
