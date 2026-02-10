// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "facade/facade_types.h"

namespace dfly {

using CompactObjType = unsigned;

using facade::ArgS;
using facade::CmdArgList;
using facade::CmdArgVec;
using facade::MutableSlice;
using facade::OpResult;

using StringVec = std::vector<std::string>;

// Dependent on ExpirePeriod representation of the value.
constexpr int64_t kMaxExpireDeadlineSec = (1u << 28) - 1;  // 8.5 years
constexpr int64_t kMaxExpireDeadlineMs = kMaxExpireDeadlineSec * 1000;

enum class TimeUnit : uint8_t { SEC, MSEC };

enum ExpireFlags {
  EXPIRE_ALWAYS = 0,
  EXPIRE_NX = 1 << 0,  // Set expiry only when key has no expiry
  EXPIRE_XX = 1 << 2,  // Set expiry only when the key has expiry
  EXPIRE_GT = 1 << 3,  // GT: Set expiry only when the new expiry is greater than current one
  EXPIRE_LT = 1 << 4,  // LT: Set expiry only when the new expiry is less than current one
};

// I use relative time from Feb 1, 2023 in seconds.
constexpr uint64_t kMemberExpiryBase = 1675209600;

inline uint32_t MemberTimeSeconds(uint64_t now_ms) {
  return (now_ms / 1000) - kMemberExpiryBase;
}

bool ParseHumanReadableBytes(std::string_view str, int64_t* num_bytes);
bool ParseDouble(std::string_view src, double* value);

const char* RdbTypeName(unsigned type);

}  // namespace dfly
