// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <optional>
#include <string_view>

#include "facade/facade_types.h"

namespace dfly {

using CompactObjType = unsigned;
class GlobMatcher;

using facade::CmdArgList;
using facade::OpResult;

struct ScanOpts {
  ~ScanOpts();  // because of forward declaration
  ScanOpts() = default;
  ScanOpts(ScanOpts&& other) = default;

  bool Matches(std::string_view val_name) const;
  static OpResult<ScanOpts> TryFrom(CmdArgList args, bool allow_novalues = false);

  std::unique_ptr<GlobMatcher> matcher;
  size_t limit = 10;
  std::optional<CompactObjType> type_filter;
  unsigned bucket_id = UINT_MAX;
  enum class Mask {
    Volatile,   // volatile, keys that have ttl
    Permanent,  // permanent, keys that do not have ttl
    Accessed,   // accessed, the key has been accessed since the last load/flush event, or the last
                // time a flag was reset.
    Untouched,  // untouched, the key has not been accessed/touched.
  };
  std::optional<Mask> mask;
  size_t min_malloc_size = 0;
  bool novalues = false;
};

}  // namespace dfly
