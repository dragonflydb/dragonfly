// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/glob_matcher.h"

extern "C" {
#include "redis/util.h"
}

namespace dfly {

GlobMatcher::GlobMatcher(std::string_view pattern, bool case_sensitive)
    : pattern_(pattern), case_sensitive_(case_sensitive) {
}

bool GlobMatcher::Matches(std::string_view str) const {
  return stringmatchlen(pattern_.data(), pattern_.size(), str.data(), str.size(),
                        int(!case_sensitive_)) != 0;
}

}  // namespace dfly
