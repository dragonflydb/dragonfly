// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <string_view>

namespace dfly {

class GlobMatcher {
 public:
  explicit GlobMatcher(std::string_view pattern, bool case_sensitive);

  bool Matches(std::string_view str) const;

 private:
  std::string_view pattern_;
  bool case_sensitive_;
};

}  // namespace dfly
