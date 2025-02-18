// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <reflex/matcher.h>

#include <string>
#include <string_view>

namespace dfly {

class GlobMatcher {
  GlobMatcher(const GlobMatcher&) = delete;
  GlobMatcher& operator=(const GlobMatcher&) = delete;

 public:
  explicit GlobMatcher(std::string_view pattern, bool case_sensitive);

  bool Matches(std::string_view str) const;

  // Exposed for testing purposes.
  static std::string Glob2Regex(std::string_view glob);

 private:
  mutable reflex::Matcher matcher_;

  bool case_sensitive_;
  bool starts_with_star_ = false;
  bool ends_with_star_ = false;
  bool empty_pattern_ = false;
};

}  // namespace dfly
