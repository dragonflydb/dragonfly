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
 // TODO: we fix the problem of stringmatchlen being much
 // faster when the result is immediately known to be false, for example: "a*" vs "bxxxxx".
 // The goal is to demonstrate on-par performance for the following case:
 // > debug populate 5000000 keys 32 RAND
 // > while true; do time valkey-cli scan 0 match 'foo*bar'; done
 // Also demonstrate that the "improved" performance via SCAN command and not only via
 // micro-benchmark.
 // The performance of naive algorithm becomes worse in cases where string is long enough,
 // and the pattern has a star at the start (or it matches at first).
#ifdef FIX_PERFORMANCE_MATCHING
  mutable reflex::Matcher matcher_;

  bool starts_with_star_ = false;
  bool ends_with_star_ = false;
  bool empty_pattern_ = false;
#else
  std::string glob_;
#endif
  bool case_sensitive_;
};

}  // namespace dfly
