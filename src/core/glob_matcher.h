// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <reflex/matcher.h>

#include <string>
#include <string_view>

// We opt for using Reflex library for glob matching.
// While I find PCRE2 faster, it's not substantially faster to justify the shared lib dependency.
#define REFLEX_PERFORMANCE

#ifndef REFLEX_PERFORMANCE
#ifdef USE_PCRE2
  #define PCRE2_CODE_UNIT_WIDTH 8
  #include <pcre2.h>
#endif
#endif


namespace dfly {

class GlobMatcher {
  GlobMatcher(const GlobMatcher&) = delete;
  GlobMatcher& operator=(const GlobMatcher&) = delete;

 public:
  explicit GlobMatcher(std::string_view pattern, bool case_sensitive);
  ~GlobMatcher();

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
#ifdef REFLEX_PERFORMANCE
  mutable reflex::Matcher matcher_;

  bool starts_with_star_ = false;
  bool ends_with_star_ = false;
#elif USE_PCRE2
  pcre2_code_8* re_ = nullptr;
  pcre2_match_data_8* match_data_ = nullptr;
#endif
  std::string_view glob_;
  bool case_sensitive_;
};

}  // namespace dfly
