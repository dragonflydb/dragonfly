// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <gmock/gmock.h>

#include "io/io.h"
#include "server/redis_parser.h"
#include "util/proactor_pool.h"

namespace dfly {

class RespMatcher {
 public:
  RespMatcher(std::string_view val, RespExpr::Type t = RespExpr::STRING) : type_(t), exp_str_(val) {
  }

  RespMatcher(int64_t val, RespExpr::Type t = RespExpr::INT64)
      : type_(t), exp_int_(val) {
  }

  using is_gtest_matcher = void;

  bool MatchAndExplain(const RespExpr& e, testing::MatchResultListener*) const;

  void DescribeTo(std::ostream* os) const;

  void DescribeNegationTo(std::ostream* os) const;

 private:
  RespExpr::Type type_;

  std::string exp_str_;
  int64_t exp_int_;
};

class RespTypeMatcher {
 public:
  RespTypeMatcher(RespExpr::Type type) : type_(type) {
  }

  using is_gtest_matcher = void;

  bool MatchAndExplain(const RespExpr& e, testing::MatchResultListener*) const;

  void DescribeTo(std::ostream* os) const;

  void DescribeNegationTo(std::ostream* os) const;

 private:
  RespExpr::Type type_;
};

inline ::testing::PolymorphicMatcher<RespMatcher> StrArg(std::string_view str) {
  return ::testing::MakePolymorphicMatcher(RespMatcher(str));
}

inline ::testing::PolymorphicMatcher<RespMatcher> ErrArg(std::string_view str) {
  return ::testing::MakePolymorphicMatcher(RespMatcher(str, RespExpr::ERROR));
}

inline ::testing::PolymorphicMatcher<RespMatcher> IntArg(int64_t ival) {
  return ::testing::MakePolymorphicMatcher(RespMatcher(ival));
}

inline ::testing::PolymorphicMatcher<RespMatcher> ArrLen(size_t len) {
  return ::testing::MakePolymorphicMatcher(RespMatcher(len, RespExpr::ARRAY));
}

inline ::testing::PolymorphicMatcher<RespTypeMatcher> ArgType(RespExpr::Type t) {
  return ::testing::MakePolymorphicMatcher(RespTypeMatcher(t));
}

inline bool operator==(const RespExpr& left, const char* s) {
  return left.type == RespExpr::STRING && ToSV(left.GetBuf()) == s;
}

void PrintTo(const RespExpr::Vec& vec, std::ostream* os);

MATCHER_P(RespEq, val, "") {
  return ::testing::ExplainMatchResult(::testing::ElementsAre(StrArg(val)), arg, result_listener);
}

}  // namespace dfly
