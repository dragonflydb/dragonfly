// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <gmock/gmock.h>

#include <ostream>
#include <string>
#include <string_view>

#include "facade/resp_expr.h"

namespace facade {

class RespMatcher {
 public:
  RespMatcher(std::string_view val, RespExpr::Type t = RespExpr::STRING) : type_(t), exp_str_(val) {
  }

  RespMatcher(int64_t val, RespExpr::Type t = RespExpr::INT64) : type_(t), exp_int_(val) {
  }

  RespMatcher(double_t val, RespExpr::Type t = RespExpr::DOUBLE) : type_(t), exp_double_(val) {
  }
  using is_gtest_matcher = void;

  bool MatchAndExplain(RespExpr e, testing::MatchResultListener*) const;

  void DescribeTo(std::ostream* os) const;

  void DescribeNegationTo(std::ostream* os) const;

 private:
  RespExpr::Type type_;

  std::string exp_str_;
  int64_t exp_int_ = 0;
  double_t exp_double_ = 0;
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

inline ::testing::PolymorphicMatcher<RespMatcher> ErrArg(std::string_view str) {
  return ::testing::MakePolymorphicMatcher(RespMatcher(str, RespExpr::ERROR));
}

inline ::testing::PolymorphicMatcher<RespMatcher> IntArg(int64_t ival) {
  return ::testing::MakePolymorphicMatcher(RespMatcher(ival));
}

inline ::testing::PolymorphicMatcher<RespMatcher> DoubleArg(double_t dval) {
  return ::testing::MakePolymorphicMatcher(RespMatcher(dval));
}

inline ::testing::PolymorphicMatcher<RespMatcher> ArrLen(size_t len) {
  return ::testing::MakePolymorphicMatcher(RespMatcher((int64_t)len, RespExpr::ARRAY));
}

inline ::testing::PolymorphicMatcher<RespTypeMatcher> ArgType(RespExpr::Type t) {
  return ::testing::MakePolymorphicMatcher(RespTypeMatcher(t));
}

MATCHER_P(RespArray, value, "") {
  return ExplainMatchResult(
      testing::AllOf(ArgType(RespExpr::ARRAY), testing::Property(&RespExpr::GetVec, value)), arg,
      result_listener);
}

template <typename... Args> auto RespElementsAre(const Args&... matchers) {
  return RespArray(::testing::ElementsAre(matchers...));
}

inline bool operator==(const RespExpr& left, std::string_view s) {
  return left.type == RespExpr::STRING && ToSV(left.GetBuf()) == s;
}

inline bool operator==(const RespExpr& left, int64_t val) {
  return left.type == RespExpr::INT64 && left.GetInt() == val;
}

inline bool operator!=(const RespExpr& left, std::string_view s) {
  return !(left == s);
}

inline bool operator==(std::string_view s, const RespExpr& right) {
  return right == s;
}

inline bool operator!=(std::string_view s, const RespExpr& right) {
  return !(right == s);
}

void PrintTo(const RespExpr::Vec& vec, std::ostream* os);

}  // namespace facade
