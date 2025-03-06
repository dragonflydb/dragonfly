// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/facade_test.h"

#include <absl/strings/match.h>
#include <absl/strings/numbers.h>

#include "base/logging.h"

namespace facade {

using namespace testing;
using namespace std;

bool RespMatcher::MatchAndExplain(RespExpr e, MatchResultListener* listener) const {
  if (e.type != type_) {
    if (e.type == RespExpr::STRING && type_ == RespExpr::DOUBLE) {
      // Doubles are encoded as strings, unless RESP3 is selected. So parse string and try to
      // compare it.
      double d = 0;
      if (!absl::SimpleAtod(e.GetString(), &d)) {
        *listener << "\nCan't parse as double: " << e.GetString();
        return false;
      }
      e.type = RespExpr::DOUBLE;
      e.u = d;
    } else {
      *listener << "\nWrong type: " << RespExpr::TypeName(e.type);
      return false;
    }
  }

  if (type_ == RespExpr::STRING || type_ == RespExpr::ERROR) {
    RespExpr::Buffer ebuf = e.GetBuf();
    std::string_view actual{reinterpret_cast<char*>(ebuf.data()), ebuf.size()};

    if (type_ == RespExpr::ERROR && !absl::StrContains(actual, exp_str_)) {
      *listener << "Actual does not contain '" << exp_str_ << "'";
      return false;
    }
    if (type_ == RespExpr::STRING && exp_str_ != actual) {
      *listener << "\nActual string: " << actual;
      return false;
    }
  } else if (type_ == RespExpr::INT64) {
    auto actual = get<int64_t>(e.u);
    if (exp_int_ != actual) {
      *listener << "\nActual : " << actual << " expected: " << exp_int_;
      return false;
    }
  } else if (type_ == RespExpr::DOUBLE) {
    auto actual = get<double>(e.u);
    if (abs(exp_double_ - actual) > 0.0001) {
      *listener << "\nActual : " << actual << " expected: " << exp_double_;
      return false;
    }
  } else if (type_ == RespExpr::ARRAY) {
    size_t len = get<RespVec*>(e.u)->size();
    if (len != size_t(exp_int_)) {
      *listener << "Actual length " << len << ", expected: " << exp_int_;
      return false;
    }
  }

  return true;
}

void RespMatcher::DescribeTo(std::ostream* os) const {
  *os << "is ";
  switch (type_) {
    case RespExpr::STRING:
    case RespExpr::ERROR:
      *os << exp_str_;
      break;

    case RespExpr::INT64:
      *os << exp_str_;
      break;
    case RespExpr::ARRAY:
      *os << "array of length " << exp_int_;
      break;
    case RespExpr::DOUBLE:
      *os << exp_double_;
      break;
    default:
      *os << "TBD";
      break;
  }
}

void RespMatcher::DescribeNegationTo(std::ostream* os) const {
  *os << "is not ";
}

bool RespTypeMatcher::MatchAndExplain(const RespExpr& e, MatchResultListener* listener) const {
  if (e.type != type_) {
    *listener << "\nWrong type: " << RespExpr::TypeName(e.type);
    return false;
  }

  return true;
}

void RespTypeMatcher::DescribeTo(std::ostream* os) const {
  *os << "is " << RespExpr::TypeName(type_);
}

void RespTypeMatcher::DescribeNegationTo(std::ostream* os) const {
  *os << "is not " << RespExpr::TypeName(type_);
}

void PrintTo(const RespExpr::Vec& vec, std::ostream* os) {
  *os << "Vec: [";
  if (!vec.empty()) {
    for (size_t i = 0; i < vec.size() - 1; ++i) {
      *os << vec[i] << ",";
    }
    *os << vec.back();
  }
  *os << "]\n";
}

}  // namespace facade
