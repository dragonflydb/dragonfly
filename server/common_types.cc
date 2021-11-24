// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "server/common_types.h"

#include <absl/strings/str_cat.h>

#include "base/logging.h"

namespace dfly {

using std::string;


string WrongNumArgsError(std::string_view cmd) {
  return absl::StrCat("wrong number of arguments for '", cmd, "' command");
}

const char kSyntaxErr[] = "syntax error";
const char kWrongTypeErr[] = "-WRONGTYPE Operation against a key holding the wrong kind of value";
const char kKeyNotFoundErr[] = "no such key";
const char kInvalidIntErr[] = "value is not an integer or out of range";
const char kUintErr[] = "value is out of range, must be positive";
const char kInvalidFloatErr[] = "value is not a valid float";
const char kInvalidScoreErr[] = "resulting score is not a number (NaN)";
const char kDbIndOutOfRangeErr[] = "DB index is out of range";
const char kInvalidDbIndErr[] = "invalid DB index";
const char kSameObjErr[] = "source and destination objects are the same";

}  // namespace dfly

namespace std {

ostream& operator<<(ostream& os, dfly::CmdArgList ras) {
  os << "[";
  if (!ras.empty()) {
    for (size_t i = 0; i < ras.size() - 1; ++i) {
      os << dfly::ArgS(ras, i) << ",";
    }
    os << dfly::ArgS(ras, ras.size() - 1);
  }
  os << "]";

  return os;
}

}  // namespace std