// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/common_types.h"

#include <absl/strings/str_cat.h>

#include "base/logging.h"
#include "server/error.h"

namespace dfly {

using std::string;


string WrongNumArgsError(std::string_view cmd) {
  return absl::StrCat("wrong number of arguments for '", cmd, "' command");
}

const char kSyntaxErr[] = "syntax error";
const char kInvalidIntErr[] = "value is not an integer or out of range";
const char kUintErr[] = "value is out of range, must be positive";

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