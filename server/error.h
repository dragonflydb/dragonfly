// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>

namespace dfly {

std::string WrongNumArgsError(std::string_view cmd);

extern const char kSyntaxErr[];
extern const char kWrongTypeErr[];
extern const char kKeyNotFoundErr[];
extern const char kInvalidIntErr[];
extern const char kUintErr[];
extern const char kDbIndOutOfRangeErr[];
extern const char kInvalidDbIndErr[];

#ifndef RETURN_ON_ERR

#define RETURN_ON_ERR(x) \
  do {                   \
    auto ec = (x);       \
    if (ec)              \
      return ec;         \
  } while (0)
#endif

}  // namespace dfly
