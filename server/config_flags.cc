// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "server/config_flags.h"

namespace dfly {

bool ValidateConfigEnum(const char* nm, const std::string& val, const ConfigEnum* ptr, unsigned len,
                     int* dest) {
  for (unsigned i = 0; i < len; ++i) {
    if (val == ptr[i].first) {
      *dest = ptr[i].second;
      return true;
    }
  }
  return false;
}

}  // namespace dfly
