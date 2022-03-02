// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <string>

#include "facade/error.h"

namespace dfly {

using facade::kWrongTypeErr;
using facade::kInvalidIntErr;
using facade::kSyntaxErr;
using facade::kInvalidDbIndErr;
using facade::kDbIndOutOfRangeErr;

#ifndef RETURN_ON_ERR

#define RETURN_ON_ERR(x) \
  do {                   \
    auto ec = (x);       \
    if (ec)              \
      return ec;         \
  } while (0)

#endif  // RETURN_ON_ERR

namespace rdb {

enum errc {
  wrong_signature = 1,
  bad_version = 2,
  module_not_supported = 3,
  duplicate_key = 4,
  rdb_file_corrupted = 5,
  bad_checksum = 6,
  bad_db_index = 7,
  invalid_rdb_type = 8,
  invalid_encoding = 9,
};

}  // namespace rdb

}  // namespace dfly
