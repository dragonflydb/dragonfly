// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <string>

#include "facade/error.h"

namespace dfly {

using facade::kDbIndOutOfRangeErr;
using facade::kInvalidDbIndErr;
using facade::kInvalidIntErr;
using facade::kSyntaxErr;
using facade::kWrongTypeErr;

#ifndef RETURN_ON_ERR

#define RETURN_ON_ERR(x)                                      \
  do {                                                        \
    auto __ec = (x);                                          \
    if (__ec) {                                               \
      LOG(ERROR) << "Error " << __ec << " while calling " #x; \
      return __ec;                                            \
    }                                                         \
  } while (0)

#endif  // RETURN_ON_ERR

namespace rdb {

enum errc {
  wrong_signature = 1,
  bad_version = 2,
  feature_not_supported = 3,
  duplicate_key = 4,
  rdb_file_corrupted = 5,
  bad_checksum = 6,
  bad_db_index = 7,
  invalid_rdb_type = 8,
  invalid_encoding = 9,
  empty_key = 10,
  out_of_memory = 11,
};

}  // namespace rdb

}  // namespace dfly
