// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <string>
#include <system_error>

#include "facade/error.h"

namespace dfly {

using facade::kDbIndOutOfRangeErr;
using facade::kInvalidDbIndErr;
using facade::kInvalidIntErr;
using facade::kSyntaxErr;
using facade::kWrongTypeErr;

#ifndef RETURN_ON_ERR

#define RETURN_ON_ERR_T(T, x)                                          \
  do {                                                                 \
    std::error_code __ec = (x);                                        \
    if (__ec) {                                                        \
      DLOG(ERROR) << "Error while calling " #x ": " << __ec.message(); \
      return (T)(__ec);                                                \
    }                                                                  \
  } while (0)

#define RETURN_ON_ERR(x) RETURN_ON_ERR_T(std::error_code, x)

#endif  // RETURN_ON_ERR

#ifndef RETURN_ON_BAD_STATUS

#define RETURN_ON_BAD_STATUS(x)  \
  do {                           \
    OpStatus __s = (x).status(); \
    if (__s != OpStatus::OK) {   \
      return __s;                \
    }                            \
  } while (0)

#endif  // RETURN_ON_BAD_STATUS

#ifndef GET_OR_SEND_UNEXPECTED

#define GET_OR_SEND_UNEXPECTED(expr)        \
  ({                                        \
    auto expr_res = (expr);                 \
    if (!expr_res) {                        \
      builder->SendError(expr_res.error()); \
      return;                               \
    }                                       \
    std::move(expr_res).value();            \
  })

#endif  // GET_OR_SEND_UNEXPECTED

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
  bad_json_string = 12,
  unsupported_operation = 13,
};

}  // namespace rdb

std::error_code RdbError(rdb::errc ev);

}  // namespace dfly
