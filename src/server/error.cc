// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/error.h"

#include <absl/strings/str_cat.h>

using namespace std;

namespace dfly {
namespace rdb {

class error_category : public std::error_category {
 public:
  const char* name() const noexcept final {
    return "dragonfly.rdbload";
  }

  string message(int ev) const final;

  error_condition default_error_condition(int ev) const noexcept final;

  bool equivalent(int ev, const error_condition& condition) const noexcept final {
    return condition.value() == ev && &condition.category() == this;
  }

  bool equivalent(const error_code& error, int ev) const noexcept final {
    return error.value() == ev && &error.category() == this;
  }
};

string error_category::message(int ev) const {
  switch (ev) {
    case errc::wrong_signature:
      return "Wrong signature while trying to load from rdb file";
    case errc::out_of_memory:
      return "Out of memory, or used memory is too high";
    default:
      return absl::StrCat("Internal error when loading RDB file ", ev);
      break;
  }
}

error_condition error_category::default_error_condition(int ev) const noexcept {
  return error_condition{ev, *this};
}

static error_category rdb_category;

}  // namespace rdb

error_code RdbError(rdb::errc ev) {
  return error_code{static_cast<int>(ev), rdb::rdb_category};
}

}  // namespace dfly
