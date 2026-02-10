// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/type_aliases.h"

#include <absl/strings/match.h>
#include <fast_float/fast_float.h>

extern "C" {
#include "redis/rdb.h"
}

namespace dfly {

using namespace std;

bool ParseDouble(string_view src, double* value) {
  if (src.empty())
    return false;

  if (absl::EqualsIgnoreCase(src, "-inf")) {
    *value = -HUGE_VAL;
  } else if (absl::EqualsIgnoreCase(src, "+inf")) {
    *value = HUGE_VAL;
  } else {
    fast_float::from_chars_result result = fast_float::from_chars(src.data(), src.end(), *value);
    // nan double could be sent as "nan" with any case.
    if (int(result.ec) != 0 || result.ptr != src.end() || isnan(*value))
      return false;
  }
  return true;
}

const char* RdbTypeName(unsigned type) {
  switch (type) {
    case RDB_TYPE_STRING:
      return "string";
    case RDB_TYPE_LIST:
      return "list";
    case RDB_TYPE_SET:
      return "set";
    case RDB_TYPE_ZSET:
      return "zset";
    case RDB_TYPE_HASH:
      return "hash";
    case RDB_TYPE_STREAM_LISTPACKS:
      return "stream";
  }
  return "other";
}

}  // namespace dfly
