// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/base.h"

#include <absl/strings/numbers.h>

namespace dfly::search {

std::string_view QueryParams::operator[](std::string_view name) const {
  if (auto it = params.find(name); it != params.end())
    return it->second;
  return "";
}

std::string& QueryParams::operator[](std::string_view k) {
  return params[k];
}

std::optional<double> ParseNumericField(std::string_view value) {
  double value_as_double;
  if (absl::SimpleAtod(value, &value_as_double))
    return value_as_double;
  return std::nullopt;
}

DefragmentResult& DefragmentResult::Merge(DefragmentResult&& other) {
  quota_depleted |= other.quota_depleted;
  objects_moved += other.objects_moved;
  return *this;
}

}  // namespace dfly::search
