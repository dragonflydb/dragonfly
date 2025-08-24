// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <string>
#include <vector>

#include "absl/flags/flag.h"

namespace facade {
template <typename... Ts> std::vector<std::string> GetFlagNames(const absl::Flag<Ts>&... flags) {
  return {std::string{absl::GetFlagReflectionHandle(flags).Name()}...};
}
}  // namespace facade
