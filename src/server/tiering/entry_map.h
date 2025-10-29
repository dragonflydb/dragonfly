// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <string>

#include "server/tx_base.h"

namespace dfly::tiering {

namespace detail {
struct Hasher {
  using is_transparent = void;
  template <typename S> size_t operator()(const std::pair<DbIndex, S>& p) const {
    return absl::Hash(p)();
  }
};

struct Eq {
  using is_transparent = void;
  template <typename S1, typename S2>
  bool operator()(const std::pair<DbIndex, S1>& l, const std::pair<DbIndex, S2>& r) const {
    return l == r;
  }
};
}  // namespace detail

// Map of key (db index, string key) -> T with heterogeneous lookup
template <typename T>
using EntryMap =
    absl::flat_hash_map<std::pair<DbIndex, std::string>, T, detail::Hasher, detail::Eq>;

}  // namespace dfly::tiering
