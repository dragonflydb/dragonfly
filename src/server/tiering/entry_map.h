// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <cstdint>
#include <string>

namespace dfly {
using DbIndex = uint16_t;  // Forward declare typedef from tx_base.h
}

namespace dfly::tiering {

namespace detail {
struct Hasher {
  using is_transparent = void;
  template <typename S> size_t operator()(const std::pair<DbIndex, S>& p) const {
    return absl::HashOf(p);
  }
};

struct Eq {
  using is_transparent = void;
  template <typename S1, typename S2>
  bool operator()(const std::pair<DbIndex, S1>& l, const std::pair<DbIndex, S2>& r) const {
    const auto& [i1, s1] = l;
    const auto& [i2, s2] = r;
    return i1 == i2 && s1 == s2;
  }
};
}  // namespace detail

using DbKeyId = std::pair<DbIndex, std::string>;

// Map of key (db index, string key) -> T with heterogeneous lookup
template <typename T> using EntryMap = absl::flat_hash_map<DbKeyId, T, detail::Hasher, detail::Eq>;

}  // namespace dfly::tiering
