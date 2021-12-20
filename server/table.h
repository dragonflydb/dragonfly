// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

namespace dfly {

struct MainValue {
  std::string str;

  MainValue() = default;
  MainValue(std::string_view s) : str(s) {
  }

  bool HasExpire() const {
    return has_expire_;
  }

  void SetExpire(bool b) {
    has_expire_ = b;
  }

 private:
  bool has_expire_ = false;
};

using MainTable = absl::flat_hash_map<std::string, MainValue>;
using ExpireTable = absl::flat_hash_map<std::string, uint64_t>;

/// Iterators are invalidated when new keys are added to the table or some entries are deleted.
/// Iterators are still valid  if a different entry in the table was mutated.
using MainIterator = MainTable::iterator;
using ExpireIterator = ExpireTable::iterator;

}  // namespace dfly
