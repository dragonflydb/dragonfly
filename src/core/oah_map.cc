// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/oah_map.h"

#include <absl/random/random.h>

#include <algorithm>

namespace dfly {

void OAHMap::RandomPairsUnique(unsigned count, std::vector<std::string>& keys,
                               std::vector<std::string>& vals, bool with_value) {
  const unsigned total = SizeSlow();
  count = std::min(count, total);

  keys.reserve(keys.size() + count);
  if (with_value)
    vals.reserve(vals.size() + count);

  static thread_local absl::InsecureBitGen rng;
  unsigned index = 0;
  unsigned remaining = count;
  for (auto it = begin(), it_end = end(); remaining && it != it_end; ++it, ++index) {
    const double threshold = double(remaining) / (total - index);
    if (absl::Uniform(rng, 0.0, 1.0) <= threshold) {
      OAHPair pair = *it;
      const oah::key::Decoded key = DecodeKey(pair);
      keys.emplace_back(key.view());
      if (with_value)
        vals.emplace_back(pair.Value());
      --remaining;
    }
  }
}

}  // namespace dfly
