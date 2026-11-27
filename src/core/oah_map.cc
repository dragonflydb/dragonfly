// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/oah_map.h"

#include <absl/random/random.h>

#include <algorithm>
#include <cassert>
#include <utility>

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

void OAHMap::RandomPairs(unsigned count, std::vector<std::string>& keys,
                         std::vector<std::string>& vals, bool with_value) {
  const unsigned total = SizeSlow();
  if (total == 0 || count == 0)
    return;

  // Draw live-entry indices uniformly, then copy every pick in one ordered pass over the table.
  // Each pick keeps its draw slot, so the output order stays random.
  static thread_local absl::InsecureBitGen rng;
  std::vector<std::pair<unsigned, unsigned>> picks(count);  // {entry index, output slot}
  for (unsigned i = 0; i < count; ++i)
    picks[i] = {absl::Uniform(rng, 0u, total), i};
  std::sort(picks.begin(), picks.end());

  const size_t keys_base = keys.size();
  const size_t vals_base = vals.size();
  keys.resize(keys_base + count);
  if (with_value)
    vals.resize(vals_base + count);

  auto pick = picks.begin();
  unsigned index = 0;
  for (auto it = begin(), it_end = end(); pick != picks.end() && it != it_end; ++it, ++index) {
    if (pick->first != index)
      continue;
    OAHPair pair = *it;
    const oah::key::Decoded key = DecodeKey(pair);
    for (; pick != picks.end() && pick->first == index; ++pick) {
      keys[keys_base + pick->second] = key.view();
      if (with_value)
        vals[vals_base + pick->second] = pair.Value();
    }
  }
  assert(pick == picks.end());
}

}  // namespace dfly
