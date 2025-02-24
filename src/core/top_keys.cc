// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/top_keys.h"

#include <xxhash.h>

#include "absl/numeric/bits.h"
#include "absl/random/distributions.h"
#include "base/logging.h"

namespace dfly {

using namespace std;

TopKeys::TopKeys(Options options)
    : options_(options), fingerprints_(options_.buckets * options_.depth) {
  if (options_.min_key_count_to_record < 2) {
    options_.min_key_count_to_record = 2;
  }
}

void TopKeys::Touch(std::string_view key) {
  auto ResetCell = [&](Cell& cell, uint64_t fingerprint) {
    cell.fingerprint = fingerprint;
    cell.count = 1;
    cell.key.clear();
  };

  uint64_t fingerprint = XXH3_64bits(key.data(), key.size());
  constexpr uint64_t kPrime = 0xff51afd7ed558ccd;
  for (uint64_t id = 0; id < options_.depth; ++id) {
    const unsigned bucket = fingerprint % options_.buckets;
    fingerprint *= kPrime;
    Cell& cell = GetCell(id, bucket);
    if (cell.count == 0) {
      // No fingerprint in cell.
      ResetCell(cell, fingerprint);
    } else if (cell.fingerprint == fingerprint) {
      // Same fingerprint, simply increment count.

      // We could make sure that, if !cell.key.empty(), then key == cell.key.empty() here. However,
      // what do we do in case they are different?
      ++cell.count;

      if (cell.count >= options_.min_key_count_to_record && cell.key.empty()) {
        cell.key = key;
      }
    } else {
      // Different fingerprint, apply exponential decay.
      const double rand = absl::Uniform(bitgen_, 0, 1.0);
      if (rand < std::pow(options_.decay_base, -static_cast<double>(cell.count))) {
        --cell.count;
        if (cell.count == 0) {
          ResetCell(cell, fingerprint);
        }
      }
    }
  }
}

absl::flat_hash_map<std::string, uint64_t> TopKeys::GetTopKeys() const {
  absl::flat_hash_map<std::string, uint64_t> results;
  for (unsigned array = 0; array < options_.depth; ++array) {
    for (unsigned bucket = 0; bucket < options_.buckets; ++bucket) {
      const Cell& cell = GetCell(array, bucket);
      if (!cell.key.empty()) {
        auto [it, added] = results.emplace(cell.key, cell.count);
        if (!added && it->second < cell.count) {
          it->second = cell.count;
        }
      }
    }
  }
  return results;
}

TopKeys::Cell& TopKeys::GetCell(uint32_t d, uint32_t bucket) {
  DCHECK(d < options_.depth);
  DCHECK(bucket < options_.buckets);
  return fingerprints_[d * options_.buckets + bucket];
}

const TopKeys::Cell& TopKeys::GetCell(uint32_t d, uint32_t bucket) const {
  DCHECK(d < options_.depth);
  DCHECK(bucket < options_.buckets);
  return fingerprints_[d * options_.buckets + bucket];
}

}  // end of namespace dfly
