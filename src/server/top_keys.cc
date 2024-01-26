// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/top_keys.h"

#include <xxhash.h>

#include "absl/numeric/bits.h"
#include "absl/random/distributions.h"
#include "base/logging.h"

namespace dfly {

TopKeys::TopKeys(Options options)
    : options_(options), fingerprints_(options.enabled ? options_.buckets * options_.arrays : 0) {
}

void TopKeys::Touch(std::string_view key) {
  if (!IsEnabled()) {
    return;
  }

  auto ResetCell = [&](Cell& cell, uint64_t fingerprint) {
    cell.fingerprint = fingerprint;
    cell.count = 1;
    if (cell.count >= options_.min_key_count_to_record) {
      cell.key = key;
    }
  };

  const uint64_t fingerprint = XXH3_64bits(key.data(), key.size());
  const int shift = absl::bit_width(options_.buckets);

  for (uint64_t array = 0; array < options_.arrays; ++array) {
    // TODO: if we decide to keep this logic, CHECK that bit_width(buckets) * arrays < 64
    const int bucket = (fingerprint >> (shift * array)) % options_.buckets;
    Cell& cell = GetCell(array, bucket);
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
  if (!IsEnabled()) {
    return {};
  }

  absl::flat_hash_map<std::string, uint64_t> results;
  for (uint64_t array = 0; array < options_.arrays; ++array) {
    for (uint64_t bucket = 0; bucket < options_.buckets; ++bucket) {
      const Cell& cell = GetCell(array, bucket);
      if (!cell.key.empty()) {
        results[cell.key] = std::max(results[cell.key], cell.count);
      }
    }
  }
  return results;
}

bool TopKeys::IsEnabled() const {
  return options_.enabled;
}

TopKeys::Cell& TopKeys::GetCell(uint64_t array, uint64_t bucket) {
  DCHECK(IsEnabled());
  DCHECK(array < options_.arrays);
  DCHECK(bucket < options_.buckets);
  return fingerprints_[array * options_.buckets + bucket];
}

const TopKeys::Cell& TopKeys::GetCell(uint64_t array, uint64_t bucket) const {
  DCHECK(IsEnabled());
  DCHECK(array < options_.arrays);
  DCHECK(bucket < options_.buckets);
  return fingerprints_[array * options_.buckets + bucket];
}

}  // end of namespace dfly
