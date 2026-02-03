// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <string>
#include <string_view>
#include <vector>
#include "base/random.h"

namespace dfly {

// TopKeys is a utility class that helps determine the most frequently used keys.
// Based on: HeavyKeeper paper,  https://www.usenix.org/conference/atc18/presentation/gong
//
// Usage:
// - Instantiate this class with proper options (see below)
// - For every used key k, call Touch(k)
// - At some point(s) in time, call GetTopKeys() to get an estimated list of top keys along with
//   their approximate count (i.e. how many times Touch() was invoked for them).
//
// Notes:
// - This class implements a slightly modified version of HeavyKeeper, a data structure designed
//   for a similar problem domain. The modification made is to store the keys directly within the
//   tables, when they meet a certain threshold, instead of using a min-heap.
// - This class is statistical in nature. Do *not* expect accurate counts.
// - When misconfigured, real top keys may be missing from GetTopKeys(). This can occur when there
//   are too few buckets, or when min_key_count_to_record is too high, depending on actual usage.
class TopKeys {
  TopKeys(const TopKeys&) = delete;
  TopKeys& operator=(const TopKeys&) = delete;
 public:
  struct Options {
    // HeavyKeeper options
    uint32_t buckets = 1 << 16;
    uint32_t depth = 4;

    // What is the minimum times Touch() has to be called for a given key in order for the key to be
    // saved. Use lower values when load is low, or higher values when load is high. The cost of a
    // low value for high load is frequent string copying and memory allocation.
    // Min value: 2
    uint32_t min_key_count_to_record = 50;

    double decay_base = 1.08;
  };

  explicit TopKeys(Options options);

  void Touch(std::string_view key);
  absl::flat_hash_map<std::string, uint64_t> GetTopKeys() const;

 private:
  // Each cell consists of a key-fingerprint, a count, and potentially the key itself, when it's
  // above options_.min_key_count_to_record.
  struct Cell {
    uint64_t fingerprint = 0;
    uint64_t count = 0;
    std::string key;
  };
  Cell& GetCell(uint32_t d, uint32_t bucket);
  const Cell& GetCell(uint32_t d, uint32_t bucket) const;

  Options options_;
  base::Xoroshiro128p bitgen_;

  // fingerprints_'s size is options_.buckets * options_.arrays. Always access fields via GetCell().
  std::vector<Cell> fingerprints_;
};

}  // end of namespace dfly
