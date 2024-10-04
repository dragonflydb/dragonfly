// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_set.h>
#include <absl/random/random.h>

#include <cstdint>

extern "C" {
#include "redis/sds.h"
}
namespace dfly {

// Copy str to thread local sds instance. Valid until next WrapSds call on thread
sds WrapSds(std::string_view str);

using RandomPick = uint32_t;

class PicksGenerator {
 public:
  virtual RandomPick Generate() = 0;
  virtual ~PicksGenerator() = default;
};

class NonUniquePicksGenerator : public PicksGenerator {
 public:
  /* The generated value will be within the closed-open interval [0, max_range) */
  NonUniquePicksGenerator(RandomPick max_range);

  RandomPick Generate() override;

 private:
  const RandomPick max_range_;
  absl::BitGen bitgen_{};
};

/*
 * Generates unique index in O(1).
 *
 * picks_count specifies the number of random indexes to be generated.
 * In other words, this is the number of times the Generate() function is called.
 *
 * The class uses Robert Floyd's sampling algorithm
 * https://dl.acm.org/doi/pdf/10.1145/30401.315746
 * */
class UniquePicksGenerator : public PicksGenerator {
 public:
  /* The generated value will be within the closed-open interval [0, max_range) */
  UniquePicksGenerator(uint32_t picks_count, RandomPick max_range);

  RandomPick Generate() override;

 private:
  RandomPick current_random_limit_;
  uint32_t remaining_picks_count_;
  absl::flat_hash_set<RandomPick> picked_indexes_;
  absl::BitGen bitgen_{};
};

}  // namespace dfly
