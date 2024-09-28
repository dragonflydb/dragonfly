#include "server/family_utils.h"

#include "base/logging.h"

namespace dfly {

sds WrapSds(std::string_view s) {
  static __thread sds tmp_sds = nullptr;
  if (tmp_sds == nullptr)
    tmp_sds = sdsempty();
  return tmp_sds = sdscpylen(tmp_sds, s.data(), s.length());
}

NonUniquePicksGenerator::NonUniquePicksGenerator(RandomPick max_range) : max_range_(max_range) {
  CHECK_GT(max_range, RandomPick(0));
}

RandomPick NonUniquePicksGenerator::Generate() {
  return absl::Uniform(bitgen_, 0u, max_range_);
}

UniquePicksGenerator::UniquePicksGenerator(std::uint32_t picks_count, RandomPick max_range)
    : remaining_picks_count_(picks_count), picked_indexes_(picks_count) {
  CHECK_GE(max_range, picks_count);
  current_random_limit_ = max_range - picks_count;
}

RandomPick UniquePicksGenerator::Generate() {
  DCHECK_GT(remaining_picks_count_, 0u);

  remaining_picks_count_--;

  const RandomPick max_index = current_random_limit_++;
  const RandomPick random_index = absl::Uniform(bitgen_, 0u, max_index + 1u);

  const bool random_index_is_picked = picked_indexes_.emplace(random_index).second;
  if (random_index_is_picked) {
    return random_index;
  }

  picked_indexes_.insert(max_index);
  return max_index;
}

}  // namespace dfly
