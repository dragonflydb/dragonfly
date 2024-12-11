#include "server/family_utils.h"

#include "base/logging.h"

extern "C" {
#include "redis/stream.h"
#include "redis/zmalloc.h"
}

namespace dfly {

using namespace std;

sds WrapSds(std::string_view s) {
  static thread_local sds tmp_sds = sdsempty();
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

streamConsumer* StreamCreateConsumer(streamCG* cg, string_view name, uint64_t now_ms, int flags) {
  DCHECK(cg);
  DCHECK(!name.empty());
  if (cg == NULL)
    return NULL;

  streamConsumer* consumer = (streamConsumer*)zmalloc(sizeof(*consumer));

  int success =
      raxTryInsert(cg->consumers, (unsigned char*)name.data(), name.size(), consumer, NULL);
  if (!success) {
    zfree(consumer);
    return NULL;
  }
  consumer->name = sdsnewlen(name.data(), name.size());
  consumer->pel = raxNew();
  consumer->seen_time = now_ms;
  consumer->active_time = -1;

  return consumer;
}

}  // namespace dfly
