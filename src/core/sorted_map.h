// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/functional/function_ref.h>

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

extern "C" {
#include "redis/zset.h"
}

#include "core/bptree_set.h"
#include "core/score_map.h"

namespace dfly {

namespace detail {

/**
 * @brief SortedMap is a sorted map implementation based on zset.h. It holds unique strings that
 * are ordered by score and lexicographically. The score is a double value and has higher priority.
 * The map is implemented as a skip list and a hash table. For more details see
 * zset.h and t_zset.c files in Redis.
 */
class SortedMap {
 public:
  using ScoredMember = std::pair<std::string, double>;
  using ScoredArray = std::vector<ScoredMember>;
  using ScoreSds = void*;
  using RankAndScore = std::pair<unsigned, double>;

  SortedMap(PMR_NS::memory_resource* res);
  ~SortedMap();

  SortedMap(const SortedMap&) = delete;
  SortedMap& operator=(const SortedMap&) = delete;

  struct ScoreSdsPolicy {
    using KeyT = ScoreSds;

    struct KeyCompareTo {
      int operator()(KeyT a, KeyT b) const;
    };
  };

  bool Reserve(size_t sz);
  int Add(double score, sds ele, int in_flags, int* out_flags, double* newscore);
  bool Insert(double score, sds member);
  bool Delete(sds ele);

  // Upper bound size of the set.
  // Note: Currently we do not allow member expiry in sorted sets, therefore it's exact
  // But if we decide to add expire, this method will provide an approximation from above.
  size_t Size() const {
    return score_map->UpperBoundSize();
  }

  size_t MallocSize() const;

  size_t DeleteRangeByRank(unsigned start, unsigned end);
  size_t DeleteRangeByScore(const zrangespec& range);
  size_t DeleteRangeByLex(const zlexrangespec& range);

  ScoredArray PopTopScores(unsigned count, bool reverse);

  std::optional<double> GetScore(sds ele) const;
  std::optional<unsigned> GetRank(sds ele, bool reverse) const;
  std::optional<RankAndScore> GetRankAndScore(sds ele, bool reverse) const;
  ScoredArray GetRange(const zrangespec& r, unsigned offs, unsigned len, bool rev) const;
  ScoredArray GetLexRange(const zlexrangespec& r, unsigned o, unsigned l, bool rev) const;

  size_t Count(const zrangespec& range) const;
  size_t LexCount(const zlexrangespec& range) const;

  // Runs cb for each element in the range [start_rank, start_rank + len).
  // Stops iteration if cb returns false. Returns false in this case.
  bool Iterate(unsigned start_rank, unsigned len, bool reverse,
               std::function<bool(sds, double)> cb) const;

  uint64_t Scan(uint64_t cursor, absl::FunctionRef<void(std::string_view, double)> cb) const;

  uint8_t* ToListPack() const;
  static SortedMap* FromListPack(PMR_NS::memory_resource* res, const uint8_t* lp);

  bool DefragIfNeeded(float ratio);

 private:
  using ScoreTree = BPTree<ScoreSds, ScoreSdsPolicy>;

  // hash map from fields to scores.
  ScoreMap* score_map = nullptr;

  // sorted tree of (score,field) items.
  ScoreTree* score_tree = nullptr;
};

// Used by CompactObject.
unsigned char* ZzlInsert(unsigned char* zl, sds ele, double score);

}  // namespace detail
}  // namespace dfly
