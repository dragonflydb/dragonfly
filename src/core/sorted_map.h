// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/functional/function_ref.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

extern "C" {
#include "redis/dict.h"
#include "redis/zset.h"
}

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

  SortedMap();
  SortedMap(const SortedMap&) = delete;
  SortedMap& operator=(const SortedMap&) = delete;

  ~SortedMap();

  // The ownership for the returned SortedMap stays with the caller
  static std::unique_ptr<SortedMap> FromListPack(const uint8_t* lp);

  size_t Size() const {
    return zsl_->length;
  }

  bool Reserve(size_t sz) {
    return dictExpand(dict_, sz) == DICT_OK;
  }

  // Interface equivalent to zsetAdd.
  int Add(double score, sds ele, int in_flags, int* out_flags, double* newscore);

  bool Insert(double score, sds member);

  uint8_t* ToListPack() const;
  size_t MallocSize() const;

  dict* GetDict() const {
    return dict_;
  }

  size_t DeleteRangeByRank(unsigned start, unsigned end) {
    return zslDeleteRangeByRank(zsl_, start + 1, end + 1, dict_);
  }

  size_t DeleteRangeByScore(const zrangespec& range) {
    return zslDeleteRangeByScore(zsl_, &range, dict_);
  }

  size_t DeleteRangeByLex(const zlexrangespec& range) {
    return zslDeleteRangeByLex(zsl_, &range, dict_);
  }

  // returns true if the element was deleted.
  bool Delete(sds ele);

  std::optional<double> GetScore(sds ele) const;
  std::optional<unsigned> GetRank(sds ele, bool reverse) const;

  ScoredArray GetRange(const zrangespec& range, unsigned offset, unsigned limit,
                       bool reverse) const;
  ScoredArray GetLexRange(const zlexrangespec& range, unsigned offset, unsigned limit,
                          bool reverse) const;

  ScoredArray PopTopScores(unsigned count, bool reverse);
  size_t Count(const zrangespec& range) const;
  size_t LexCount(const zlexrangespec& range) const;

  // Runs cb for each element in the range [start_rank, start_rank + len).
  // Stops iteration if cb returns false. Returns false in this case.
  bool Iterate(unsigned start_rank, unsigned len, bool reverse,
               absl::FunctionRef<bool(sds, double)> cb) const;

 private:
  dict* dict_ = nullptr;
  zskiplist* zsl_ = nullptr;
};

}  // namespace detail
}  // namespace dfly
