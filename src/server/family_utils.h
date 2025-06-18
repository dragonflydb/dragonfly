// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_set.h>
#include <absl/random/random.h>

#include <cstdint>

#include "facade/facade_types.h"
#include "server/engine_shard.h"
#include "server/search/doc_index.h"
#include "server/table.h"

extern "C" {
#include "redis/sds.h"
}

typedef struct streamConsumer streamConsumer;
typedef struct streamCG streamCG;

namespace dfly {

template <typename DenseSet>
std::vector<long> ExpireElements(DenseSet* owner, facade::CmdArgList values, uint32_t ttl_sec);

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

streamConsumer* StreamCreateConsumer(streamCG* cg, std::string_view name, uint64_t now_ms,
                                     int flags);

/* Use these methods to add or remove documents from the indexes for generic commands when the key
 * being modified could potentially be of type HSET or JSON. */
void AddKeyToIndexesIfNeeded(std::string_view key, const DbContext& db_cntx, const PrimeValue& pv,
                             EngineShard* shard);
void RemoveKeyFromIndexesIfNeeded(std::string_view key, const DbContext& db_cntx,
                                  const PrimeValue& pv, EngineShard* shard);

// Returns true if this key type could potentially be indexed.
// Or in other words, if the key is of type HSET or JSON.
bool IsIndexedKeyType(const PrimeValue& pv);

// Implementation
/******************************************************************/
template <typename DenseSet>
inline std::vector<long> ExpireElements(DenseSet* owner, facade::CmdArgList values,
                                        uint32_t ttl_sec) {
  std::vector<long> res;
  res.reserve(values.size());

  for (size_t i = 0; i < values.size(); i++) {
    std::string_view field = facade::ToSV(values[i]);
    auto it = owner->Find(field);
    if (it != owner->end()) {
      it.SetExpiryTime(ttl_sec);
      res.emplace_back(ttl_sec == 0 ? 0 : 1);
    } else {
      res.emplace_back(-2);
    }
  }

  return res;
}

inline void AddKeyToIndexesIfNeeded(std::string_view key, const DbContext& db_cntx,
                                    const PrimeValue& pv, EngineShard* shard) {
  if (IsIndexedKeyType(pv)) {
    shard->search_indices()->AddDoc(key, db_cntx, pv);
  }
}

inline void RemoveKeyFromIndexesIfNeeded(std::string_view key, const DbContext& db_cntx,
                                         const PrimeValue& pv, EngineShard* shard) {
  if (IsIndexedKeyType(pv)) {
    shard->search_indices()->RemoveDoc(key, db_cntx, pv);
  }
}

inline bool IsIndexedKeyType(const PrimeValue& pv) {
  return pv.ObjType() == OBJ_HASH || pv.ObjType() == OBJ_JSON;
}

}  // namespace dfly
