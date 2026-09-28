// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_set.h>
#include <absl/random/random.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>

#include "facade/cmd_arg_parser.h"
#include "facade/facade_types.h"
#include "server/common_types.h"
#include "server/engine_shard.h"
#include "server/search/doc_index.h"
#include "server/table.h"

extern "C" {
#include "redis/sds.h"
}

typedef struct streamConsumer streamConsumer;
typedef struct streamCG streamCG;

namespace dfly {

struct ExpiryOption {
  ExpT type = ExpT::EX;
  std::optional<int64_t> value;
};

struct ExpiryOrPersistOptions {
  ExpiryOption expiry;
  bool persist = false;
};

template <class P = int64_t> consteval auto ExpiryOneOf() {
  using namespace facade;
  using T = ExpiryOption;
  return OneOf("", TagValue<P>("EX", &T::type, ExpT::EX, &T::value),
               TagValue<P>("PX", &T::type, ExpT::PX, &T::value),
               TagValue<P>("EXAT", &T::type, ExpT::EXAT, &T::value),
               TagValue<P>("PXAT", &T::type, ExpT::PXAT, &T::value));
}

template <class P = int64_t> consteval auto ExpiryOrPersist() {
  using namespace facade;
  using T = ExpiryOrPersistOptions;
  return OneOf("", Into(&T::expiry, ExpiryOneOf<P>()), Exist("PERSIST", &T::persist));
}

// Compute XXH3 hash and return as 16-character hex string
std::string XXH3_Digest(std::string_view s);

// Returns seconds since kMemberExpiryBase, -3 if missing, or -1 if persistent. May expire members.
int32_t FieldExpireTime(const DbContext& db_cntx, const PrimeValue& pv, std::string_view field);

// Deletes and journals empty sets/hashes. Run post-updaters first; may invalidate pv and yield.
bool DeleteCollectionIfEmpty(DbSlice& db_slice, const DbContext& db_cntx, std::string_view key,
                             const PrimeValue& pv);

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
void AddKeyToIndexesIfNeeded(std::string_view key, const DbContext& db_cntx, PrimeValue& pv,
                             EngineShard* shard);
void RemoveKeyFromIndexesIfNeeded(std::string_view key, const DbContext& db_cntx, PrimeValue& pv,
                                  EngineShard* shard);

// Validate and convert field/value ziplist pairs into listpack.
// Returns 1 on success, 0 on integrity failure.
int ZiplistPairsConvertAndValidateIntegrity(const uint8_t* zl, size_t size, unsigned char** lp);

// Returns true if this key type could potentially be indexed.
// Or in other words, if the key is of type HSET or JSON.
bool IsIndexedKeyType(const PrimeValue& pv);

// Implementation
/******************************************************************/
// Caller must set owner's time. Returns -2 (missing), 0 (skipped), 1 (updated), or 2 (deleted).
// before_update runs before setting a positive TTL; it must not mutate owner or yield.
template <typename Container, typename Fields, typename BeforeUpdate = std::nullptr_t>
inline std::vector<long> ExpireElements(Container* owner, const Fields& fields, uint32_t ttl_sec,
                                        ExpireFlags flags = ExpireFlags::EXPIRE_ALWAYS,
                                        BeforeUpdate before_update = nullptr) {
  std::vector<long> res;
  res.reserve(fields.size());

  auto can_update = [&](auto& it) {
    switch (flags) {
      case ExpireFlags::EXPIRE_NX:
        return !it.HasExpiry();
      case ExpireFlags::EXPIRE_XX:
        return it.HasExpiry();
      case ExpireFlags::EXPIRE_GT:
        return it.ExpiryTime() - owner->time_now() < ttl_sec;
      case ExpireFlags::EXPIRE_LT:
        return it.ExpiryTime() - owner->time_now() > ttl_sec;
      case ExpireFlags::EXPIRE_ALWAYS:
        break;
    }
    return true;
  };

  for (std::string_view field : fields) {
    auto it = owner->Find(field);
    if (it == owner->end()) {
      res.emplace_back(-2);
    } else if (!can_update(it)) {
      res.emplace_back(0);
    } else if (ttl_sec == 0) {
      owner->Erase(field);
      res.emplace_back(2);
    } else {
      if constexpr (!std::is_null_pointer_v<BeforeUpdate>) {
        before_update(field, it);
      }
      it.SetExpiryTime(ttl_sec);
      res.emplace_back(1);
    }
  }

  return res;
}

inline void AddKeyToIndexesIfNeeded(std::string_view key, const DbContext& db_cntx, PrimeValue& pv,
                                    EngineShard* shard) {
  if (IsIndexedKeyType(pv)) {
    shard->search_indices()->AddDoc(key, db_cntx, &pv);
  }
}

inline void RemoveKeyFromIndexesIfNeeded(std::string_view key, const DbContext& db_cntx,
                                         PrimeValue& pv, EngineShard* shard) {
  if (IsIndexedKeyType(pv)) {
    shard->search_indices()->RemoveDoc(key, db_cntx, pv);
  }
}

inline bool IsIndexedKeyType(const PrimeValue& pv) {
  return pv.ObjType() == OBJ_HASH || pv.ObjType() == OBJ_JSON;
}

}  // namespace dfly
