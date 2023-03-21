// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/table.h"

#include <absl/strings/strip.h>

#include "base/flags.h"
#include "base/logging.h"

ABSL_FLAG(bool, enable_top_keys_tracking, false,
          "Enables / disables tracking of hot keys debugging feature");

namespace dfly {

#define ADD(x) (x) += o.x

// It should be const, but we override this variable in our tests so that they run faster.
unsigned kInitSegmentLog = 3;

DbTableStats& DbTableStats::operator+=(const DbTableStats& o) {
  constexpr size_t kDbSz = sizeof(DbTableStats);
  static_assert(kDbSz == 64);

  ADD(inline_keys);
  ADD(obj_memory_usage);
  ADD(strval_memory_usage);
  ADD(update_value_amount);
  ADD(listpack_blob_cnt);
  ADD(listpack_bytes);
  ADD(tiered_entries);
  ADD(tiered_size);

  return *this;
}

void DbTableTypesCount::Increment(uint32_t type) {
  IncrementBy(type, 1);
}

void DbTableTypesCount::Decrement(uint32_t type) {
  IncrementBy(type, -1);
}

absl::flat_hash_map<std::string, uint64_t> DbTableTypesCount::GetCounts() const {
  struct ObjectTypeNameAndCount {
    std::string name;
    std::atomic_uint64_t count = 0;
  };
  std::array<ObjectTypeNameAndCount, std::tuple_size<Array>::value> counts;

#define REGISTER_OBJECT_TYPE(type) counts[type].name = absl::StripPrefix(#type, "OBJ_")
  REGISTER_OBJECT_TYPE(OBJ_STRING);
  REGISTER_OBJECT_TYPE(OBJ_LIST);
  REGISTER_OBJECT_TYPE(OBJ_SET);
  REGISTER_OBJECT_TYPE(OBJ_ZSET);
  REGISTER_OBJECT_TYPE(OBJ_HASH);
  REGISTER_OBJECT_TYPE(OBJ_MODULE);
  REGISTER_OBJECT_TYPE(OBJ_STREAM);
#undef REGISTER_OBJECT_TYPE

  for (size_t i = 0; i < types_count_.size(); ++i) {
    counts[i].count += types_count_[i];
  }

  absl::flat_hash_map<std::string, uint64_t> result;
  for (const auto& c : counts) {
    result[c.name] = c.count;
  }
  return result;
}

void DbTableTypesCount::IncrementBy(uint32_t type, int how_much) {
  if (type < types_count_.size()) {
    uint64_t& count = types_count_[type];
    CHECK_GE(static_cast<int64_t>(count) + how_much, 0);
    count += how_much;
  }
}

DbTable::DbTable(std::pmr::memory_resource* mr)
    : prime(kInitSegmentLog, detail::PrimeTablePolicy{}, mr),
      expire(0, detail::ExpireTablePolicy{}, mr), mcflag(0, detail::ExpireTablePolicy{}, mr),
      top_keys({.enabled = absl::GetFlag(FLAGS_enable_top_keys_tracking)}) {
}

DbTable::~DbTable() {
}

void DbTable::Clear() {
  prime.size();
  prime.Clear();
  expire.Clear();
  mcflag.Clear();
  stats = DbTableStats{};
}

void DbTable::Release(IntentLock::Mode mode, std::string_view key, unsigned count) {
  DVLOG(1) << "Release " << IntentLock::ModeName(mode) << " " << count << " for " << key;

  auto it = trans_locks.find(key);
  CHECK(it != trans_locks.end()) << key;
  it->second.Release(mode, count);
  if (it->second.IsFree()) {
    trans_locks.erase(it);
  }
}

}  // namespace dfly
