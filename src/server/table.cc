// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/table.h"

#include "base/flags.h"
#include "base/logging.h"

ABSL_FLAG(bool, enable_top_keys_tracking, false,
          "Enables / disables tracking of hot keys debugging feature");

namespace dfly {

#define ADD(x) (x) += o.x

// It should be const, but we override this variable in our tests so that they run faster.
unsigned kInitSegmentLog = 3;

void DbTableStats::AddTypeMemoryUsage(unsigned type, int64_t delta) {
  if (type >= memory_usage_by_type.size()) {
    LOG_FIRST_N(WARNING, 1) << "Encountered unknown type when aggregating per-type memory: "
                            << type;
    DCHECK(false) << "Unsupported type " << type;
    return;
  }
  obj_memory_usage += delta;
  memory_usage_by_type[type] += delta;
}

DbTableStats& DbTableStats::operator+=(const DbTableStats& o) {
  constexpr size_t kDbSz = sizeof(DbTableStats);
  static_assert(kDbSz == 176);

  ADD(inline_keys);
  ADD(obj_memory_usage);
  ADD(listpack_blob_cnt);
  ADD(listpack_bytes);
  ADD(tiered_entries);
  ADD(tiered_size);

  for (size_t i = 0; i < o.memory_usage_by_type.size(); ++i) {
    memory_usage_by_type[i] += o.memory_usage_by_type[i];
  }

  return *this;
}

SlotStats& SlotStats::operator+=(const SlotStats& o) {
  static_assert(sizeof(SlotStats) == 32);

  ADD(key_count);
  ADD(total_reads);
  ADD(total_writes);
  ADD(memory_bytes);
  return *this;
}

void LockTable::Key::MakeOwned() const {
  if (std::holds_alternative<std::string_view>(val_))
    val_ = std::string{std::get<std::string_view>(val_)};
}

size_t LockTable::Size() const {
  return locks_.size();
}

std::optional<const IntentLock> LockTable::Find(std::string_view key) const {
  DCHECK_EQ(KeyLockArgs::GetLockKey(key), key);

  if (auto it = locks_.find(Key{key}); it != locks_.end())
    return it->second;
  return std::nullopt;
}

bool LockTable::Acquire(std::string_view key, IntentLock::Mode mode) {
  DCHECK_EQ(KeyLockArgs::GetLockKey(key), key);

  auto [it, inserted] = locks_.try_emplace(Key{key});
  if (!inserted)            // If more than one transaction refers to a key
    it->first.MakeOwned();  // we must fall back to using a self-contained string

  return it->second.Acquire(mode);
}

void LockTable::Release(std::string_view key, IntentLock::Mode mode) {
  DCHECK_EQ(KeyLockArgs::GetLockKey(key), key);

  auto it = locks_.find(Key{key});
  CHECK(it != locks_.end()) << key;

  it->second.Release(mode);
  if (it->second.IsFree())
    locks_.erase(it);
}

DbTable::DbTable(PMR_NS::memory_resource* mr, DbIndex db_index)
    : prime(kInitSegmentLog, detail::PrimeTablePolicy{}, mr),
      expire(0, detail::ExpireTablePolicy{}, mr),
      mcflag(0, detail::ExpireTablePolicy{}, mr),
      top_keys({.enabled = absl::GetFlag(FLAGS_enable_top_keys_tracking)}),
      index(db_index) {
  if (ClusterConfig::IsEnabled()) {
    slots_stats.resize(ClusterConfig::kMaxSlotNum + 1);
  }
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

PrimeIterator DbTable::Launder(PrimeIterator it, std::string_view key) {
  if (!it.IsOccupied() || it->first != key) {
    it = prime.Find(key);
  }
  return it;
}

}  // namespace dfly
