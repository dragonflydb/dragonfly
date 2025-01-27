// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/table.h"

#include "base/flags.h"
#include "base/logging.h"
#include "server/cluster_support.h"
#include "server/server_state.h"

ABSL_FLAG(bool, enable_top_keys_tracking, false,
          "Enables / disables tracking of hot keys debugging feature");

using namespace std;
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
  constexpr size_t kDbSz = sizeof(DbTableStats) - sizeof(memory_usage_by_type);
  static_assert(kDbSz == 48);

  ADD(inline_keys);
  ADD(obj_memory_usage);
  ADD(listpack_blob_cnt);
  ADD(listpack_bytes);
  ADD(tiered_entries);
  ADD(tiered_used_bytes);

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

std::optional<const IntentLock> LockTable::Find(LockTag tag) const {
  LockFp fp = tag.Fingerprint();
  if (auto it = locks_.find(fp); it != locks_.end())
    return it->second;
  return std::nullopt;
}

std::optional<const IntentLock> LockTable::Find(uint64_t fp) const {
  if (auto it = locks_.find(fp); it != locks_.end())
    return it->second;
  return std::nullopt;
}

void LockTable::Release(uint64_t fp, IntentLock::Mode mode) {
  auto it = locks_.find(fp);
  DCHECK(it != locks_.end()) << fp;

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
  if (IsClusterEnabled()) {
    slots_stats.resize(kMaxSlotNum + 1);
  }
  thread_index = ServerState::tlocal()->thread_index();
}

DbTable::~DbTable() {
  DCHECK_EQ(thread_index, ServerState::tlocal()->thread_index());
}

void DbTable::Clear() {
  prime.size();
  prime.Clear();
  expire.Clear();
  mcflag.Clear();
  stats = DbTableStats{};
}

PrimeIterator DbTable::Launder(PrimeIterator it, string_view key) {
  if (!it.IsOccupied() || it->first != key) {
    it = prime.Find(key);
  }
  return it;
}

}  // namespace dfly
