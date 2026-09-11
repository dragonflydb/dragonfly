// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/table.h"

#include "base/flags.h"
#include "base/logging.h"
#include "core/top_keys.h"
#include "server/cluster_support.h"
#include "server/server_state.h"

using namespace std;
namespace dfly {
#define ADD(x) (x) += o.x

// It should be const, but we override this variable in our tests so that they run faster.
unsigned kInitSegmentLog = 3;

void DbTableStats::AddTypeMemoryUsage(unsigned type, int64_t delta) {
  if (type >= memory_usage_by_type.size()) {
    LOG(DFATAL) << "Encountered unknown type when aggregating per-type memory: " << type;
    return;
  }

  DCHECK_GE(obj_memory_usage, memory_usage_by_type[type]);

  if (delta < 0 && memory_usage_by_type[type] < size_t(-delta)) {
#ifdef NDEBUG
    LOG_EVERY_T(ERROR, 1)
#else
    LOG_EVERY_T(FATAL, 1)
#endif
        << "Encountered underflow memory usage when aggregating per-type memory: "
        << memory_usage_by_type[type] << " + " << delta << ", type: " << type;

    // Truncate delta to avoid underflow, but keep the memory usage consistent with the sum of
    // per-type usage.
    delta = -static_cast<int64_t>(memory_usage_by_type[type]);
  }

  obj_memory_usage += delta;
  memory_usage_by_type[type] += delta;
}

DbTableStats& DbTableStats::operator+=(const DbTableStats& o) {
  constexpr size_t kDbSz = sizeof(DbTableStats) - sizeof(memory_usage_by_type);
  static_assert(kDbSz == 72);

  ADD(inline_keys);
  ADD(expire_count);
  ADD(obj_memory_usage);
  ADD(tiered_entries);
  ADD(tiered_used_bytes);
  ADD(events.hits);
  ADD(events.misses);
  ADD(events.expired_keys);
  ADD(events.evicted_keys);

  for (size_t i = 0; i < o.memory_usage_by_type.size(); ++i) {
    memory_usage_by_type[i] += o.memory_usage_by_type[i];
  }

  return *this;
}

SlotStats& SlotStats::operator+=(const SlotStats& o) {
  static_assert(sizeof(SlotStats) == 40);

  ADD(key_count);
  ADD(total_reads);
  ADD(total_writes);
  ADD(memory_bytes);
  ADD(tiered_bytes);
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

void LockTable::PrepareForSingleShotHeapDestroy() {
  CHECK(locks_.empty());
  locks_.rehash(0);
}

[[maybe_unused]] constexpr size_t kSzTable = sizeof(DbTable);

DbTable::SampleTopKeys::~SampleTopKeys() {
  delete top_keys;
}

DbTable::SampleUniqueKeys::~SampleUniqueKeys() {
  delete[] dense_hll;
}

DbTable::DbTable(PMR_NS::memory_resource* mr, DbIndex db_index)
    : prime(kInitSegmentLog, detail::PrimeTablePolicy{}, mr),
      mcflag(0, detail::ExpireTablePolicy{}, mr),
      index(db_index),
      memory_resource_(mr) {
  if (IsClusterEnabled()) {
    slots_stats.reset(new SlotStats[kMaxSlotNum + 1]);
  }
  thread_index = ServerState::tlocal()->thread_index();
}

DbTable::~DbTable() {
  DCHECK_EQ(thread_index, ServerState::tlocal()->thread_index());
  delete sample_top_keys;
  delete sample_unique_keys;
  delete sample_values_hist;
}

void DbTable::PrepareForSingleShotHeapDestroy() {
  LOG(ERROR) << "DbTable::PrepareForSingleShotHeapDestroy: trans_locks db_index=" << index;
  trans_locks.PrepareForSingleShotHeapDestroy();
  CHECK(watched_keys.empty());
  watched_keys.rehash(0);

  LOG(ERROR) << "DbTable::PrepareForSingleShotHeapDestroy: sample structures db_index=" << index;
  delete std::exchange(sample_top_keys, nullptr);
  delete std::exchange(sample_unique_keys, nullptr);
  delete std::exchange(sample_values_hist, nullptr);
  slots_stats.reset();
  LOG(ERROR) << "DbTable::PrepareForSingleShotHeapDestroy: done db_index=" << index;
}

void intrusive_ptr_add_ref(DbTable* table) noexcept {
  ++table->use_count_;
}

void intrusive_ptr_release(DbTable* table) noexcept {
  DCHECK_GT(table->use_count_, 0u);
  if (--table->use_count_ == 0) {
    auto* mr = table->memory_resource_;
    std::destroy_at(table);
    mr->deallocate(table, sizeof(DbTable), alignof(DbTable));
  }
}

void DbTable::Clear() {
  prime.Clear();
  mcflag.Clear();
  stats = DbTableStats{};
  expire_cursor = PrimeTable::Cursor::end();
  segment_defrag_cursor = PrimeTable::Cursor::end();
}

PrimeIterator DbTable::Launder(PrimeIterator it, string_view key) {
  if (!it.IsOccupied() || it->first != key) {
    it = prime.Find(key);
  }
  return it;
}

}  // namespace dfly
