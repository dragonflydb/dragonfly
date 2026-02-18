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

// It should be const, but we override this variable in our tests so that they run faster.
unsigned kInitSegmentLog = 3;

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

[[maybe_unused]] constexpr size_t kSzTable = sizeof(DbTable);

DbTable::SampleTopKeys::~SampleTopKeys() {
  delete top_keys;
}

DbTable::SampleUniqueKeys::~SampleUniqueKeys() {
  delete[] dense_hll;
}

DbTable::DbTable(PMR_NS::memory_resource* mr, DbIndex db_index)
    : prime(kInitSegmentLog, detail::PrimeTablePolicy{}, mr),
      expire(0, detail::ExpireTablePolicy{}, mr),
      mcflag(0, detail::ExpireTablePolicy{}, mr),
      index(db_index) {
  if (IsClusterEnabled()) {
    slots_stats.reset(new SlotStats[kMaxSlotNum + 1]);
  }
  thread_index = ServerState::tlocal()->thread_index();
}

DbTable::~DbTable() {
  DCHECK_EQ(thread_index, ServerState::tlocal()->thread_index());
  delete sample_top_keys;
  delete sample_unique_keys;
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
