// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tx_base.h"

#include "base/logging.h"
#include "facade/facade_types.h"
#include "server/cluster/cluster_defs.h"
#include "server/engine_shard_set.h"
#include "server/journal/journal.h"
#include "server/transaction.h"

namespace dfly {

using namespace std;
using Payload = journal::Entry::Payload;

size_t ShardArgs::Size() const {
  size_t sz = 0;
  for (const auto& s : slice_.second)
    sz += (s.second - s.first);
  return sz;
}

void RecordJournal(const OpArgs& op_args, string_view cmd, const ShardArgs& args,
                   uint32_t shard_cnt, bool multi_commands) {
  VLOG(2) << "Logging command " << cmd << " from txn " << op_args.tx->txid();
  op_args.tx->LogJournalOnShard(op_args.shard, Payload(cmd, args), shard_cnt, multi_commands,
                                false);
}

void RecordJournal(const OpArgs& op_args, std::string_view cmd, facade::ArgSlice args,
                   uint32_t shard_cnt, bool multi_commands) {
  VLOG(2) << "Logging command " << cmd << " from txn " << op_args.tx->txid();
  op_args.tx->LogJournalOnShard(op_args.shard, Payload(cmd, args), shard_cnt, multi_commands,
                                false);
}

void RecordJournalFinish(const OpArgs& op_args, uint32_t shard_cnt) {
  op_args.tx->FinishLogJournalOnShard(op_args.shard, shard_cnt);
}

void RecordExpiry(DbIndex dbid, string_view key) {
  auto journal = EngineShard::tlocal()->journal();
  CHECK(journal);
  journal->RecordEntry(0, journal::Op::EXPIRED, dbid, 1, cluster::KeySlot(key),
                       Payload("DEL", ArgSlice{key}), false);
}

void TriggerJournalWriteToSink() {
  auto journal = EngineShard::tlocal()->journal();
  CHECK(journal);
  journal->RecordEntry(0, journal::Op::NOOP, 0, 0, nullopt, {}, true);
}

std::ostream& operator<<(std::ostream& os, ArgSlice list) {
  os << "[";
  if (!list.empty()) {
    std::for_each(list.begin(), list.end() - 1, [&os](const auto& val) { os << val << ", "; });
    os << (*(list.end() - 1));
  }
  return os << "]";
}

LockTag::LockTag(std::string_view key) {
  if (LockTagOptions::instance().enabled)
    str_ = LockTagOptions::instance().Tag(key);
  else
    str_ = key;
}

LockFp LockTag::Fingerprint() const {
  return XXH64(str_.data(), str_.size(), 0x1C69B3F74AC4AE35UL);
}

}  // namespace dfly
