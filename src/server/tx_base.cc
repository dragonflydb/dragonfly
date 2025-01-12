// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tx_base.h"

#include "base/logging.h"
#include "facade/facade_types.h"
#include "server/cluster/cluster_defs.h"
#include "server/engine_shard_set.h"
#include "server/journal/journal.h"
#include "server/namespaces.h"
#include "server/transaction.h"

namespace dfly {

using namespace std;
using Payload = journal::Entry::Payload;

unsigned KeyIndex::operator*() const {
  if (bonus)
    return *bonus;
  return start;
}

KeyIndex& KeyIndex::operator++() {
  if (bonus)
    bonus.reset();
  else
    start = std::min(end, start + step);
  return *this;
}

bool KeyIndex::operator!=(const KeyIndex& ki) const {
  return std::tie(start, end, step, bonus) != std::tie(ki.start, ki.end, ki.step, ki.bonus);
}

DbSlice& DbContext::GetDbSlice(ShardId shard_id) const {
  return ns->GetDbSlice(shard_id);
}

DbSlice& OpArgs::GetDbSlice() const {
  return db_cntx.GetDbSlice(shard->shard_id());
}

size_t ShardArgs::Size() const {
  size_t sz = 0;
  for (const auto& s : slice_.second)
    sz += (s.second - s.first);
  return sz;
}

void RecordJournal(const OpArgs& op_args, string_view cmd, const ShardArgs& args,
                   uint32_t shard_cnt) {
  VLOG(2) << "Logging command " << cmd << " from txn " << op_args.tx->txid();
  op_args.tx->LogJournalOnShard(op_args.shard, Payload(cmd, args), shard_cnt);
}

void RecordJournal(const OpArgs& op_args, std::string_view cmd, facade::ArgSlice args,
                   uint32_t shard_cnt) {
  VLOG(2) << "Logging command " << cmd << " from txn " << op_args.tx->txid();
  op_args.tx->LogJournalOnShard(op_args.shard, Payload(cmd, args), shard_cnt);
}

void RecordExpiry(DbIndex dbid, string_view key) {
  auto journal = EngineShard::tlocal()->journal();
  CHECK(journal);

  journal->RecordEntry(0, journal::Op::EXPIRED, dbid, 1, KeySlot(key),
                       Payload("DEL", ArgSlice{key}));
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
