// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include "server/common.h"
#include "server/table.h"

namespace dfly {
namespace journal {

enum class Op : uint8_t {
  NOOP = 0,
  LOCK = 1,
  UNLOCK = 2,
  LOCK_SHARD = 3,
  UNLOCK_SHARD = 4,
  SCHED = 5,
  VAL = 10,
  DEL,
  MSET,
};

// TODO: to pass all the attributes like ttl, stickiness etc.
struct Entry {
  Entry(Op op, DbIndex did, TxId tid, std::string_view skey)
      : opcode(op), db_ind(did), txid(tid), key(skey) {
  }

  Entry(DbIndex did, TxId tid, std::string_view skey, const PrimeValue& pval)
      : Entry(Op::VAL, did, tid, skey) {
    pval_ptr = &pval;
  }

  static Entry Sched(TxId tid) {
    return Entry{Op::SCHED, 0, tid, {}};
  }

  Op opcode;
  DbIndex db_ind;
  TxId txid;
  std::string_view key;
  const PrimeValue* pval_ptr = nullptr;
  uint64_t expire_ms = 0;  // 0 means no expiry.
};

using ChangeCallback = std::function<void(const Entry&)>;

}  // namespace journal
}  // namespace dfly
