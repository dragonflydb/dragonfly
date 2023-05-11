// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>
#include <string_view>

#include "glog/logging.h"
#include "server/engine_shard_set.h"
#include "server/server_state.h"
#include "server/table.h"
#include "server/transaction.h"
#include "src/facade/op_status.h"

namespace dfly {

struct Transaction;

struct ShardFFResult {
  PrimeKey key;
  ShardId sid = kInvalidSid;
};

OpResult<ShardFFResult> FindFirst(Transaction* trans, int req_obj_type);

/**
 * Used to implement blocking commands that can watch multiple keys.
 */
template <class R> class MKBlocking {
  using cb_t = std::function<R(Transaction*, EngineShard*, std::string_view)>;

 public:
  explicit MKBlocking(cb_t&& cb) : cb_(std::move(cb)) {
  }

  // Returns WRONG_TYPE, OK.
  // If OK is returned then use Result() to fetch the value.
  facade::OpStatus Run(Transaction* trans, int req_obj_type, unsigned limit_ms) {
    auto limit_tp = limit_ms
                        ? std::chrono::steady_clock::now() + std::chrono::milliseconds(limit_ms)
                        : Transaction::time_point::max();
    bool is_multi = trans->IsMulti();

    trans->Schedule();

    auto* stats = ServerState::tl_connection_stats();

    OpResult<ShardFFResult> result = FindFirst(trans, req_obj_type);

    if (result.ok()) {
      ff_result_ = std::move(result.value());
    } else if (result.status() == OpStatus::KEY_NOTFOUND) {
      // Close transaction and return.
      if (is_multi) {
        auto cb = [](Transaction* t, EngineShard* shard) { return OpStatus::OK; };
        trans->Execute(std::move(cb), true);
        return OpStatus::TIMED_OUT;
      }

      auto wcb = [](Transaction* t, EngineShard* shard) {
        return t->GetShardArgs(shard->shard_id());
      };

      VLOG(1) << "Blocking BLPOP " << trans->DebugId();
      ++stats->num_blocked_clients;
      bool wait_succeeded = trans->WaitOnWatch(limit_tp, std::move(wcb));
      --stats->num_blocked_clients;

      if (!wait_succeeded)
        return OpStatus::TIMED_OUT;
    } else {
      // Could be the wrong-type error.
      // cleanups, locks removal etc.
      auto cb = [](Transaction* t, EngineShard* shard) { return OpStatus::OK; };
      trans->Execute(std::move(cb), true);

      DCHECK_NE(result.status(), OpStatus::KEY_NOTFOUND);
      return result.status();
    }

    auto cb = [this](Transaction* t, EngineShard* shard) {
      if (auto wake_key = t->GetWakeKey(shard->shard_id()); wake_key) {
        key_ = *wake_key;
        value_ = cb_(t, shard, key_);
      } else if (shard->shard_id() == ff_result_.sid) {
        ff_result_.key.GetString(&key_);
        value_ = cb_(t, shard, key_);
      }
      return OpStatus::OK;
    };
    trans->Execute(std::move(cb), true);

    return OpStatus::OK;
  }

  std::string_view Key() const {
    return key_;
  }

  const R& Result() const {
    return *value_;
  }

 private:
  ShardFFResult ff_result_;

  cb_t cb_;
  std::string key_;
  OpResult<R> value_;
};

}  // namespace dfly
