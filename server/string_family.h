// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/common_types.h"
#include "server/engine_shard_set.h"
#include "util/proactor_pool.h"

namespace dfly {

class ConnectionContext;
class CommandRegistry;

class SetCmd {
  DbSlice* db_slice_;

 public:
  explicit SetCmd(DbSlice* db_slice);
  ~SetCmd();

  enum SetHow { SET_ALWAYS, SET_IF_NOTEXIST, SET_IF_EXISTS };

  struct SetParams {
    SetHow how = SET_ALWAYS;
    DbIndex db_index;

    // Relative value based on now. 0 means no expiration.
    uint64_t expire_after_ms = 0;
    mutable std::optional<std::string>* prev_val = nullptr;  // GETSET option
    bool keep_expire = false;                                // KEEPTTL - TODO: to implement it.

    explicit SetParams(DbIndex dib) : db_index(dib) {
    }
  };

  OpResult<void> Set(const SetParams& params, std::string_view key, std::string_view value);

 private:
  OpResult<void> SetExisting(DbIndex db_ind, std::string_view value, uint64_t expire_at_ms,
                             MainIterator dest, ExpireIterator exp_it);
};

class StringFamily {
 public:
  static void Init(util::ProactorPool* pp);
  static void Shutdown();

  static void Register(CommandRegistry* registry);

 private:
  static void Set(CmdArgList args, ConnectionContext* cntx);
  static void Get(CmdArgList args, ConnectionContext* cntx);
  static void GetSet(CmdArgList args, ConnectionContext* cntx);
  static void MGet(CmdArgList args, ConnectionContext* cntx);
  static void MSet(CmdArgList args, ConnectionContext* cntx);
  static void Incr(CmdArgList args, ConnectionContext* cntx);
  static void IncrBy(CmdArgList args, ConnectionContext* cntx);
  static void Decr(CmdArgList args, ConnectionContext* cntx);
  static void DecrBy(CmdArgList args, ConnectionContext* cntx);

  static void IncrByGeneric(std::string_view key, int64_t val, ConnectionContext* cntx);

  using MGetResponse = std::vector<std::optional<std::string>>;

  static MGetResponse OpMGet(const Transaction* t, EngineShard* shard);

  static OpStatus OpMSet(const Transaction* t, EngineShard* es);
  static OpResult<int64_t> OpIncrBy(const OpArgs& op_args, std::string_view key, int64_t val);
};

}  // namespace dfly
