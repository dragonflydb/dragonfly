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
using facade::OpStatus;
using facade::OpResult;

class SetCmd {
  DbSlice* db_slice_;

 public:
  explicit SetCmd(DbSlice* db_slice);
  ~SetCmd();

  enum SetHow { SET_ALWAYS, SET_IF_NOTEXIST, SET_IF_EXISTS };

  struct SetParams {
    SetHow how = SET_ALWAYS;
    DbIndex db_index;

    uint32_t memcache_flags = 0;
    // Relative value based on now. 0 means no expiration.
    uint64_t expire_after_ms = 0;
    mutable std::optional<std::string>* prev_val = nullptr;  // GETSET option
    bool keep_expire = false;                                // KEEPTTL - TODO: to implement it.

    explicit SetParams(DbIndex dib) : db_index(dib) {
    }
  };

  OpResult<void> Set(const SetParams& params, std::string_view key, std::string_view value);
};

class StringFamily {
 public:
  static void Init(util::ProactorPool* pp);
  static void Shutdown();

  static void Register(CommandRegistry* registry);

 private:
  static void Set(CmdArgList args, ConnectionContext* cntx);
  static void SetEx(CmdArgList args, ConnectionContext* cntx);
  static void Get(CmdArgList args, ConnectionContext* cntx);
  static void GetSet(CmdArgList args, ConnectionContext* cntx);
  static void MGet(CmdArgList args, ConnectionContext* cntx);
  static void MSet(CmdArgList args, ConnectionContext* cntx);
  static void Incr(CmdArgList args, ConnectionContext* cntx);
  static void IncrBy(CmdArgList args, ConnectionContext* cntx);
  static void Decr(CmdArgList args, ConnectionContext* cntx);
  static void DecrBy(CmdArgList args, ConnectionContext* cntx);
  static void Append(CmdArgList args, ConnectionContext* cntx);
  static void Prepend(CmdArgList args, ConnectionContext* cntx);
  static void StrLen(CmdArgList args, ConnectionContext* cntx);
  static void GetRange(CmdArgList args, ConnectionContext* cntx);
  static void SetRange(CmdArgList args, ConnectionContext* cntx);
  static void PSetEx(CmdArgList args, ConnectionContext* cntx);

  static void IncrByGeneric(std::string_view key, int64_t val, ConnectionContext* cntx);
  static void ExtendGeneric(CmdArgList args, bool prepend, ConnectionContext* cntx);
  static void SetExGeneric(bool seconds, CmdArgList args, ConnectionContext* cntx);

  struct GetResp {
    std::string value;
    uint64_t mc_ver = 0;  // 0 means we do not output it (i.e has not been requested).
    uint32_t mc_flag = 0;
  };

  using MGetResponse = std::vector<std::optional<GetResp>>;
  static MGetResponse OpMGet(bool fetch_mcflag, bool fetch_mcver, const Transaction* t,
                             EngineShard* shard);

  static OpStatus OpMSet(const Transaction* t, EngineShard* es);

  // if skip_on_missing - returns KEY_NOTFOUND.
  static OpResult<int64_t> OpIncrBy(const OpArgs& op_args, std::string_view key, int64_t val,
                                    bool skip_on_missing);

  // Returns the length of the extended string. if prepend is false - appends the val.
  static OpResult<uint32_t> ExtendOrSet(const OpArgs& op_args, std::string_view key,
                                        std::string_view val, bool prepend);

  // Returns true if was extended, false if the key was not found.
  static OpResult<bool> ExtendOrSkip(const OpArgs& op_args, std::string_view key,
                                     std::string_view val, bool prepend);
};

}  // namespace dfly
