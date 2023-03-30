// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/common.h"
#include "server/engine_shard_set.h"
#include "util/proactor_pool.h"

namespace dfly {

class ConnectionContext;
class CommandRegistry;
using facade::OpResult;
using facade::OpStatus;

class SetCmd {
  const OpArgs op_args_;
  bool manual_journal_;

 public:
  explicit SetCmd(const OpArgs& op_args, bool manual_journal)
      : op_args_(op_args), manual_journal_{manual_journal} {
  }

  enum SetFlags {
    SET_ALWAYS = 0,
    SET_IF_NOTEXIST = 1 << 0,    /* NX: Set if key not exists. */
    SET_IF_EXISTS = 1 << 1,      /* XX: Set if key exists. */
    SET_KEEP_EXPIRE = 1 << 2,    /* KEEPTTL: Set and keep the ttl */
    SET_GET = 1 << 3,            /* GET: Set if want to get key before set */
    SET_EXPIRE_AFTER_MS = 1 << 4 /* EX,PX,EXAT,PXAT: Expire after ms. */
  };

  struct SetParams {
    uint16_t flags = SET_ALWAYS;
    uint16_t memcache_flags = 0;
    // Relative value based on now. 0 means no expiration.
    uint64_t expire_after_ms = 0;
    mutable std::optional<std::string>* prev_val = nullptr;  // GETSET option

    constexpr bool IsConditionalSet() const {
      return flags & SET_IF_NOTEXIST || flags & SET_IF_EXISTS;
    }
  };

  OpResult<std::optional<std::string>> Set(const SetParams& params, std::string_view key,
                                           std::string_view value);

 private:
  OpStatus SetExisting(const SetParams& params, PrimeIterator it, ExpireIterator e_it,
                       std::string_view key, std::string_view value);

  void RecordJournal(const SetParams& params, std::string_view key, std::string_view value);
};

class StringFamily {
 public:
  static void Init(util::ProactorPool* pp);
  static void Shutdown();

  static void Register(CommandRegistry* registry);

 private:
  static void Append(CmdArgList args, ConnectionContext* cntx);
  static void Decr(CmdArgList args, ConnectionContext* cntx);
  static void DecrBy(CmdArgList args, ConnectionContext* cntx);
  static void Get(CmdArgList args, ConnectionContext* cntx);
  static void GetDel(CmdArgList args, ConnectionContext* cntx);
  static void GetRange(CmdArgList args, ConnectionContext* cntx);
  static void GetSet(CmdArgList args, ConnectionContext* cntx);
  static void GetEx(CmdArgList args, ConnectionContext* cntx);
  static void Incr(CmdArgList args, ConnectionContext* cntx);
  static void IncrBy(CmdArgList args, ConnectionContext* cntx);
  static void IncrByFloat(CmdArgList args, ConnectionContext* cntx);
  static void MGet(CmdArgList args, ConnectionContext* cntx);
  static void MSet(CmdArgList args, ConnectionContext* cntx);
  static void MSetNx(CmdArgList args, ConnectionContext* cntx);

  static void Set(CmdArgList args, ConnectionContext* cntx);
  static void SetEx(CmdArgList args, ConnectionContext* cntx);
  static void SetNx(CmdArgList args, ConnectionContext* cntx);
  static void SetRange(CmdArgList args, ConnectionContext* cntx);
  static void StrLen(CmdArgList args, ConnectionContext* cntx);
  static void Prepend(CmdArgList args, ConnectionContext* cntx);
  static void PSetEx(CmdArgList args, ConnectionContext* cntx);

  static void ClThrottle(CmdArgList args, ConnectionContext* cntx);

  // These functions are used internally, they do not implement any specific command
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
};

}  // namespace dfly
