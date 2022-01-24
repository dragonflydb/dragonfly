// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "core/op_status.h"
#include "server/common_types.h"

namespace util {
class ProactorPool;
}  // namespace util

namespace dfly {

class ConnectionContext;
class CommandRegistry;
class EngineShard;

class GenericFamily {
 public:
  static void Init(util::ProactorPool* pp);
  static void Shutdown();

  static void Register(CommandRegistry* registry);

 private:
  enum TimeUnit { SEC, MSEC };

  struct ExpireParams {
    int64_t ts;
    bool absolute = false;

    TimeUnit unit = SEC;
  };

  static void Del(CmdArgList args, ConnectionContext* cntx);
  static void Ping(CmdArgList args, ConnectionContext* cntx);
  static void Exists(CmdArgList args, ConnectionContext* cntx);
  static void Expire(CmdArgList args, ConnectionContext* cntx);
  static void ExpireAt(CmdArgList args, ConnectionContext* cntx);

  static void Rename(CmdArgList args, ConnectionContext* cntx);
  static void RenameNx(CmdArgList args, ConnectionContext* cntx);
  static void Ttl(CmdArgList args, ConnectionContext* cntx);
  static void Pttl(CmdArgList args, ConnectionContext* cntx);

  static void Echo(CmdArgList args, ConnectionContext* cntx);
  static void Select(CmdArgList args, ConnectionContext* cntx);
  static void Scan(CmdArgList args, ConnectionContext* cntx);
  static void Type(CmdArgList args, ConnectionContext* cntx);

  static OpResult<void> RenameGeneric(CmdArgList args, bool skip_exist_dest,
                                      ConnectionContext* cntx);
  static void TtlGeneric(CmdArgList args, ConnectionContext* cntx, TimeUnit unit);

  static OpStatus OpExpire(const OpArgs& op_args, std::string_view key, const ExpireParams& params);

  static OpResult<uint64_t> OpTtl(Transaction* t, EngineShard* shard, std::string_view key);
  static OpResult<uint32_t> OpDel(const OpArgs& op_args, ArgSlice keys);
  static OpResult<uint32_t> OpExists(const OpArgs& op_args, ArgSlice keys);
  static OpResult<void> OpRen(const OpArgs& op_args, std::string_view from, std::string_view to,
                              bool skip_exists);
  static void OpScan(const OpArgs& op_args, uint64_t* cursor, std::vector<std::string>* vec);
};

}  // namespace dfly
