// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "facade/op_status.h"
#include "server/common.h"

namespace dfly {

using facade::OpResult;

class ConnectionContext;
class CommandRegistry;
class EngineShard;

class ListFamily {
 public:
  static void Register(CommandRegistry* registry);

 private:
  static void LPush(CmdArgList args, ConnectionContext* cntx);
  static void LPushX(CmdArgList args, ConnectionContext* cntx);
  static void RPush(CmdArgList args, ConnectionContext* cntx);
  static void RPushX(CmdArgList args, ConnectionContext* cntx);
  static void LPop(CmdArgList args, ConnectionContext* cntx);
  static void RPop(CmdArgList args, ConnectionContext* cntx);
  static void BLPop(CmdArgList args, ConnectionContext* cntx);
  static void BRPop(CmdArgList args, ConnectionContext* cntx);
  static void LLen(CmdArgList args, ConnectionContext* cntx);
  static void LIndex(CmdArgList args, ConnectionContext* cntx);
  static void LInsert(CmdArgList args, ConnectionContext* cntx);
  static void LTrim(CmdArgList args, ConnectionContext* cntx);
  static void LRange(CmdArgList args, ConnectionContext* cntx);
  static void LRem(CmdArgList args, ConnectionContext* cntx);
  static void LSet(CmdArgList args, ConnectionContext* cntx);
  static void RPopLPush(CmdArgList args, ConnectionContext* cntx);

  static void PopGeneric(ListDir dir, CmdArgList args, ConnectionContext* cntx);
  static void PushGeneric(ListDir dir, bool skip_notexist, CmdArgList args,
                          ConnectionContext* cntx);

  static void BPopGeneric(ListDir dir, CmdArgList args, ConnectionContext* cntx);

  static OpResult<uint32_t> OpPush(const OpArgs& op_args, std::string_view key, ListDir dir,
                                   bool skip_notexist, absl::Span<std::string_view> vals);

  static OpResult<StringVec> OpPop(const OpArgs& op_args, std::string_view key, ListDir dir,
                                   uint32_t count);

  static OpResult<uint32_t> OpLen(const OpArgs& op_args, std::string_view key);
  static OpResult<std::string> OpIndex(const OpArgs& op_args, std::string_view key, long index);
  static OpResult<int> OpInsert(const OpArgs& op_args, std::string_view key, std::string_view pivot,
                                std::string_view elem, int insert_param);

  static OpResult<uint32_t> OpRem(const OpArgs& op_args, std::string_view key,
                                  std::string_view elem, long count);
  static facade::OpStatus OpSet(const OpArgs& op_args, std::string_view key, std::string_view elem,
                                long count);
  static facade::OpStatus OpTrim(const OpArgs& op_args, std::string_view key, long start, long end);

  static OpResult<StringVec> OpRange(const OpArgs& op_args, std::string_view key, long start,
                                     long end);

  static OpResult<std::string> OpRPopLPushSingleShard(const OpArgs& op_args, std::string_view src,
                                                      std::string_view dest);
};

}  // namespace dfly
