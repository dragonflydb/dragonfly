// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "facade/op_status.h"
#include "server/common_types.h"

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
  static void LLen(CmdArgList args, ConnectionContext* cntx);
  static void LIndex(CmdArgList args, ConnectionContext* cntx);
  static void LTrim(CmdArgList args, ConnectionContext* cntx);
  static void LRange(CmdArgList args, ConnectionContext* cntx);
  static void LRem(CmdArgList args, ConnectionContext* cntx);
  static void LSet(CmdArgList args, ConnectionContext* cntx);

  static void PopGeneric(ListDir dir, const CmdArgList& args, ConnectionContext* cntx);
  static void PushGeneric(ListDir dir, bool skip_notexist, CmdArgList args,
                          ConnectionContext* cntx);

  static OpResult<uint32_t> OpPush(const OpArgs& op_args, std::string_view key, ListDir dir,
                                   bool skip_notexist, absl::Span<std::string_view> vals);
  static OpResult<std::string> OpPop(const OpArgs& op_args, std::string_view key, ListDir dir);
  static OpResult<uint32_t> OpLen(const OpArgs& op_args, std::string_view key);
  static OpResult<std::string> OpIndex(const OpArgs& op_args, std::string_view key, long index);

  static OpResult<uint32_t> OpRem(const OpArgs& op_args, std::string_view key,
                                  std::string_view elem, long count);
  static facade::OpStatus OpSet(const OpArgs& op_args, std::string_view key, std::string_view elem,
                                long count);
  static facade::OpStatus OpTrim(const OpArgs& op_args, std::string_view key, long start, long end);

  static OpResult<StringVec> OpRange(const OpArgs& op_args, std::string_view key, long start,
                                     long end);
};

}  // namespace dfly
