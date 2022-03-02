// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "facade/op_status.h"
#include "server/common_types.h"

namespace dfly {

class ConnectionContext;
class CommandRegistry;
using facade::OpResult;

class HSetFamily {
 public:
  static void Register(CommandRegistry* registry);

 private:
  static void HDel(CmdArgList args, ConnectionContext* cntx);
  static void HLen(CmdArgList args, ConnectionContext* cntx);
  static void HExists(CmdArgList args, ConnectionContext* cntx);
  static void HGet(CmdArgList args, ConnectionContext* cntx);
  static void HIncrBy(CmdArgList args, ConnectionContext* cntx);

  // hmset is deprecated, we should not implement it unless we have to.
  static void HSet(CmdArgList args, ConnectionContext* cntx);
  static void HSetNx(CmdArgList args, ConnectionContext* cntx);
  static void HStrLen(CmdArgList args, ConnectionContext* cntx);

  static OpResult<uint32_t> OpHSet(const OpArgs& op_args, std::string_view key, CmdArgList values,
                                   bool skip_if_exists);
};

}  // namespace dfly
