// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <optional>

#include "facade/op_status.h"
#include "server/command_registry.h"
#include "server/common.h"
#include "server/table.h"
namespace dfly {

class StringMap;

using facade::OpResult;
using facade::OpStatus;

class HSetFamily {
 public:
  static void Register(CommandRegistry* registry);

  // Does not free lp.
  static StringMap* ConvertToStrMap(uint8_t* lp);

  static int32_t FieldExpireTime(const DbContext& db_context, const PrimeValue& pv,
                                 std::string_view field);

  static std::vector<long> SetFieldsExpireTime(const OpArgs& op_args, uint32_t ttl_sec,
                                               ExpireFlags flags, std::string_view key,
                                               CmdArgList values, PrimeValue* pv);

 private:
  using SinkReplyBuilder = facade::SinkReplyBuilder;

  static void HExpire(CmdArgList args, CommandContext* cmd_cntx);
  static void HDel(CmdArgList args, CommandContext* cmd_cntx);
  static void HLen(CmdArgList args, CommandContext* cmd_cntx);
  static void HExists(CmdArgList args, CommandContext* cmd_cntx);
  static void HGet(CmdArgList args, CommandContext* cmd_cntx);
  static void HMGet(CmdArgList args, CommandContext* cmd_cntx);
  static void HIncrBy(CmdArgList args, CommandContext* cmd_cntx);
  static void HKeys(CmdArgList args, CommandContext* cmd_cntx);
  static void HVals(CmdArgList args, CommandContext* cmd_cntx);
  static void HGetAll(CmdArgList args, CommandContext* cmd_cntx);
  static void HIncrByFloat(CmdArgList args, CommandContext* cmd_cntx);
  static void HScan(CmdArgList args, CommandContext* cmd_cntx);
  static void HSet(CmdArgList args, CommandContext* cmd_cntx);
  static void HSetNx(CmdArgList args, CommandContext* cmd_cntx);
  static void HStrLen(CmdArgList args, CommandContext* cmd_cntx);
  static void HRandField(CmdArgList args, CommandContext* cmd_cntx);
};

}  // namespace dfly
