// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <optional>

#include "facade/op_status.h"
#include "server/common.h"
#include "server/table.h"

namespace dfly {

class CommandRegistry;
class StringMap;
class Transaction;

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
                                               std::string_view key, CmdArgList values,
                                               PrimeValue* pv);

 private:
  using SinkReplyBuilder = facade::SinkReplyBuilder;

  static void HExpire(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void HDel(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void HLen(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void HExists(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void HGet(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void HMGet(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void HIncrBy(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void HKeys(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void HVals(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void HGetAll(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void HIncrByFloat(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void HScan(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void HSet(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder,
                   ConnectionContext* cntx);
  static void HSetNx(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void HStrLen(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  static void HRandField(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
};

}  // namespace dfly
