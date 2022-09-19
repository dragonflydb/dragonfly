// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <optional>

#include "facade/op_status.h"
#include "server/common.h"

namespace dfly {

class ConnectionContext;
class CommandRegistry;
using facade::OpResult;
using facade::OpStatus;

class HSetFamily {
 public:
  static void Register(CommandRegistry* registry);
  static uint32_t MaxListPackLen();

 private:
  enum GetAllMode : uint8_t { FIELDS = 1, VALUES = 2 };

  static void HDel(CmdArgList args, ConnectionContext* cntx);
  static void HLen(CmdArgList args, ConnectionContext* cntx);
  static void HExists(CmdArgList args, ConnectionContext* cntx);
  static void HGet(CmdArgList args, ConnectionContext* cntx);
  static void HMGet(CmdArgList args, ConnectionContext* cntx);
  static void HIncrBy(CmdArgList args, ConnectionContext* cntx);
  static void HKeys(CmdArgList args, ConnectionContext* cntx);
  static void HVals(CmdArgList args, ConnectionContext* cntx);
  static void HGetAll(CmdArgList args, ConnectionContext* cntx);
  static void HIncrByFloat(CmdArgList args, ConnectionContext* cntx);
  static void HScan(CmdArgList args, ConnectionContext* cntx);
  static void HSet(CmdArgList args, ConnectionContext* cntx);
  static void HSetNx(CmdArgList args, ConnectionContext* cntx);
  static void HStrLen(CmdArgList args, ConnectionContext* cntx);
  static void HRandField(CmdArgList args, ConnectionContext* cntx);

  static void HGetGeneric(CmdArgList args, ConnectionContext* cntx, uint8_t getall_mask);

  static OpResult<uint32_t> OpSet(const OpArgs& op_args, std::string_view key, CmdArgList values,
                                  bool skip_if_exists);
  static OpResult<uint32_t> OpDel(const OpArgs& op_args, std::string_view key, CmdArgList values);

  using OptStr = std::optional<std::string>;
  static OpResult<std::vector<OptStr>> OpMGet(const OpArgs& op_args, std::string_view key,
                                              CmdArgList fields);

  static OpResult<uint32_t> OpLen(const OpArgs& op_args, std::string_view key);

  static OpResult<std::string> OpGet(const OpArgs& op_args, std::string_view key,
                                     std::string_view field);

  static OpResult<std::vector<std::string>> OpGetAll(const OpArgs& op_args, std::string_view key,
                                                     uint8_t getall_mask);
  static OpResult<size_t> OpStrLen(const OpArgs& op_args, std::string_view key,
                                   std::string_view field);

  using IncrByParam = std::variant<double, int64_t>;
  static OpStatus OpIncrBy(const OpArgs& op_args, std::string_view key, std::string_view field,
                           IncrByParam* param);

  static OpResult<StringVec> OpScan(const OpArgs& op_args, std::string_view key, uint64_t* cursor);
};

}  // namespace dfly
