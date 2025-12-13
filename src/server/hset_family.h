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
};

}  // namespace dfly
