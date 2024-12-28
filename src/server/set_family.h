// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "facade/op_status.h"
#include "server/table.h"
#include "server/tx_base.h"

typedef struct intset intset;

namespace dfly {

using facade::OpResult;

class CommandRegistry;
class StringSet;

class SetFamily {
 public:
  static void Register(CommandRegistry* registry);

  static uint32_t MaxIntsetEntries();

  // Returns nullptr on OOM.
  static StringSet* ConvertToStrSet(const intset* is, size_t expected_len);

  // returns expiry time in seconds since kMemberExpiryBase date.
  // returns -3 if field was not found, -1 if no ttl is associated with the item.
  static int32_t FieldExpireTime(const DbContext& db_context, const PrimeValue& pv,
                                 std::string_view field);

  static std::vector<long> SetFieldsExpireTime(const OpArgs& op_args, uint32_t ttl_sec,
                                               CmdArgList values, PrimeValue* pv);
};

}  // namespace dfly
