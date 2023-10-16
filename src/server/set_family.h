// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "facade/op_status.h"
#include "server/common.h"
#include "server/table.h"

typedef struct intset intset;
typedef struct redisObject robj;
typedef struct dict dict;

namespace dfly {

using facade::OpResult;

class ConnectionContext;
class CommandRegistry;
class EngineShard;

class SetFamily {
 public:
  static void Register(CommandRegistry* registry);

  static uint32_t MaxIntsetEntries();

  static void ConvertTo(const intset* src, dict* dest);

  // Returns true if succeeded, false on OOM.
  static bool ConvertToStrSet(const intset* is, size_t expected_len, robj* dest);

  // returns expiry time in seconds since kMemberExpiryBase date.
  // returns -3 if field was not found, -1 if no ttl is associated with the item.
  static int32_t FieldExpireTime(const DbContext& db_context, const PrimeValue& pv,
                                 std::string_view field);

 private:
};

}  // namespace dfly
