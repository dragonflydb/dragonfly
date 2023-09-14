// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "facade/op_status.h"
#include "server/common.h"

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

 private:
};

}  // namespace dfly
