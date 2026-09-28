// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "facade/facade_types.h"
#include "server/table.h"
#include "server/tx_base.h"

typedef struct intset intset;

namespace dfly {

using facade::OpResult;

// Captures the selected SET implementation after command-line flags have been parsed.
void InitSetFamilyFlags();

class StringSet;

class SetFamily {
 public:
  static void Register(CommandRegistry* registry);

  static LoadBlobResult LoadIntSetBlob(std::string_view blob, bool deep, PrimeValue* pv);
  static LoadBlobResult LoadLPSetBlob(std::string_view blob, bool deep, PrimeValue* pv);

  static uint32_t MaxIntsetEntries();

  // Returns nullptr on OOM. The returned pointer is StringSet* if --use_oah_set is false,
  // or OAHSet* if --use_oah_set is true. Callers store it as a void* in CompactObj and
  // dispatch via dfly::g_use_oah_set.
  static void* ConvertToStrSet(const intset* is, size_t expected_len);

  static std::vector<long> SetFieldsExpireTime(const OpArgs& op_args, uint32_t ttl_sec,
                                               facade::CmdArgList values, PrimeValue* pv);
};

}  // namespace dfly
