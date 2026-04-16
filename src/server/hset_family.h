// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <optional>

#include "facade/op_status.h"
#include "server/common.h"
#include "server/table.h"
namespace dfly {

class StringMap;

using facade::OpResult;
using facade::OpStatus;

class HSetFamily {
 public:
  static void Register(CommandRegistry* registry);

  static LoadBlobResult LoadZiplistBlob(std::string_view blob, PrimeValue* pv);
  static LoadBlobResult LoadListpackBlob(std::string_view blob, PrimeValue* pv);

  // Does not free lp.
  static StringMap* ConvertToStrMap(uint8_t* lp);

  static int32_t FieldExpireTime(const DbContext& db_context, const PrimeValue& pv,
                                 std::string_view field);

  // Delete the hash key if it became empty after lazy field expiry.
  // Returns true if the key was deleted.
  static bool DeleteIfEmpty(DbSlice& db_slice, const DbContext& db_cntx, std::string_view key,
                            const PrimeValue& pv);

  static std::vector<long> SetFieldsExpireTime(const OpArgs& op_args, uint32_t ttl_sec,
                                               ExpireFlags flags, std::string_view key,
                                               CmdArgList values, PrimeValue* pv);
};

}  // namespace dfly
