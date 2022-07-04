// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "facade/op_status.h"
#include "server/common.h"


typedef struct intset intset;
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

 private:
  static void SAdd(CmdArgList args,  ConnectionContext* cntx);
  static void SIsMember(CmdArgList args,  ConnectionContext* cntx);
  static void SRem(CmdArgList args,  ConnectionContext* cntx);
  static void SCard(CmdArgList args,  ConnectionContext* cntx);
  static void SPop(CmdArgList args,  ConnectionContext* cntx);
  static void SUnion(CmdArgList args,  ConnectionContext* cntx);
  static void SUnionStore(CmdArgList args,  ConnectionContext* cntx);
  static void SDiff(CmdArgList args,  ConnectionContext* cntx);
  static void SDiffStore(CmdArgList args,  ConnectionContext* cntx);
  static void SMembers(CmdArgList args,  ConnectionContext* cntx);
  static void SMove(CmdArgList args,  ConnectionContext* cntx);
  static void SInter(CmdArgList args,  ConnectionContext* cntx);
  static void SInterStore(CmdArgList args,  ConnectionContext* cntx);
  static void SScan(CmdArgList args,  ConnectionContext* cntx);

  // count - how many elements to pop.
  static OpResult<StringVec> OpPop(const OpArgs& op_args, std::string_view key, unsigned count);
  static OpResult<StringVec> OpScan(const OpArgs& op_args, std::string_view key, uint64_t* cursor);

};

}  // namespace dfly
