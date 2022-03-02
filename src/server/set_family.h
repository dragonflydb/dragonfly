// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "facade/op_status.h"
#include "server/common_types.h"

namespace dfly {

using facade::OpResult;

class ConnectionContext;
class CommandRegistry;
class EngineShard;

class SetFamily {
 public:
  static void Register(CommandRegistry* registry);

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


  using StringSet = std::vector<std::string>;

  static OpResult<StringSet> OpUnion(const OpArgs& op_args, const ArgSlice& args);
  static OpResult<StringSet> OpDiff(const Transaction* t, EngineShard* es);
  static OpResult<StringSet> OpInter(const Transaction* t, EngineShard* es, bool remove_first);

  // count - how many elements to pop.
  static OpResult<StringSet> OpPop(const OpArgs& op_args, std::string_view key, unsigned count);

};

}  // namespace dfly
