// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/wasm/wasm_family.h"

#include "absl/strings/str_cat.h"
#include "facade/facade_types.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/wasm/engine_example.h"

namespace dfly::wasm {

using MemberFunc = void (WasmFamily::*)(CmdArgList args, ConnectionContext* cntx);

CommandId::Handler HandlerFunc(WasmFamily* wasm, MemberFunc f) {
  return [=](CmdArgList args, ConnectionContext* cntx) { return (wasm->*f)(args, cntx); };
}

#define HFUNC(x) SetHandler(HandlerFunc(this, &WasmFamily::x))

void WasmFamily::Register(dfly::CommandRegistry* registry) {
  using CI = dfly::CommandId;
  registry->StartFamily();
  *registry << CI{"CALLWASM", dfly::CO::LOADING, 2, 0, 0, acl::WASM}.HFUNC(CallWasm);
}

void WasmFamily::CallWasm(CmdArgList args, ConnectionContext* cntx) {
  std::string path = absl::StrCat(facade::ToSV(args[0]), "\0");
  EngineCall(path, cntx);
}

}  // namespace dfly::wasm
