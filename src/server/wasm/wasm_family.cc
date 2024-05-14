// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/wasm/wasm_family.h"

#include "absl/strings/str_cat.h"
#include "facade/facade_types.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/conn_context.h"

namespace dfly {
namespace wasm {

using MemberFunc = void (WasmFamily::*)(CmdArgList args, ConnectionContext* cntx);

CommandId::Handler HandlerFunc(WasmFamily* wasm, MemberFunc f) {
  return [=](CmdArgList args, ConnectionContext* cntx) { return (wasm->*f)(args, cntx); };
}

#define HFUNC(x) SetHandler(HandlerFunc(this, &WasmFamily::x))

void WasmFamily::Register(dfly::CommandRegistry* registry) {
  using CI = dfly::CommandId;
  registry->StartFamily();
  *registry << CI{"WASMCALL", dfly::CO::LOADING, 2, 0, 0, acl::WASM}.HFUNC(Call);
  *registry << CI{"WASMLOAD", dfly::CO::LOADING, 2, 0, 0, acl::WASM}.HFUNC(Load);
  *registry << CI{"WASMDEL", dfly::CO::LOADING, 2, 0, 0, acl::WASM}.HFUNC(Delete);
}

void WasmFamily::Load(CmdArgList args, ConnectionContext* cntx) {
  auto path = absl::StrCat(facade::ToSV(args[0]), "\0");
  if (auto res = registry_.Add(path); !res.empty()) {
    cntx->SendError(res);
    return;
  }
  cntx->SendOk();
}

void WasmFamily::Call(CmdArgList args, ConnectionContext* cntx) {
  auto name = facade::ToSV(args[0]);
  auto res = registry_.GetInstanceFromModule(name);
  if (!res) {
    cntx->SendError(absl::StrCat("Could not find module with ", name));
    return;
  }
  auto& wasm_instance = *res;
  wasm_instance();
  cntx->SendOk();
}

void WasmFamily::Delete(CmdArgList args, ConnectionContext* cntx) {
  auto name = facade::ToSV(args[0]);
  cntx->SendLong(registry_.Delete(name));
}

}  // namespace wasm
}  // namespace dfly
