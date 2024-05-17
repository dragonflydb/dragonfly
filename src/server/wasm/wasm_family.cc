// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/wasm/wasm_family.h"

#include "absl/strings/str_cat.h"
#include "base/flags.h"
#include "facade/facade_types.h"
#include "facade/service_interface.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/conn_context.h"

ABSL_FLAG(std::string, wasmpaths, "",
          "Comma separated list of paths (including wasm file) to load WASM modules from");

namespace dfly {
namespace wasm {

using MemberFunc = void (WasmFamily::*)(CmdArgList args, ConnectionContext* cntx);

CommandId::Handler HandlerFunc(WasmFamily* wasm, MemberFunc f) {
  return [=](CmdArgList args, ConnectionContext* cntx) { return (wasm->*f)(args, cntx); };
}

#define HFUNC(x) SetHandler(HandlerFunc(this, &WasmFamily::x))

WasmFamily::WasmFamily(facade::ServiceInterface& service) {
  if (auto wasm_modules = absl::GetFlag(FLAGS_wasmpaths); !wasm_modules.empty()) {
    registry_ = std::make_unique<WasmRegistry>(service);
  }
}

void WasmFamily::Register(dfly::CommandRegistry* registry) {
  using CI = dfly::CommandId;
  registry->StartFamily();
  *registry << CI{"WASMCALL", dfly::CO::LOADING, 3, 0, 0, acl::WASM}.HFUNC(Call);
  //  *registry << CI{"WASMLOAD", dfly::CO::LOADING, 2, 0, 0, acl::WASM}.HFUNC(Load);
  //  *registry << CI{"WASMDEL", dfly::CO::LOADING, 2, 0, 0, acl::WASM}.HFUNC(Delete);
}

void WasmFamily::Load(CmdArgList args, ConnectionContext* cntx) {
  // TODO figure out how to load modules dynamically
  //     auto path = absl::StrCat(facade::ToSV(args[0]), "\0");
  //     if (auto res = registry_->Add(path); !res.empty()) {
  //       cntx->SendError(res);
  //       return;
  //     }
  //     auto slash = path.rfind('/');
  //     auto name = path;
  //     if (slash != path.npos) {
  //       name = name.substr(slash + 1);
  //     }
  //     cntx->SendOk();
}

void WasmFamily::Call(CmdArgList args, ConnectionContext* cntx) {
  if (!registry_) {
    cntx->SendError("Wasm is not enabled");
    return;
  }
  auto module_name = facade::ToSV(args[0]);
  auto exported_fun_name = facade::ToSV(args[1]);
  auto res = registry_->GetInstanceFromModule(module_name);
  if (!res) {
    cntx->SendError(absl::StrCat("Could not find module with ", module_name));
    return;
  }
  auto& wasm_function = *res;
  // This is fine because we block and is atomic in respect to proactor.
  // When we switch to async execution of the runtime this will be able to suspend and resume
  // which will make the current approach invalid.
  auto wasm_result = wasm_function(exported_fun_name);
  if (!wasm_result.empty()) {
    cntx->SendError(wasm_result);
    return;
  }
  cntx->SendOk();
}

void WasmFamily::Delete(CmdArgList args, ConnectionContext* cntx) {
  // TODO figure out how to load modules dynamically
  auto name = facade::ToSV(args[0]);
  cntx->SendLong(registry_->Delete(name));
}

}  // namespace wasm
}  // namespace dfly
