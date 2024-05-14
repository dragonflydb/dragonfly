// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "facade/facade_types.h"
#include "server/command_registry.h"
#include "server/wasm/wasm_registry.h"

namespace dfly {

class ConnectionContext;
namespace wasm {

class WasmFamily final {
 public:
  void Register(CommandRegistry* registry);

 private:
  void Load(facade::CmdArgList args, ConnectionContext* cntx);
  void Call(facade::CmdArgList args, ConnectionContext* cntx);
  void Delete(facade::CmdArgList args, ConnectionContext* cntx);

  WasmRegistry registry_;
};

}  // namespace wasm
}  // namespace dfly
