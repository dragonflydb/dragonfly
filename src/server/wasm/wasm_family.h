// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "facade/facade_types.h"
#include "server/command_registry.h"

namespace dfly {

class ConnectionContext;
namespace wasm {

class WasmFamily final {
 public:
  void Register(CommandRegistry* registry);

 private:
  void CallWasm(CmdArgList args, ConnectionContext* cntx);
};

}  // namespace wasm
}  // namespace dfly
