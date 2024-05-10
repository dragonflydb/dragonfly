// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

// This is just a toy example to demonstrate the high level concepts -- there is no encapsulation
// and the code is `raw`

#pragma once

#include <string>

#include "server/conn_context.h"
#include "wasi.h"
#include "wasm.h"
#include "wasmtime.h"

namespace dfly::wasm {
void EngineCall(std::string path, ConnectionContext* cntx);
}
