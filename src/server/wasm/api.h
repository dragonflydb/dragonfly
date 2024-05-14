// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>

#include "base/logging.h"
#include "wasm.h"

namespace dfly::wasm::api {

template <typename... T> wasm_trap_t* Hello(T...) {
  LOG(INFO) << "HELLO FROM WASM";
  return nullptr;
}

}  // namespace dfly::wasm::api
