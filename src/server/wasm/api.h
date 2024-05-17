// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <initializer_list>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>

#include "base/logging.h"
#include "server/wasm/wasmtime.hh"

namespace dfly::wasm::api {

// TODO Make extensible or don't use it at all
template <typename Fn>
bool RegisterApiFunction(std::string_view name, Fn f, wasmtime::Linker* linker) {
  std::string module_name = "dragonfly";

  auto args_signature = wasmtime::FuncType({wasmtime::ValKind::I32}, {});

  auto res = linker->func_new(module_name, name, args_signature, f);
  return (bool)res;
}

}  // namespace dfly::wasm::api
