// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <optional>
#include <string>
#include <string_view>

#include "base/expected.hpp"
#include "base/logging.h"
#include "wasmtime.h"

namespace dfly::wasm {

class WasmModule {
 public:
  WasmModule(wasmtime_module_t* module) {
    module_ = module;
  }

  WasmModule(WasmModule&&) = default;
  WasmModule& operator=(WasmModule&&) = default;
  WasmModule(const WasmModule&) = delete;

  ~WasmModule() {
    if (module_) {
      wasmtime_module_delete(module_);
    }
  }

  wasmtime_module_t* GetImpl() {
    return module_;
  }

 private:
  wasmtime_module_t* module_;
};

}  // namespace dfly::wasm
