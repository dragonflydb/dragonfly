// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>

#include "absl/container/flat_hash_map.h"
#include "base/logging.h"
#include "util/fibers/synchronization.h"
#include "wasi.h"
#include "wasm.h"
#include "wasmtime.h"

namespace dfly::wasm {

template <typename T, auto fn> struct Deleter {
  void operator()(T* ptr) {
    fn(ptr);
  }
};

template <typename T, auto fn> using Handle = std::unique_ptr<T, Deleter<T, fn>>;

class WasmModule {
 public:
  WasmModule(wasmtime_module_t* module) {
    module_ = module;
  }

  WasmModule(WasmModule&&) = default;
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

class WasmRegistry {
 public:
  WasmRegistry();
  WasmRegistry(const WasmRegistry&) = delete;
  WasmRegistry(WasmRegistry&&) = delete;
  ~WasmRegistry();
  std::string Add(std::string_view path);
  bool Delete(std::string_view name);

  // Very light-weight. Each Module is compiled *once* but each UDF call, e,g, `CALLWASM`
  // will spawn its own instantiation of the wasm module. This is fine, because the former
  // is the expensive operation while the later is used as the context upon the function
  // will execute (effectively allowing concurrent calls to the same wasm module)
  class WasmModuleInstance {
   public:
    explicit WasmModuleInstance(wasmtime_store_t* store, wasmtime_context_t* cntx,
                                wasmtime_func_t wasm_fun)
        : store_{store}, context_{cntx}, wasm_fun_{wasm_fun} {
    }

    void operator()() {
      wasm_trap_t* trap = nullptr;
      auto error = wasmtime_func_call(context_, &wasm_fun_, nullptr, 0, nullptr, 0, &trap);
      if (error != nullptr || trap != nullptr) {
        LOG(INFO) << "error calling default export";
        return;
      }
    }

    ~WasmModuleInstance() {
      wasmtime_store_delete(store_);
    }

   private:
    // Full ownership
    wasmtime_store_t* store_;
    wasmtime_context_t* context_;
    wasmtime_func_t wasm_fun_;
  };

  std::optional<WasmModuleInstance> GetInstanceFromModule(std::string_view module_name);

 private:
  absl::flat_hash_map<std::string, WasmModule> modules_;
  mutable util::fb2::SharedMutex mu_;

  // Global available for all threads
  // see: https://docs.wasmtime.dev/c-api/wasmtime_8h.html in section thread safety
  wasm_engine_t* engine_;
  wasmtime_linker_t* linker_;
  wasi_config_t* wasi_config_;
  // TODO move this to an API registry since we will need multiple API functions
  wasm_valtype_vec_t arg_types_;
  wasm_valtype_vec_t result_types_;
  wasm_functype_t* hello_handle_;
};

}  // namespace dfly::wasm
