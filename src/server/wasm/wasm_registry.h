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
#include "server/wasm/api.h"
#include "server/wasm/wasmtime.hh"
#include "util/fibers/synchronization.h"

namespace dfly::wasm {

class WasmModule {
 public:
  explicit WasmModule(wasmtime::Module module) : module_(std::move(module)) {
  }

  WasmModule(WasmModule&&) = default;
  WasmModule& operator=(WasmModule&&) = default;
  WasmModule(const WasmModule&) = delete;
  ~WasmModule() = default;

  wasmtime::Module& GetImpl() {
    return module_;
  }

 private:
  wasmtime::Module module_;
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
    explicit WasmModuleInstance(wasmtime::Instance instance, wasmtime::Store* store)
        : instance_{instance}, store_(store) {
    }

    void operator()() {
      // Users will export functions for their modules via the attribute
      //  __attribute__((export_name(func_name))). We will expose this in our sdk
      auto extern_def = instance_.get(*store_, "my_fun");
      if (!extern_def) {
        // return error
        return;
      }
      auto run = std::get<wasmtime::Func>(*extern_def);
      run.call(store_, {}).unwrap();
    }

   private:
    wasmtime::Instance instance_;
    wasmtime::Store* store_;
  };

  std::optional<WasmModuleInstance> GetInstanceFromModule(std::string_view module_name);

 private:
  absl::flat_hash_map<std::string, WasmModule> modules_;
  mutable util::fb2::SharedMutex mu_;

  // Global available for all threads
  // see: https://docs.wasmtime.dev/c-api/wasmtime_8h.html in section thread safety
  wasmtime::Engine engine_;
  wasmtime::Linker linker_;
  wasmtime::Store store_;
  static wasmtime::Config GetConfig() {
    wasmtime::Config config;
    config.epoch_interruption(false);
    return config;
  }
};

}  // namespace dfly::wasm
