// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/wasm/wasm_registry.h"

#include <server/wasm/api.h>

#include <shared_mutex>

#include "absl/cleanup/cleanup.h"
#include "absl/strings/str_cat.h"
#include "base/logging.h"
#include "io/file_util.h"
#include "server/wasm/api.h"
#include "wasm.h"
#include "wasmtime.h"

namespace dfly::wasm {

WasmRegistry::WasmRegistry() {
  engine_ = wasm_engine_new();
  CHECK(engine_);
  linker_ = wasmtime_linker_new(engine_);
  CHECK(linker_);
  CHECK_EQ(wasmtime_linker_define_wasi(linker_), nullptr);

  // There are more configs we should look at. In particular we can spend a little more
  // time in compiling but generate a more optimized binary as well ass signal handling config.
  wasi_config_ = wasi_config_new();
  CHECK(wasi_config_);
  wasi_config_inherit_argv(wasi_config_);
  wasi_config_inherit_env(wasi_config_);
  wasi_config_inherit_stdin(wasi_config_);
  wasi_config_inherit_stdout(wasi_config_);
  wasi_config_inherit_stderr(wasi_config_);

  // Define API functions
  // This should really be done with a separate abstraction. Something line
  // WasmFunction::Register()
  std::string mod = "dragonfly";
  std::string export_f = "hello";
  wasm_valtype_vec_new_empty(&result_types_);
  wasm_valtype_vec_new_empty(&arg_types_);
  hello_handle_ = wasm_functype_new(&arg_types_, &result_types_);
  auto error =
      wasmtime_linker_define_func(linker_, mod.data(), mod.size(), export_f.data(), export_f.size(),
                                  hello_handle_, api::Hello, nullptr, nullptr);
  CHECK_EQ(error, nullptr);
}

WasmRegistry::~WasmRegistry() {
  wasi_config_delete(wasi_config_);
  wasmtime_linker_delete(linker_);
  wasm_engine_delete(engine_);
}

std::string WasmRegistry::Add(std::string_view path) {
  // 1. Read the wasm file in path
  auto is_file_read = io::ReadFileToString(path);
  if (!is_file_read) {
    return absl::StrCat("File error for path: ", path, " with error ",
                        is_file_read.error().message());
  }

  // 2. Setup && compile
  //  wasmtime_module_t* module = nullptr;
  //  wasm_byte_vec_t wasm;
  //  wasm_byte_vec_new_uninitialized(&wasm, is_file_read->size());
  //  std::memcpy(wasm.data, is_file_read->data(), wasm.size);

  // The following line is problematic, it causes a SEGFAULT when we switch fibers
  // after the command gets executed

  //  auto error = wasmtime_module_new(engine_, (uint8_t*)wasm.data, wasm.size, &module);
  //  if(error != nullptr || !module) {
  //    wasm_byte_vec_delete(&wasm);
  //    return "Could not compile WASM module";
  //  }
  //
  //
  //  // 3. Insert to registry
  //  auto slash = path.rfind('/');
  //  auto name = path;
  //  if(slash != path.npos) {
  //    name.substr(slash);
  //  }
  //  std::unique_lock<util::fb2::SharedMutex> lock(mu_);
  //  modules_.emplace(name, module);
  //  wasm_byte_vec_delete(&wasm);
  //  wasmtime_module_delete(module);
  return {};
}

bool WasmRegistry::Delete(std::string_view name) {
  std::unique_lock<util::fb2::SharedMutex> lock(mu_);
  return modules_.erase(name);
}

std::optional<WasmRegistry::WasmModuleInstance> WasmRegistry::GetInstanceFromModule(
    std::string_view module_name) {
  std::shared_lock<util::fb2::SharedMutex> lock(mu_);

  if (!modules_.contains(module_name)) {
    return {};
  }

  auto& module = modules_.at(module_name);

  wasmtime_store_t* store = wasmtime_store_new(engine_, nullptr, nullptr);
  wasmtime_context_t* context = wasmtime_store_context(store);
  if (!store) {
    return {};
  }
  auto error = wasmtime_context_set_wasi(context, wasi_config_);
  if (error != nullptr) {
    return {};
  }
  // instantiate the module
  error = wasmtime_linker_module(linker_, context, "", 0, module.GetImpl());
  if (error != nullptr) {
    return {};
  }

  // Create the function handler
  wasmtime_func_t func;
  error = wasmtime_linker_get_default(linker_, context, "", 0, &func);
  if (error != nullptr) {
    return {};
  }

  return WasmModuleInstance(store, context, func);
}

}  // namespace dfly::wasm
