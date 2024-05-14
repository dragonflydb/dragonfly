// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/wasm/wasm_module.h"

namespace dfly::wasm {

#include <assert.h>

#include <string>

#include "absl/cleanup/cleanup.h"
#include "base/logging.h"
#include "server/wasm/engine_example.h"
#include "wasm.h"

namespace dfly::wasm {
// Name of the module: dragonfly:
// exported function hello()
thread_local ConnectionContext* local_cntx;

void hello() {
  local_cntx->SendSimpleString("Hello from WASM engine!");
}

ModuleOrError Create(std::string_view path) {
  // Set up our context
  wasm_engine_t* engine = wasm_engine_new();
  CHECK(engine != nullptr);
  wasmtime_store_t* store = wasmtime_store_new(engine, nullptr, nullptr);
  CHECK(store != nullptr);
  wasmtime_context_t* context = wasmtime_store_context(store);

  // Create a linker with WASI functions defined
  wasmtime_linker_t* linker = wasmtime_linker_new(engine);
  wasmtime_error_t* error = wasmtime_linker_define_wasi(linker);
  if (error != nullptr)
    LOG(INFO) << "failed to link wasi";

  // HACK, for some reason if we capture the lambda changes type and it gets rejected by
  // wasm linker function
  local_cntx = cntx;
  auto hello_wrapper = [](auto...) -> wasm_trap_t* {
    hello();
    return nullptr;
  };

  std::string mod = "dragonfly";
  std::string export_f = "hello";
  wasm_valtype_vec_t arg_types;
  wasm_valtype_vec_t result_types;
  wasm_valtype_vec_new_empty(&result_types);
  wasm_valtype_vec_new_empty(&arg_types);
  auto* handle = wasm_functype_new(&arg_types, &result_types);
  error = wasmtime_linker_define_func(linker, mod.data(), mod.size(), export_f.data(),
                                      export_f.size(), handle, hello_wrapper, nullptr, nullptr);

  // Compile our modules
  wasmtime_module_t* module = nullptr;

  absl::Cleanup clean([&]() {
    // Clean up after ourselves at this point
    if (handle)
      wasm_functype_delete(handle);
    if (module)
      wasmtime_module_delete(module);
    if (store)
      wasmtime_store_delete(store);
    if (engine)
      wasm_engine_delete(engine);
  });

  if (error != nullptr) {
    LOG(INFO) << "failed to link wasi";
    return;
  }

  wasm_byte_vec_t wasm;

  // Load our input file to parse it next
  FILE* file = fopen(path.data(), "rb");
  if (!file) {
    LOG(INFO) << "error opening file";
    return;
  }
  fseek(file, 0L, SEEK_END);
  size_t file_size = ftell(file);
  wasm_byte_vec_new_uninitialized(&wasm, file_size);
  fseek(file, 0L, SEEK_SET);
  if (fread(wasm.data, file_size, 1, file) != 1) {
    printf("> Error loading module!\n");
    exit(1);
  }
  fclose(file);

  error = wasmtime_module_new(engine, (uint8_t*)wasm.data, wasm.size, &module);
  if (!module) {
    LOG(INFO) << "failed to compile module";
    return;
  }
  wasm_byte_vec_delete(&wasm);

  // Instantiate wasi
  wasi_config_t* wasi_config = wasi_config_new();
  assert(wasi_config);
  wasi_config_inherit_argv(wasi_config);
  wasi_config_inherit_env(wasi_config);
  wasi_config_inherit_stdin(wasi_config);
  wasi_config_inherit_stdout(wasi_config);
  wasi_config_inherit_stderr(wasi_config);
  wasm_trap_t* trap = nullptr;
  error = wasmtime_context_set_wasi(context, wasi_config);
  if (error != nullptr) {
    LOG(INFO) << "failed to instantiate WASI";
    return;
  }

  // Instantiate the module
  error = wasmtime_linker_module(linker, context, "", 0, module);
  if (error != nullptr) {
    LOG(INFO) << "failed to link";
    return;
  }

  // Run it.
  wasmtime_func_t func;
  error = wasmtime_linker_get_default(linker, context, "", 0, &func);
  if (error != nullptr) {
    LOG(INFO) << "failed to locate default export for module";
    return;
  }

  error = wasmtime_func_call(context, &func, nullptr, 0, nullptr, 0, &trap);
  if (error != nullptr || trap != nullptr) {
    LOG(INFO) << "error calling default export";
    return;
  }
}

}
