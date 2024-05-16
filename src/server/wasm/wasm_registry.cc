// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/wasm/wasm_registry.h"

#include <server/wasm/api.h>

#include <shared_mutex>
#include <variant>

#include "absl/cleanup/cleanup.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "base/flags.h"
#include "base/logging.h"
#include "io/file_util.h"
#include "server/wasm/api.h"
#include "server/wasm/wasmtime.hh"

ABSL_DECLARE_FLAG(std::string, wasmpaths);

namespace dfly::wasm {

WasmRegistry::WasmRegistry()
    : engine_(WasmRegistry::GetConfig()), linker_(engine_), store_(engine_) {
  auto hellofunc = [](wasmtime::Caller caller, auto params, auto results) {
    auto res = caller.get_export("allocate_on_guest_mem");
    std::string value = "Hello world from wasm!";
    value.push_back('\0');

    // Call the exported alloc to allocate memory on the guest
    auto alloc = std::get<wasmtime::Func>(*res);
    const int32_t alloc_size = static_cast<int32_t>(value.size());
    auto result = alloc.call(caller.context(), {wasmtime::Val{alloc_size}});
    if (!result) {
      // handle errors
    }
    auto wasm_value = result.ok().front();
    auto offset = wasm_value.i32();

    wasmtime::Memory memory = std::get<wasmtime::Memory>(*caller.get_export("memory"));

    uint8_t* data = memory.data(caller.context()).data() + offset;
    absl::little_endian::Store32(data, value.size());
    // TODO inject payload size at the front so we dont have to push an extra \0
    memcpy(data, value.c_str(), value.size());

    results[0] = wasm_value;
    return std::monostate();
  };

  api::RegisterApiFunction("hello", hellofunc, &linker_);

  wasmtime::WasiConfig wasi;
  wasi.inherit_argv();
  wasi.inherit_env();
  wasi.inherit_stdin();
  wasi.inherit_stdout();
  wasi.inherit_stderr();
  store_.context().set_wasi(std::move(wasi)).unwrap();

  linker_.define_wasi().unwrap();

  InstantiateAndLinkModules();
}

WasmRegistry::~WasmRegistry() {
}

void WasmRegistry::InstantiateAndLinkModules() {
  auto wasm_modules = absl::GetFlag(FLAGS_wasmpaths);
  std::vector<std::string_view> modules = absl::StrSplit(wasm_modules, ",");
  for (auto mod_path : modules) {
    Add(mod_path);
  }
}

std::string WasmRegistry::Add(std::string_view path) {
  // 1. Read the wasm file in path
  auto is_file_read = io::ReadFileToString(path);
  if (!is_file_read) {
    LOG(ERROR) << "File error for path: " << path << " with error "
               << is_file_read.error().message();
    exit(1);
  }

  // In this context the cast is safe
  wasmtime::Span<uint8_t> wasm_bin{reinterpret_cast<uint8_t*>(is_file_read->data()),
                                   is_file_read->size()};

  // 2. Setup && compile
  auto result = wasmtime::Module::compile(engine_, wasm_bin);
  if (!result) {
    LOG(ERROR) << "Error compiling file: " << path << " with error: " << result.err().message();
    exit(1);
  }

  // 3. Insert to registry
  auto slash = path.rfind('/');
  auto name = path;
  if (slash != path.npos) {
    name = name.substr(slash + 1);
  }
  modules_.emplace(name, std::move(result.ok()));

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

  auto instance = linker_.instantiate(store_, module.GetImpl());
  if (!instance) {
    LOG(INFO) << instance.err().message();
    return {};
  }

  return WasmModuleInstance{instance.ok(), &store_};
}

}  // namespace dfly::wasm
