// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/wasm/wasm_registry.h"

#include <server/wasm/api.h>

#include <memory>
#include <shared_mutex>
#include <variant>

#include "absl/cleanup/cleanup.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "base/flags.h"
#include "base/logging.h"
#include "facade/facade_types.h"
#include "facade/reply_capture.h"
#include "facade/reply_formats.h"
#include "facade/service_interface.h"
#include "io/file_util.h"
#include "server/conn_context.h"
#include "server/wasm/api.h"
#include "server/wasm/wasmtime.hh"

ABSL_DECLARE_FLAG(std::string, wasmpaths);

namespace dfly::wasm {

namespace {

std::vector<std::string> ParseArguments(uint8_t* data) {
  uint32_t parts;
  memcpy(&parts, data, sizeof(uint32_t));
  data += sizeof(uint32_t);

  std::vector<std::string> out;
  for (int32_t i = 0; i < parts; i++) {
    uint32_t length;
    memcpy(&length, data, sizeof(uint32_t));
    data += sizeof(uint32_t);

    out.emplace_back(reinterpret_cast<char*>(data), length);
    data += length;
  }

  return out;
}

}  // namespace

WasmRegistry::WasmRegistry(facade::ServiceInterface& service)
    : engine_(WasmRegistry::GetConfig()), linker_(engine_), store_(engine_) {
  auto callfunc = [&service](wasmtime::Caller caller, wasmtime::Span<const wasmtime::Val> params,
                             auto results) {
    auto res = caller.get_export("allocate_on_guest_mem");

    wasmtime::Memory memory = std::get<wasmtime::Memory>(*caller.get_export("memory"));

    facade::CapturingReplyBuilder capture;
    ConnectionContext cntx(nullptr, nullptr);
    delete cntx.Inject(&capture);

    auto arguments = ParseArguments(memory.data(caller.context()).data() + params[0].i32());
    facade::CmdArgVec arguments_span;
    for (auto& str : arguments)
      arguments_span.emplace_back(str);

    service.DispatchCommand(absl::MakeSpan(arguments_span), &cntx);
    cntx.Inject(nullptr);

    std::string value = facade::FormatToJson(capture.Take());

    // Call the exported alloc to allocate memory on the guest
    auto alloc = std::get<wasmtime::Func>(*res);
    const int32_t alloc_size = static_cast<int32_t>(value.size() + sizeof(uint32_t));
    auto result = alloc.call(caller.context(), {wasmtime::Val{alloc_size}});
    if (!result) {
      // handle errors
    }
    auto wasm_value = result.ok().front();
    auto offset = wasm_value.i32();

    uint8_t* data = memory.data(caller.context()).data() + offset;
    absl::little_endian::Store32(data, value.size());
    memcpy(data + sizeof(uint32_t), value.c_str(), value.size());

    results[0] = wasm_value;
    return std::monostate();
  };

  api::RegisterApiFunction("call", callfunc, &linker_);

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
