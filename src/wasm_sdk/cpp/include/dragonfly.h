// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <initializer_list>
#include <memory>
#include <string>

namespace dragonfly {

/* PUBLIC FACING API */

extern "C" {
#define WASM_IMPORT(mod, name) __attribute__((import_module(#mod), import_name(#name)))

WASM_IMPORT(dragonfly, call)
void call(uint32_t);

// Add rest of functions here
}

// guard against multiple defines
std::string call_buffer;

inline std::string_view call(std::initializer_list<std::string> arguments) {
  std::string data(4, 'x');

  uint32_t parts = arguments.size();
  memcpy((void*)data.data(), &parts, 4);

  for (const std::string& str : arguments) {
    data.append(4, 'x');

    uint32_t strsize = str.size();
    memcpy((void*)(data.data() + data.size() - 4), &strsize, 4);

    data += str;
  }

  call((uint64_t)data.data());
  return call_buffer;
}

/* Used to export functions from wasm modules */

// Use this macro if you want your function to be available within dragonfly,
// that is if you want to call `my_fun` from module `module.wasm` via `WASMCALL`
// then you have to register it first via DF_EXPORT("my_fun")
#define DF_EXPORT(name) __attribute__((export_name(name)))

/* Private and NOT part of the public API */
DF_EXPORT("provide_buffer")
inline char* provide_buffer(int32_t bytes) {
  call_buffer.resize(bytes);
  return call_buffer.data();
}

}  // namespace dragonfly
