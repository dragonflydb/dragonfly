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
uint8_t* call(uint32_t);

// Add rest of functions here
}

inline std::string deserialize(uint8_t* ptr);

inline std::string call(std::initializer_list<std::string> arguments) {
  std::string data(4, 'x');

  uint32_t parts = arguments.size();
  memcpy((void*)data.data(), &parts, 4);

  for (const std::string& str : arguments) {
    data.append(4, 'x');

    uint32_t strsize = str.size();
    memcpy((void*)(data.data() + data.size() - 4), &strsize, 4);

    data += str;
  }

  uint32_t ptr = (uint64_t)data.data();
  return deserialize(call(ptr));
}

/* Used to export functions from wasm modules */

// Use this macro if you want your function to be available within dragonfly,
// that is if you want to call `my_fun` from module `module.wasm` via `WASMCALL`
// then you have to register it first via DF_EXPORT("my_fun")
#define DF_EXPORT(name) __attribute__((export_name(name)))

/* Private and NOT part of the public API */
DF_EXPORT("allocate_on_guest_mem")
inline uint8_t* allocate_on_guest_mem(size_t bytes) {
  return new uint8_t[bytes];
}

/* Entry point to deserialize data coming from Dragonfly */
/* For now this is hardcoded and only returns a string and should */
/* be extended with json */
inline std::string deserialize(/*Get ownership*/ uint8_t* ptr) {
  // TODO Figure out how to reduce copies. This is two copies:
  // 1. Host allocates via allocate_on_guest_mem and copies data
  // 2. Data is deserialized on a new location
  uint32_t length;
  memcpy(&length, ptr, sizeof(length));

  std::string res(reinterpret_cast<char*>(ptr) + sizeof(length), length);
  delete[] ptr;
  return res;
}

}  // namespace dragonfly
