// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

namespace dragonfly {

/* PUBLIC FACING API */

extern "C" {
#define WASM_IMPORT(mod, name) __attribute__((import_module(#mod), import_name(#name)))

WASM_IMPORT(dragonfly, hello)
void hello();

// Add rest of functions here
}

/* Used to export functions from wasm modules */

// Use this macro if you want your function to be available within dragonfly,
// that is if you want to call `my_fun` from module `module.wasm` via `WASMCALL`
// then you have to register it first via DF_EXPORT("my_fun")
#define DF_EXPORT(name) __attribute__((export_name(name)))

}  // namespace dragonfly
