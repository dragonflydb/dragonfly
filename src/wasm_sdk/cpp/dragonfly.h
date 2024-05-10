// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

namespace dragonfly {

/* PUBLIC FACING API */

extern "C" {
#define WASM_IMPORT(mod, name) __attribute__((import_module(#mod), import_name(#name)))

WASM_IMPORT(dragonfly, hello)
void hello();
}

}  // namespace dragonfly
