#include <string>

#include "dragonfly.h"

// wasi-sdk-22.0/bin/clang++ -std=c++11 example.cc -o example.wasm
//
//  Launch Dragonfly via `./dragonfly --alsologtostderr --wasmpaths="/path/to/example.wasm"
//  And call it via redis-cli:
//  > CALLWASM example.wasm my_fun
//
// You can also export multiple functions per module

DF_EXPORT("my_fun")
void my_fun() {
  std::string result = dragonfly::hello_world();
  // passes
  assert(result == "Hello world from wasm!");
  // when we return the bson here we need to be carefull with the allocation.
  // does that mean the host will need to free the memory? e.g, export a `free` function?
}
