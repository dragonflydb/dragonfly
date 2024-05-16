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
  // call exported dragonfly function hello()
  hello();
}
