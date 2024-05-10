#include "dragonfly.h"

// Compile with wasi-sdk found here: https://github.com/WebAssembly/wasi-sdk.git
// wasi-sdk-22.0/bin/clang++ -std=c++11 example.cc -o udf.wasm
// then use redis-cli and call
// callwasm /path/to/udf.wasm

int main() {
  dragonfly::hello();
}
