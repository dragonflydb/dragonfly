# Dragonfly C++ SDK for wasm

1. Download the wasi-sdk found in https://github.com/WebAssembly/wasi-sdk.git
2. Compile your source files with wasi-sdk clang: `wasi-sdk-22.0/bin/clang++`
3. Load the module at startup via `WASMPATH`
4. Call an exported function via `WASMCALL module.wasm function_name`
