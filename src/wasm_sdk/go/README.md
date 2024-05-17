# Dragonfly Go SDK for wasm

1. Download tiny go
2. Compile with wasi as target: `tinygo build -o go.wasm -target=wasi example.go`
3. Load the module at startup via `WASMPATH`
4. Call an exported function via `WASMCALL go.wasm go_hi`
