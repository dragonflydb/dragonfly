# Public facing API for experimental Dragonfly wasm functions

This is the top folder for our public API. Each subfolder should serve as the
sdk for each language we support. For example:
`/python` subfolder would contain the declarations (but not the definitions! these will be
provided/exported by dragonfly and users wasm binaries will be linked against them
to resolve the symbols) for our API. A client function would be:

```
#include <dragonfly/wasm.h>

int main() {
  dragonfly::RegisterUDF("my-udf", [](auto df) {
    auto val = df.get("foo")
    df.set(val * 15);
    return 1;
  });
}
```

and we could call it via:

```
> CALL my-udf
> 1
```
