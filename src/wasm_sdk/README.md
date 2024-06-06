# Public facing API for experimental Dragonfly wasm functions

This is the top folder for our public API. Each subfolder should serve as the
sdk for each language we support. For example:
`/python` subfolder would contain the declarations (but not the definitions! these will be
provided/exported by dragonfly and users wasm binaries will be linked against them
to resolve the symbols) for our API. A client function would be:

```
#include <dragonfly.h>

DF_EXPORT("my_fun")
void my_fun() {
  // work here
  return 1;
}
```

Loading the WASM binary in df is done via the flag `wasmpaths="path/to/mod1.wasm,path/to/mode2.wasm`

Calling an exported function is as simple as:

```
> CALL mode2.wasm my_fun
> 1
```
