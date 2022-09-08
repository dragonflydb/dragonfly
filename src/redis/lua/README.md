Since version 5.2 `luaL_register` is deprecated and removed. The new `luaL_newlib` function doesn't make the module globally available upon registration and is ment to be used with the `require` function.

To provide the modules globally, `luaL_newlib` is followed by a `lua_setglobal` for bit and struct.
