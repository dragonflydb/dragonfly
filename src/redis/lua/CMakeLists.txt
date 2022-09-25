add_library(lua_modules STATIC
    cjson/fpconv.c cjson/strbuf.c cjson/lua_cjson.c
    cmsgpack/lua_cmsgpack.c
    struct/lua_struct.c
    bit/bit.c
)

target_compile_options(lua_modules PRIVATE
    -Wno-sign-compare -Wno-misleading-indentation -Wno-implicit-fallthrough -Wno-undefined-inline
    -Wno-stringop-overflow)

target_link_libraries(lua_modules TRDP::lua)
