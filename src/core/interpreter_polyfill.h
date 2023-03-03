// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
// This header contains implementations of deprecated, removed or renamed lua functions.

#pragma once

extern "C" {
#include <lauxlib.h>
#include <lua.h>
#include <lualib.h>

// TODO: Fix checktab
#define aux_getn(L, n, w) (luaL_len(L, n))

LUA_API void lua_len(lua_State* L, int idx);

static int polyfill_table_getn(lua_State* L) {
  lua_len(L, 1);
  return 1;
}

static int polyfill_table_setn(lua_State* L) {
  // From Lua 5.1, ltablib.c
  luaL_checktype(L, 1, LUA_TTABLE);
  luaL_error(L, "setn is obsolete");
  lua_pushvalue(L, 1);
  return 1;
}

static int polyfill_table_foreach(lua_State* L) {
  // From Lua 5.1, ltablib.c
  luaL_checktype(L, 1, LUA_TTABLE);
  luaL_checktype(L, 2, LUA_TFUNCTION);
  lua_pushnil(L); /* first key */
  while (lua_next(L, 1)) {
    lua_pushvalue(L, 2);  /* function */
    lua_pushvalue(L, -3); /* key */
    lua_pushvalue(L, -3); /* value */
    lua_call(L, 2, 1);
    if (!lua_isnil(L, -1))
      return 1;
    lua_pop(L, 2); /* remove value and result */
  }
  return 0;
}

static int polyfill_table_foreachi(lua_State* L) {
  luaL_checktype(L, 1, LUA_TTABLE);  // Check type here because aux_getn is stripped
  // From Lua 5.1, ltablib.c
  int i;
  int n = aux_getn(L, 1, 0b11);
  luaL_checktype(L, 2, LUA_TFUNCTION);
  for (i = 1; i <= n; i++) {
    lua_pushvalue(L, 2);   /* function */
    lua_pushinteger(L, i); /* 1st argument */
    lua_rawgeti(L, 1, i);  /* 2nd argument */
    lua_call(L, 2, 1);
    if (!lua_isnil(L, -1))
      return 1;
    lua_pop(L, 1); /* remove nil result */
  }
  return 0;
}

static void register_polyfills(lua_State* lua) {
  lua_getglobal(lua, "table");

  // unpack was a global function until Lua 5.2
  lua_getfield(lua, -1, "unpack");
  lua_setglobal(lua, "unpack");

  // table.getn - removed, length operator # should be used instead
  lua_pushcfunction(lua, polyfill_table_getn);
  lua_setfield(lua, -2, "getn");

  // table.setn - removed, freely resizing a table is no longer possible
  lua_pushcfunction(lua, polyfill_table_setn);
  lua_setfield(lua, -2, "setn");

  // table.getn - removed, instead the length operator # should be used
  lua_pushcfunction(lua, polyfill_table_foreach);
  lua_setfield(lua, -2, "foreach");

  // table.forachi - removed, use for loops should be used instead
  lua_pushcfunction(lua, polyfill_table_foreachi);
  lua_setfield(lua, -2, "foreachi");

  lua_remove(lua, -1);
}
}
