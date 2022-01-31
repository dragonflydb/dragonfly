// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/interpreter.h"

#include <absl/strings/str_cat.h>
#include <openssl/sha.h>

#include <cstring>

extern "C" {
#include <lauxlib.h>
#include <lua.h>
#include <lualib.h>
}

#include "base/logging.h"

namespace dfly {
using namespace std;

namespace {

void RunSafe(lua_State* lua, string_view buf, const char* name) {
  CHECK_EQ(0, luaL_loadbuffer(lua, buf.data(), buf.size(), name));
  int err = lua_pcall(lua, 0, 0, 0);
  if (err) {
    const char* errstr = lua_tostring(lua, -1);
    LOG(FATAL) << "Error running " << name << " " << errstr;
  }
}

void Require(lua_State* lua, const char* name, lua_CFunction openf) {
  luaL_requiref(lua, name, openf, 1);
  lua_pop(lua, 1); /* remove lib */
}

void InitLua(lua_State* lua) {
  Require(lua, "", luaopen_base);
  Require(lua, LUA_TABLIBNAME, luaopen_table);
  Require(lua, LUA_STRLIBNAME, luaopen_string);
  Require(lua, LUA_MATHLIBNAME, luaopen_math);
  Require(lua, LUA_DBLIBNAME, luaopen_debug);

  /* Add a helper function we use for pcall error reporting.
   * Note that when the error is in the C function we want to report the
   * information about the caller, that's what makes sense from the point
   * of view of the user debugging a script. */
  {
    const char errh_func[] =
        "local dbg = debug\n"
        "function __redis__err__handler(err)\n"
        "  local i = dbg.getinfo(2,'nSl')\n"
        "  if i and i.what == 'C' then\n"
        "    i = dbg.getinfo(3,'nSl')\n"
        "  end\n"
        "  if i then\n"
        "    return i.source .. ':' .. i.currentline .. ': ' .. err\n"
        "  else\n"
        "    return err\n"
        "  end\n"
        "end\n";
    RunSafe(lua, errh_func, "@err_handler_def");
  }

  {
    const char code[] = R"(
local dbg=debug
local mt = {}

setmetatable(_G, mt)
mt.__newindex = function (t, n, v)
  if dbg.getinfo(2) then
    local w = dbg.getinfo(2, "S").what
    if w ~= "main" and w ~= "C" then
      error("Script attempted to create global variable '"..tostring(n).."'", 2)
    end
  end
  rawset(t, n, v)
end
mt.__index = function (t, n)
  if dbg.getinfo(2) and dbg.getinfo(2, "S").what ~= "C" then
    error("Script attempted to access nonexistent global variable '"..tostring(n).."'", 2)
  end
  return rawget(t, n)
end
debug = nil
)";
    RunSafe(lua, code, "@enable_strict_lua");
  }
}

void ToHex(const uint8_t* src, char* dest) {
  const char cset[] = "0123456789abcdef";
  for (size_t j = 0; j < 20; j++) {
    dest[j * 2] = cset[((src[j] & 0xF0) >> 4)];
    dest[j * 2 + 1] = cset[(src[j] & 0xF)];
  }
  dest[40] = '\0';
}

}  // namespace

Interpreter::Interpreter() {
  lua_ = luaL_newstate();
  InitLua(lua_);
}

Interpreter::~Interpreter() {
  lua_close(lua_);
}

void Interpreter::Fingerprint(string_view body, char* fp) {
  SHA_CTX ctx;
  uint8_t buf[20];

  SHA1_Init(&ctx);
  SHA1_Update(&ctx, body.data(), body.size());
  SHA1_Final(buf, &ctx);
  fp[0] = 'f';
  fp[1] = '_';
  ToHex(buf, fp + 2);
}

bool Interpreter::AddFunction(string_view body, string* result) {
  char funcname[43];
  Fingerprint(body, funcname);

  string script = absl::StrCat("function ", funcname, "() \n");
  absl::StrAppend(&script, body, "\nend");

  int res = luaL_loadbuffer(lua_, script.data(), script.size(), "@user_script");
  if (res == 0) {
    res = lua_pcall(lua_, 0, 0, 0);  // run func definition code
  }

  if (res) {
    result->assign(lua_tostring(lua_, -1));
    lua_pop(lua_, 1);  // Remove the error.

    return false;
  }

  result->assign(funcname);

  return true;
}

bool Interpreter::RunFunction(const char* f_id, std::string* error) {
  lua_getglobal(lua_, "__redis__err__handler");
  int type = lua_getglobal(lua_, f_id);
  if (type != LUA_TFUNCTION) {
    error->assign("function not found");  // TODO: noscripterr.
    lua_pop(lua_, 2);

    return false;
  }

  /* We have zero arguments and expect
   * a single return value. */
  int err = lua_pcall(lua_, 0, 1, -2);

  if (err) {
    *error = lua_tostring(lua_, -1);
  }
  return err == 0;
}

}  // namespace dfly
