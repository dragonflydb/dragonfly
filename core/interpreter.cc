// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/interpreter.h"

#include <absl/strings/str_cat.h>
#include <openssl/sha.h>

#include <cstring>
#include <optional>

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

/*
 * Save the give pointer on Lua registry, used to save the Lua context and
 * function context so we can retrieve them from lua_State.
 */
void SaveOnRegistry(lua_State* lua, const char* name, void* ptr) {
  lua_pushstring(lua, name);
  if (ptr) {
    lua_pushlightuserdata(lua, ptr);
  } else {
    lua_pushnil(lua);
  }
  lua_settable(lua, LUA_REGISTRYINDEX);
}

/*
 * Get a saved pointer from registry
 */
void* GetFromRegistry(lua_State* lua, const char* name) {
  lua_pushstring(lua, name);
  lua_gettable(lua, LUA_REGISTRYINDEX);

  /* must be light user data */
  DCHECK(lua_islightuserdata(lua, -1));

  void* ptr = (void*)lua_topointer(lua, -1);
  DCHECK(ptr);

  /* pops the value */
  lua_pop(lua, 1);

  return ptr;
}

/* This function is used in order to push an error on the Lua stack in the
 * format used by redis.pcall to return errors, which is a lua table
 * with a single "err" field set to the error string. Note that this
 * table is never a valid reply by proper commands, since the returned
 * tables are otherwise always indexed by integers, never by strings. */
void PushError(lua_State* lua, const char* error) {
  lua_Debug dbg;

  lua_newtable(lua);
  lua_pushstring(lua, "err");

  /* Attempt to figure out where this function was called, if possible */
  if (lua_getstack(lua, 1, &dbg) && lua_getinfo(lua, "nSl", &dbg)) {
    string msg = absl::StrCat(dbg.source, ": ", dbg.currentline, ": ", error);
    lua_pushstring(lua, msg.c_str());
  } else {
    lua_pushstring(lua, error);
  }
  lua_settable(lua, -3);
}

/* In case the error set into the Lua stack by PushError() was generated
 * by the non-error-trapping version of redis.pcall(), which is redis.call(),
 * this function will raise the Lua error so that the execution of the
 * script will be halted. */
int RaiseError(lua_State* lua) {
  lua_pushstring(lua, "err");
  lua_gettable(lua, -2);
  return lua_error(lua);
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

  lua_pushnil(lua);
  lua_setglobal(lua, "loadfile");
  lua_pushnil(lua);
  lua_setglobal(lua, "dofile");
}

void ToHex(const uint8_t* src, char* dest) {
  const char cset[] = "0123456789abcdef";
  for (size_t j = 0; j < 20; j++) {
    dest[j * 2] = cset[((src[j] & 0xF0) >> 4)];
    dest[j * 2 + 1] = cset[(src[j] & 0xF)];
  }
  dest[40] = '\0';
}

string_view TopSv(lua_State* lua) {
  return string_view{lua_tostring(lua, -1), lua_rawlen(lua, -1)};
}

optional<int> FetchKey(lua_State* lua, const char* key) {
  lua_pushstring(lua, key);
  int type = lua_gettable(lua, -2);
  if (type == LUA_TNIL) {
    lua_pop(lua, 1);
    return nullopt;
  }
  return type;
}

int RedisSha1Command(lua_State* lua) {
  int argc = lua_gettop(lua);
  if (argc != 1) {
    lua_pushstring(lua, "wrong number of arguments");
    return lua_error(lua);
  }

  size_t len;
  const char* s = lua_tolstring(lua, 1, &len);

  SHA_CTX ctx;
  uint8_t buf[20];
  char digest[41];

  SHA1_Init(&ctx);
  SHA1_Update(&ctx, s, len);
  SHA1_Final(buf, &ctx);
  ToHex(buf, digest);

  lua_pushstring(lua, digest);
  return 1;
}

/* Returns a table with a single field 'field' set to the string value
 * passed as argument. This helper function is handy when returning
 * a Redis Protocol error or status reply from Lua:
 *
 * return redis.error_reply("ERR Some Error")
 * return redis.status_reply("ERR Some Error")
 */
int SingleFieldTable(lua_State* lua, const char* field) {
  if (lua_gettop(lua) != 1 || lua_type(lua, -1) != LUA_TSTRING) {
    PushError(lua, "wrong number or type of arguments");
    return 1;
  }

  lua_newtable(lua);
  lua_pushstring(lua, field);
  lua_pushvalue(lua, -3);
  lua_settable(lua, -3);
  return 1;
}

int RedisErrorReplyCommand(lua_State* lua) {
  return SingleFieldTable(lua, "err");
}

int RedisStatusReplyCommand(lua_State* lua) {
  return SingleFieldTable(lua, "ok");
}

const char* kInstanceKey = "_INSTANCE";

}  // namespace

Interpreter::Interpreter() {
  lua_ = luaL_newstate();
  InitLua(lua_);
  SaveOnRegistry(lua_, kInstanceKey, this);

  /* Register the redis commands table and fields */
  lua_newtable(lua_);

  /* redis.call */
  lua_pushstring(lua_, "call");
  lua_pushcfunction(lua_, RedisCallCommand);
  lua_settable(lua_, -3);

  /* redis.pcall */
  lua_pushstring(lua_, "pcall");
  lua_pushcfunction(lua_, RedisPCallCommand);
  lua_settable(lua_, -3);

  lua_pushstring(lua_, "sha1hex");
  lua_pushcfunction(lua_, RedisSha1Command);
  lua_settable(lua_, -3);

  /* redis.error_reply and redis.status_reply */
  lua_pushstring(lua_, "error_reply");
  lua_pushcfunction(lua_, RedisErrorReplyCommand);
  lua_settable(lua_, -3);
  lua_pushstring(lua_, "status_reply");
  lua_pushcfunction(lua_, RedisStatusReplyCommand);
  lua_settable(lua_, -3);

  /* Finally set the table as 'redis' global var. */
  lua_setglobal(lua_, "redis");
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

  return AddInternal(funcname, body, result);
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

bool Interpreter::Execute(string_view body, char f_id[43], string* error) {
  lua_getglobal(lua_, "__redis__err__handler");
  Fingerprint(body, f_id);

  int type = lua_getglobal(lua_, f_id);
  if (type != LUA_TFUNCTION) {
    lua_pop(lua_, 1);
    if (!AddInternal(f_id, body, error))
      return false;
    type = lua_getglobal(lua_, f_id);
    CHECK_EQ(type, LUA_TFUNCTION);
  }

  int err = lua_pcall(lua_, 0, 1, -2);
  if (err) {
    *error = lua_tostring(lua_, -1);
  }
  return err == 0;
}

bool Interpreter::AddInternal(const char* f_id, string_view body, string* result) {
  string script = absl::StrCat("function ", f_id, "() \n");
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

  result->assign(f_id);
  return true;
}

bool Interpreter::Serialize(ObjectExplorer* serializer, std::string* error) {
  // TODO: to get rid of this check or move it to the external function.
  // It does not make sense to do this check recursively and it complicates the flow
  // were in the middle of the serialization we could theoretically fail.
  if (!lua_checkstack(lua_, 4)) {
    /* Increase the Lua stack if needed to make sure there is enough room
     * to push 4 elements to the stack. On failure, return error.
     * Notice that we need, in the worst case, 4 elements because returning a map might
     * require push 4 elements to the Lua stack.*/
    error->assign("reached lua stack limit");
    lua_pop(lua_, 1); /* pop the element from the stack */
    return false;
  }

  int t = lua_type(lua_, -1);
  bool res = true;

  switch (t) {
    case LUA_TSTRING:
      serializer->OnString(TopSv(lua_));
      break;
    case LUA_TBOOLEAN:
      serializer->OnBool(lua_toboolean(lua_, -1));
      break;
    case LUA_TNUMBER:
      if (lua_isinteger(lua_, -1)) {
        serializer->OnInt(lua_tointeger(lua_, -1));
      } else {
        serializer->OnDouble(lua_tonumber(lua_, -1));
      }
      break;
    case LUA_TTABLE: {
      unsigned len = lua_rawlen(lua_, -1);
      if (len > 0) {  // array
        serializer->OnArrayStart(len);
        for (unsigned i = 0; i < len; ++i) {
          t = lua_rawgeti(lua_, -1, i + 1);  // push table element

          // TODO: we should make sure that we have enough stack space
          // to traverse each object. This can be done as a dry-run before doing real serialization.
          // Once we are sure we are safe we can simplify the serialization flow and
          // remove the error factor.
          CHECK(Serialize(serializer, error));  // pops the element
        }
        serializer->OnArrayEnd();
      } else {
        auto fres = FetchKey(lua_, "err");
        if (fres && *fres == LUA_TSTRING) {
          serializer->OnError(TopSv(lua_));
          lua_pop(lua_, 1);
          break;
        }

        fres = FetchKey(lua_, "ok");
        if (fres && *fres == LUA_TSTRING) {
          serializer->OnStatus(TopSv(lua_));
          lua_pop(lua_, 1);
          break;
        }
        serializer->OnError("TBD");
      }
      break;
    }
    case LUA_TNIL:
      serializer->OnNil();
      break;
    default:
      error->assign(absl::StrCat("Unsupported type ", t));
  }

  lua_pop(lua_, 1);
  return res;
}

// Returns number of results, which is always 1 in this case.
// Please note that lua resets the stack once the function returns so no need
// to unwind the stack manually in the function (though lua allows doing this).
int Interpreter::RedisGenericCommand(bool raise_error) {
  /* By using Lua debug hooks it is possible to trigger a recursive call
   * to luaRedisGenericCommand(), which normally should never happen.
   * To make this function reentrant is futile and makes it slower, but
   * we should at least detect such a misuse, and abort. */
  if (cmd_depth_) {
    const char* recursion_warning =
        "luaRedisGenericCommand() recursive call detected. "
        "Are you doing funny stuff with Lua debug hooks?";
    PushError(lua_, recursion_warning);
    return 1;
  }

  cmd_depth_++;
  int argc = lua_gettop(lua_);

  /* Require at least one argument */
  if (argc == 0) {
    PushError(lua_, "Please specify at least one argument for redis.call()");
    cmd_depth_--;

    return raise_error ? RaiseError(lua_) : 1;
  }

  // TODO: to prepare arguments.

  /* Pop all arguments from the stack, we do not need them anymore
   * and this way we guaranty we will have room on the stack for the result. */
  lua_pop(lua_, argc);

  cmd_depth_--;
  lua_pushinteger(lua_, 42);

  return 1;
}

int Interpreter::RedisCallCommand(lua_State* lua) {
  void* me = GetFromRegistry(lua, kInstanceKey);
  return reinterpret_cast<Interpreter*>(me)->RedisGenericCommand(true);
}

int Interpreter::RedisPCallCommand(lua_State* lua) {
  void* me = GetFromRegistry(lua, kInstanceKey);
  return reinterpret_cast<Interpreter*>(me)->RedisGenericCommand(false);
}

}  // namespace dfly
