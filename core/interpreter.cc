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

#include <absl/strings/str_format.h>

#include "base/logging.h"

namespace dfly {
using namespace std;

namespace {

class RedisTranslator : public ObjectExplorer {
 public:
  RedisTranslator(lua_State* lua) : lua_(lua) {
  }
  void OnBool(bool b) final;
  void OnString(std::string_view str) final;
  void OnDouble(double d) final;
  void OnInt(int64_t val) final;
  void OnArrayStart(unsigned len) final;
  void OnArrayEnd() final;
  void OnNil() final;
  void OnStatus(std::string_view str) final;
  void OnError(std::string_view str) final;

 private:
  void ArrayPre() {
  }

  void ArrayPost() {
    if (!array_index_.empty()) {
      lua_rawseti(lua_, -2, array_index_.back()++); /* set table at key `i' */
    }
  }

  vector<unsigned> array_index_;
  lua_State* lua_;
};

void RedisTranslator::OnBool(bool b) {
  CHECK(!b) << "Only false (nil) supported";
  ArrayPre();
  lua_pushboolean(lua_, 0);
  ArrayPost();
}

void RedisTranslator::OnString(std::string_view str) {
  ArrayPre();
  lua_pushlstring(lua_, str.data(), str.size());
  ArrayPost();
}

// Doubles are not supported by Redis, however we can support them.
// Here is the use-case:
// local foo = redis.call('zscore', 'myzset', 'one')
// assert(type(foo) == "number")
void RedisTranslator::OnDouble(double d) {
  ArrayPre();
  lua_pushnumber(lua_, d);
  ArrayPost();
}

void RedisTranslator::OnInt(int64_t val) {
  ArrayPre();
  lua_pushinteger(lua_, val);
  ArrayPost();
}

void RedisTranslator::OnNil() {
  ArrayPre();
  lua_pushboolean(lua_, 0);
  ArrayPost();
}

void RedisTranslator::OnStatus(std::string_view str) {
  CHECK(array_index_.empty()) << "unexpected status";
  lua_newtable(lua_);
  lua_pushstring(lua_, "ok");
  lua_pushlstring(lua_, str.data(), str.size());
  lua_settable(lua_, -3);
}

void RedisTranslator::OnError(std::string_view str) {
  CHECK(array_index_.empty()) << "unexpected error";
  lua_newtable(lua_);
  lua_pushstring(lua_, "err");
  lua_pushlstring(lua_, str.data(), str.size());
  lua_settable(lua_, -3);
}

void RedisTranslator::OnArrayStart(unsigned len) {
  ArrayPre();
  lua_newtable(lua_);
  array_index_.push_back(1);
}

void RedisTranslator::OnArrayEnd() {
  CHECK(!array_index_.empty());
  DCHECK(lua_istable(lua_, -1));

  array_index_.pop_back();
  ArrayPost();
}

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

void SetGlobalArrayInternal(lua_State* lua, const char* name, Interpreter::MutSliceSpan args) {
  lua_newtable(lua);
  for (size_t j = 0; j < args.size(); j++) {
    lua_pushlstring(lua, args[j].data(), args[j].size());
    lua_rawseti(lua, -2, j + 1);
  }
  lua_setglobal(lua, name);
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

// const char* kInstanceKey = "_INSTANCE";

}  // namespace

Interpreter::Interpreter() {
  lua_ = luaL_newstate();
  InitLua(lua_);
  void** ptr = static_cast<void**>(lua_getextraspace(lua_));
  *ptr = this;
  // SaveOnRegistry(lua_, kInstanceKey, this);

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
  CHECK(lua_checkstack(lua_, 64));
}

Interpreter::~Interpreter() {
  lua_close(lua_);
}

void Interpreter::FuncSha1(string_view body, char* fp) {
  SHA_CTX ctx;
  uint8_t buf[20];

  SHA1_Init(&ctx);
  SHA1_Update(&ctx, body.data(), body.size());
  SHA1_Final(buf, &ctx);
  ToHex(buf, fp);
}

auto Interpreter::AddFunction(string_view body, string* result) -> AddResult {
  char funcname[43];
  funcname[0] = 'f';
  funcname[1] = '_';
  FuncSha1(body, funcname + 2);
  funcname[42] = '\0';

  int type = lua_getglobal(lua_, funcname);
  lua_pop(lua_, 1);

  if (type == LUA_TNIL && !AddInternal(funcname, body, result))
    return COMPILE_ERR;

  result->assign(funcname + 2);

  return type == LUA_TNIL ? ADD_OK : ALREADY_EXISTS;
}

bool Interpreter::Exists(string_view sha) const {
  if (sha.size() != 40)
    return false;

  char fname[43];
  fname[0] = 'f';
  fname[1] = '_';
  fname[42] = '\0';
  memcpy(fname + 2, sha.data(), 40);

  int type = lua_getglobal(lua_, fname);
  lua_pop(lua_, 1);

  return type == LUA_TFUNCTION;
}

auto Interpreter::RunFunction(string_view sha, std::string* error) -> RunResult {
  DCHECK_EQ(40u, sha.size());

  lua_getglobal(lua_, "__redis__err__handler");
  char fname[43];
  fname[0] = 'f';
  fname[1] = '_';
  memcpy(fname + 2, sha.data(), 40);
  fname[42] = '\0';

  int type = lua_getglobal(lua_, fname);
  if (type != LUA_TFUNCTION) {
    lua_pop(lua_, 2);

    return NOT_EXISTS;
  }

  /* We have zero arguments and expect
   * a single return value. */
  int err = lua_pcall(lua_, 0, 1, -2);

  if (err) {
    *error = lua_tostring(lua_, -1);
  }

  return err == 0 ? RUN_OK : RUN_ERR;
}

void Interpreter::SetGlobalArray(const char* name, MutSliceSpan args) {
  SetGlobalArrayInternal(lua_, name, args);
}

bool Interpreter::IsResultSafe() const {
  int top = lua_gettop(lua_);
  if (top >= 128)
    return false;

  int t = lua_type(lua_, -1);
  if (t != LUA_TTABLE)
    return true;

  bool res = IsTableSafe();
  lua_settop(lua_, top);
  return res;
}

bool Interpreter::AddInternal(const char* f_id, string_view body, string* error) {
  string script = absl::StrCat("function ", f_id, "() \n");
  absl::StrAppend(&script, body, "\nend");

  int res = luaL_loadbuffer(lua_, script.data(), script.size(), "@user_script");
  if (res == 0) {
    res = lua_pcall(lua_, 0, 0, 0);  // run func definition code
  }

  if (res) {
    error->assign(lua_tostring(lua_, -1));
    lua_pop(lua_, 1);  // Remove the error.

    return false;
  }

  return true;
}

bool Interpreter::IsTableSafe() const {
  auto fres = FetchKey(lua_, "err");
  if (fres && *fres == LUA_TSTRING) {
    return true;
  }

  fres = FetchKey(lua_, "ok");
  if (fres && *fres == LUA_TSTRING) {
    return true;
  }

  vector<pair<unsigned, unsigned>> lens;
  unsigned len = lua_rawlen(lua_, -1);
  unsigned i = 0;

  // implement dfs traversal
  while (true) {
    while (i < len) {
      DVLOG(1) << "Stack " << lua_gettop(lua_) << "/" << i << "/" << len;
      int t = lua_rawgeti(lua_, -1, i + 1);  // push table element
      if (t == LUA_TTABLE) {
        if (lens.size() >= 127)  // reached depth 128
          return false;

        CHECK(lua_checkstack(lua_, 1));
        lens.emplace_back(i + 1, len);  // save the parent state.

        // reset to iterate on the next table.
        i = 0;
        len = lua_rawlen(lua_, -1);
      } else {
        lua_pop(lua_, 1); // pop table element
        ++i;
      }
    }

    if (lens.empty())  // exit criteria
      break;

    // unwind to the state before we went down the stack.
    tie(i, len) = lens.back();
    lens.pop_back();

    lua_pop(lua_, 1);
  };
  DCHECK_EQ(1, lua_gettop(lua_));

  return true;
}

void Interpreter::SerializeResult(ObjectExplorer* serializer) {
  int t = lua_type(lua_, -1);

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

      unsigned len = lua_rawlen(lua_, -1);
      serializer->OnArrayStart(len);
      for (unsigned i = 0; i < len; ++i) {
        t = lua_rawgeti(lua_, -1, i + 1);  // push table element

        // TODO: we should make sure that we have enough stack space
        // to traverse each object. This can be done as a dry-run before doing real serialization.
        // Once we are sure we are safe we can simplify the serialization flow and
        // remove the error factor.
        SerializeResult(serializer);  // pops the element
      }
      serializer->OnArrayEnd();
      break;
    }
    case LUA_TNIL:
      serializer->OnNil();
      break;
    default:
      LOG(ERROR) << "Unsupported type " << lua_typename(lua_, t);
      serializer->OnNil();
  }

  lua_pop(lua_, 1);
}

void Interpreter::ResetStack() {
  lua_settop(lua_, 0);
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

  if (!redis_func_) {
    PushError(lua_, "internal error - redis function not defined");
    return raise_error ? RaiseError(lua_) : 1;
  }

  cmd_depth_++;
  int argc = lua_gettop(lua_);

  /* Require at least one argument */
  if (argc == 0) {
    PushError(lua_, "Please specify at least one argument for redis.call()");
    cmd_depth_--;

    return raise_error ? RaiseError(lua_) : 1;
  }

  size_t blob_len = 0;
  char tmpbuf[64];

  for (int j = 0; j < argc; j++) {
    unsigned idx = j + 1;
    if (lua_isinteger(lua_, idx)) {
      absl::AlphaNum an(lua_tointeger(lua_, idx));
      blob_len += an.size();
    } else if (lua_isnumber(lua_, idx)) {
      // fmt_len does not include '\0'.
      int fmt_len = absl::SNPrintF(tmpbuf, sizeof(tmpbuf), "%.17g", lua_tonumber(lua_, idx));
      CHECK_GT(fmt_len, 0);
      blob_len += fmt_len;
    } else if (lua_isstring(lua_, idx)) {
      blob_len += lua_rawlen(lua_, idx);  // lua_rawlen does not include '\0'.
    } else {
      PushError(lua_, "Lua redis() command arguments must be strings or integers");
      cmd_depth_--;
      return raise_error ? RaiseError(lua_) : 1;
    }
  }

  // backing storage.
  unique_ptr<char[]> blob(new char[blob_len + 8]);  // 8 safety.
  vector<absl::Span<char>> cmdargs;
  char* cur = blob.get();
  char* end = cur + blob_len;

  for (int j = 0; j < argc; j++) {
    unsigned idx = j + 1;
    size_t len = 0;
    if (lua_isinteger(lua_, idx)) {
      char* next = absl::numbers_internal::FastIntToBuffer(lua_tointeger(lua_, idx), cur);
      len = next - cur;
    } else if (lua_isnumber(lua_, idx)) {
      int fmt_len = absl::SNPrintF(cur, end - cur, "%.17g", lua_tonumber(lua_, idx));
      CHECK_GT(fmt_len, 0);
      len = fmt_len;
    } else if (lua_isstring(lua_, idx)) {
      len = lua_rawlen(lua_, idx);
      memcpy(cur, lua_tostring(lua_, idx), len);  // copy \0 as well.
    }

    cmdargs.emplace_back(cur, len);
    cur += len;
  }

  /* Pop all arguments from the stack, we do not need them anymore
   * and this way we guaranty we will have room on the stack for the result. */
  lua_pop(lua_, argc);
  RedisTranslator translator(lua_);
  redis_func_(MutSliceSpan{cmdargs}, &translator);
  DCHECK_EQ(1, lua_gettop(lua_));

  cmd_depth_--;

  return 1;
}

int Interpreter::RedisCallCommand(lua_State* lua) {
  void** ptr = static_cast<void**>(lua_getextraspace(lua));
  return reinterpret_cast<Interpreter*>(*ptr)->RedisGenericCommand(true);
}

int Interpreter::RedisPCallCommand(lua_State* lua) {
  void** ptr = static_cast<void**>(lua_getextraspace(lua));
  return reinterpret_cast<Interpreter*>(*ptr)->RedisGenericCommand(false);
}

}  // namespace dfly
