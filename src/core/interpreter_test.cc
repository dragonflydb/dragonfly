// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/interpreter.h"

extern "C" {
#include <lauxlib.h>
#include <lua.h>
}

#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>
#include <gmock/gmock.h>

#include "base/gtest.h"
#include "base/logging.h"

namespace dfly {
using namespace std;

class TestSerializer : public ObjectExplorer {
 public:
  string res;

  void OnBool(bool b) final {
    absl::StrAppend(&res, "bool(", b, ") ");
  }

  void OnString(std::string_view str) final {
    absl::StrAppend(&res, "str(", str, ") ");
  }

  void OnDouble(double d) final {
    absl::StrAppend(&res, "d(", d, ") ");
  }

  void OnInt(int64_t val) final {
    absl::StrAppend(&res, "i(", val, ") ");
  }

  void OnArrayStart(unsigned len) final {
    absl::StrAppend(&res, "[");
  }

  void OnArrayEnd() final {
    if (res.back() == ' ')
      res.pop_back();

    absl::StrAppend(&res, "] ");
  }

  void OnNil() final {
    absl::StrAppend(&res, "nil ");
  }

  void OnMapStart(unsigned len) final {
    absl::StrAppend(&res, "{");
  }

  void OnMapEnd() final {
    if (res.back() == ' ')
      res.pop_back();
    absl::StrAppend(&res, "} ");
  }

  void OnStatus(std::string_view str) {
    absl::StrAppend(&res, "status(", str, ") ");
  }

  void OnError(std::string_view str) {
    absl::StrAppend(&res, "err(", str, ") ");
  }
};

using SliceSpan = Interpreter::SliceSpan;
class InterpreterTest : public ::testing::Test {
 protected:
  InterpreterTest() {
  }

  lua_State* lua() {
    return intptr_.lua();
  }

  void RunInline(string_view buf, const char* name, unsigned num_results = 0) {
    CHECK_EQ(0, luaL_loadbuffer(lua(), buf.data(), buf.size(), name));
    CHECK_EQ(0, lua_pcall(lua(), 0, num_results, 0));
  }

  void SetGlobalArray(const char* name, const vector<string_view>& vec);

  // returns true if script run successfully.
  bool Execute(string_view script);

  Interpreter intptr_;
  TestSerializer ser_;
  string error_;
  vector<unique_ptr<string>> strings_;
};

void InterpreterTest::SetGlobalArray(const char* name, const vector<string_view>& vec) {
  vector<string_view> slices(vec.size());
  for (size_t i = 0; i < vec.size(); ++i) {
    strings_.emplace_back(new string(vec[i]));
    slices[i] = string_view{*strings_.back()};
  }
  intptr_.SetGlobalArray(name, SliceSpan{slices});
}

bool InterpreterTest::Execute(string_view script) {
  char sha_buf[64];
  Interpreter::FuncSha1(script, sha_buf);
  string_view sha{sha_buf, std::strlen(sha_buf)};

  string result;
  Interpreter::AddResult add_res = intptr_.AddFunction(sha, script, &result);
  if (add_res == Interpreter::COMPILE_ERR) {
    error_ = result;
    return false;
  }

  Interpreter::RunResult run_res = intptr_.RunFunction(sha, &error_);
  if (run_res != Interpreter::RUN_OK) {
    return false;
  }

  ser_.res.clear();
  intptr_.SerializeResult(&ser_);
  ser_.res.pop_back();

  return true;
}

TEST_F(InterpreterTest, Basic) {
  RunInline(R"(
    function foo(n)
      return n,n+1
    end)",
            "code1");

  int type = lua_getglobal(lua(), "foo");
  ASSERT_EQ(LUA_TFUNCTION, type);
  lua_pushnumber(lua(), 42);
  lua_pcall(lua(), 1, 2, 0);
  int val1 = lua_tointeger(lua(), -1);
  int val2 = lua_tointeger(lua(), -2);
  lua_pop(lua(), 2);

  EXPECT_EQ(43, val1);
  EXPECT_EQ(42, val2);
  EXPECT_EQ(0, lua_gettop(lua()));

  lua_pushstring(lua(), "foo");
  EXPECT_EQ(3, lua_rawlen(lua(), 1));
  lua_pop(lua(), 1);

  RunInline("return {nil, 'b'}", "code2", 1);
  ASSERT_EQ(1, lua_gettop(lua()));
  LOG(INFO) << lua_typename(lua(), lua_type(lua(), -1));

  ASSERT_TRUE(lua_istable(lua(), -1));
  ASSERT_EQ(2, lua_rawlen(lua(), -1));
  lua_len(lua(), -1);
  ASSERT_EQ(2, lua_tointeger(lua(), -1));
  lua_pop(lua(), 1);

  lua_pushnil(lua());
  while (lua_next(lua(), -2)) {
    /* uses 'key' (at index -2) and 'value' (at index -1) */
    int kt = lua_type(lua(), -2);
    int vt = lua_type(lua(), -1);
    LOG(INFO) << "k/v : " << lua_typename(lua(), kt) << "/" << lua_tonumber(lua(), -2) << " "
              << lua_typename(lua(), vt);
    lua_pop(lua(), 1);
  }
}

TEST_F(InterpreterTest, UnknownFunc) {
  string_view code(R"(
    function foo(n)
      return myunknownfunc(1, n)
    end)");

  CHECK_EQ(0, luaL_loadbuffer(lua(), code.data(), code.size(), "code1"));
  CHECK_EQ(0, lua_pcall(lua(), 0, 0, 0));
  int type = lua_getglobal(lua(), "myunknownfunc");
  ASSERT_EQ(LUA_TNIL, type);
}

TEST_F(InterpreterTest, Stack) {
  RunInline(R"(
local x = {}
for i=1,127 do
   x = {x}
end
return x
)",
            "code1", 1);

  ASSERT_EQ(1, lua_gettop(lua()));
  ASSERT_TRUE(intptr_.IsResultSafe());
  lua_pop(lua(), 1);

  RunInline(R"(
local x = {}
for i=1,128 do
   x = {x}
end
return x
)",
            "code1", 1);

  ASSERT_EQ(1, lua_gettop(lua()));
  ASSERT_FALSE(intptr_.IsResultSafe());
}

TEST_F(InterpreterTest, Add) {
  const char* s1 = "return 0";
  const char* s2 = "foobar";

  char sha_buf1[64], sha_buf2[64];
  Interpreter::FuncSha1(s1, sha_buf1);
  Interpreter::FuncSha1(s2, sha_buf2);
  string_view sha1{sha_buf1, std::strlen(sha_buf1)};
  string_view sha2{sha_buf2, std::strlen(sha_buf2)};

  string err;

  EXPECT_EQ(Interpreter::ADD_OK, intptr_.AddFunction(sha1, "return 0", &err));
  EXPECT_EQ(0, lua_gettop(lua()));

  EXPECT_EQ(Interpreter::COMPILE_ERR, intptr_.AddFunction(sha2, "foobar", &err));
  EXPECT_THAT(err, testing::HasSubstr("syntax error"));
  EXPECT_EQ(0, lua_gettop(lua()));

  EXPECT_TRUE(intptr_.Exists(sha1));
}

// Test cases taken from scripting.tcl
TEST_F(InterpreterTest, Execute) {
  ASSERT_TRUE(Execute("return 42"));
  EXPECT_EQ("i(42)", ser_.res);

  EXPECT_TRUE(Execute("return 'hello'"));
  EXPECT_EQ("str(hello)", ser_.res);

  // Breaks compatibility.
  EXPECT_TRUE(Execute("return 100.5"));
  EXPECT_EQ("d(100.5)", ser_.res);

  EXPECT_TRUE(Execute("return true"));
  EXPECT_EQ("bool(1)", ser_.res);

  EXPECT_TRUE(Execute("return false"));
  EXPECT_EQ("bool(0)", ser_.res);

  EXPECT_TRUE(Execute("return {ok='fine'}"));
  EXPECT_EQ("status(fine)", ser_.res);

  EXPECT_TRUE(Execute("return {err= 'bla'}"));
  EXPECT_EQ("err(bla)", ser_.res);

  EXPECT_TRUE(Execute("return {1, 2, nil, 3}"));
  EXPECT_EQ("[i(1) i(2) nil i(3)]", ser_.res);

  EXPECT_TRUE(Execute("return {1,2,3,'ciao', {1,2}}"));
  EXPECT_EQ("[i(1) i(2) i(3) str(ciao) [i(1) i(2)]]", ser_.res);

  EXPECT_TRUE(Execute("return {map={a=1,b=2}}"));
  EXPECT_THAT(ser_.res, testing::AnyOf("{str(a) i(1) str(b) i(2)}", "{str(b) i(2) str(a) i(1)}"));
}

TEST_F(InterpreterTest, Call) {
  auto cb = [](auto ca) {
    auto* reply = ca.translator;
    auto span = ca.args;
    CHECK_GE(span.size(), 1u);
    string_view cmd{span[0].data(), span[0].size()};
    if (cmd == "string") {
      reply->OnString("foo");
    } else if (cmd == "double") {
      reply->OnDouble(3.1415);
    } else if (cmd == "int") {
      reply->OnInt(42);
    } else if (cmd == "err") {
      reply->OnError("myerr");
    } else if (cmd == "status") {
      reply->OnStatus("mystatus");
    } else {
      LOG(FATAL) << "Invalid param";
    }
  };

  intptr_.SetRedisFunc(cb);
  ASSERT_TRUE(Execute("local var = redis.pcall('string'); return {type(var), var}"));
  EXPECT_EQ("[str(string) str(foo)]", ser_.res);

  EXPECT_TRUE(Execute("local var = redis.pcall('double'); return {type(var), var}"));
  EXPECT_EQ("[str(number) d(3.1415)]", ser_.res);

  EXPECT_TRUE(Execute("local var = redis.pcall('int'); return {type(var), var}"));
  EXPECT_EQ("[str(number) i(42)]", ser_.res);

  EXPECT_TRUE(Execute("local var = redis.pcall('err'); return {type(var), var}"));
  EXPECT_EQ("[str(table) err(myerr)]", ser_.res);

  EXPECT_TRUE(Execute("local var = redis.pcall('status'); return {type(var), var}"));
  EXPECT_EQ("[str(table) status(mystatus)]", ser_.res);
}

TEST_F(InterpreterTest, CallArray) {
  auto cb = [](auto ca) {
    auto* reply = ca.translator;
    reply->OnArrayStart(2);
    reply->OnArrayStart(1);
    reply->OnArrayStart(2);
    reply->OnNil();
    reply->OnString("s2");
    reply->OnArrayEnd();
    reply->OnArrayEnd();
    reply->OnInt(42);
    reply->OnArrayEnd();
  };

  intptr_.SetRedisFunc(cb);
  EXPECT_TRUE(Execute("local var = redis.call(''); return {type(var), var}"));
  EXPECT_EQ("[str(table) [[[bool(0) str(s2)]] i(42)]]", ser_.res);
}

TEST_F(InterpreterTest, ArgKeys) {
  vector<string> vec_arr{};
  vector<string_view> slices;
  SetGlobalArray("ARGV", {"foo", "bar"});
  SetGlobalArray("KEYS", {"key1", "key2"});
  EXPECT_TRUE(Execute("return {ARGV[1], KEYS[1], KEYS[2]}"));
  EXPECT_EQ("[str(foo) str(key1) str(key2)]", ser_.res);

  SetGlobalArray("INTKEYS", {"123456", "1"});
  EXPECT_TRUE(Execute("return INTKEYS[1] + 0")) << error_;
  EXPECT_EQ("i(123456)", ser_.res);
}

TEST_F(InterpreterTest, Modules) {
  // cjson module
  EXPECT_TRUE(Execute("return cjson.encode({1, 2, 3})"));
  EXPECT_EQ("str([1,2,3])", ser_.res);
  EXPECT_TRUE(Execute("return cjson.decode('{\"a\": 1}')['a']"));
  EXPECT_EQ("d(1)", ser_.res);

  // cmsgpack module
  EXPECT_TRUE(Execute("return cmsgpack.pack('ok', true)"));
  EXPECT_EQ("str(\xA2ok\xC3)", ser_.res);

  // bit module
  EXPECT_TRUE(Execute("return bit.bor(8, 4, 5)"));
  EXPECT_EQ("i(13)", ser_.res);

  // struct module
  EXPECT_TRUE(Execute("return struct.pack('bbc4', 1, 2, 'test')"));
  EXPECT_EQ("str(\x1\x2test)", ser_.res);
}

// Check compatibility with Lua 5.1
TEST_F(InterpreterTest, Compatibility) {
  // unpack is no longer global
  EXPECT_TRUE(Execute("return unpack{1,2,3}"));
  EXPECT_EQ("i(1)", ser_.res);

  string_view test_foreach_template =
      "local t = {1,'two',3;four='yes'}; local out = {};"
      "table.{TESTF} (t, function(k, v) table.insert(out, {k, v}) end); "
      "return out; ";

  // table.foreach was removed
  string test_foreach = absl::StrReplaceAll(test_foreach_template, {{"{TESTF}", "foreach"}});
  EXPECT_TRUE(Execute(test_foreach));
  EXPECT_EQ("[[i(1) i(1)] [i(2) str(two)] [i(3) i(3)] [str(four) str(yes)]]", ser_.res);

  // table.foreachi was removed
  string test_foreachi = absl::StrReplaceAll(test_foreach_template, {{"{TESTF}", "foreachi"}});
  EXPECT_TRUE(Execute(test_foreachi));
  EXPECT_EQ("[[i(1) i(1)] [i(2) str(two)] [i(3) i(3)]]", ser_.res);

  EXPECT_FALSE(Execute("table.foreachi('not-a-table', print);"));  // check invalid args

  // table.getn was replaced with length operator
  EXPECT_TRUE(Execute("return table.getn{1, 2, 3};"));
  EXPECT_EQ("i(3)", ser_.res);

  // table.setn was removed, resizing is no longer needed, it thows an error
  EXPECT_FALSE(Execute("local t = {}; local a = 1; table.setn(t, 100); return a+123;"));
}

TEST_F(InterpreterTest, AsyncReplacement) {
  const string_view kCases[] = {
      R"(
      redis.[A]call('INCR', 'A')
      redis.[A]call('INCR', 'A')
    )",
      R"(
      function test()
        redis.[A]call('INCR', 'A')
      end
    )",
      R"(
      local b = redis.call('GET', 'A') + redis.call('GET', 'B')
    )",
      R"(
      if redis.call('EXISTS', 'A') then redis.[A]call('SET', 'B', 1) end
    )",
      R"(
      while redis.call('EXISTS', 'A') do redis.[A]call('SET', 'B', 1) end
    )",
      R"(
      while
      redis.call('EXISTS', 'A') do
        print("OK")
      end
    )",
      R"(
      print(redis.call('GET', 'A'))
    )",
      R"(
      local table = {
        redis.call('GET', 'A')
      }
    )",
      R"(
      while true do
        redis.[A]call('INCR', 'A')
      end
    )",
      R"(
      if 1 + -- now this is a tricky comment
        redis.call('GET', 'A')
        > 0
      then end
    )",
      R"(
      print('Output'
      ..
      redis.call('GET', 'A')
      )
    )",
      R"(
      while
      0 < -- we have a comment here unfortunately
      redis.call('GET', 'A')
      then end
    )",
      R"(
    while
    -- we have
    -- a tricky
    -- multiline comment
    redis.call('EXISTS')
    do end
    )",
      R"(
    --[[ WE SKIP COMMENT BLOCKS FOR NOW ]]
    redis.call('ECHO', 'TEST')
    )"};

  for (auto test : kCases) {
    auto expected = absl::StrReplaceAll(test, {{"[A]", "a"}});
    auto input = absl::StrReplaceAll(test, {{"[A]", ""}});

    auto result = Interpreter::DetectPossibleAsyncCalls(input);
    string_view output = result ? *result : input;

    EXPECT_EQ(expected, output);
  }
}

TEST_F(InterpreterTest, ReplicateCommands) {
  EXPECT_TRUE(Execute("return redis.replicate_commands()"));
  EXPECT_EQ("i(1)", ser_.res);
  EXPECT_TRUE(Execute("redis.replicate_commands()"));
  EXPECT_EQ("nil", ser_.res);
}

TEST_F(InterpreterTest, Log) {
  EXPECT_TRUE(Execute(R"(redis.log('nonsense', 'nonsense'))"));
  EXPECT_EQ("nil", ser_.res);
  EXPECT_TRUE(Execute(R"(redis.log(redis.LOG_WARNING, 'warn'))"));
  EXPECT_EQ("nil", ser_.res);
  EXPECT_FALSE(Execute(R"(redis.log(redis.LOG_WARNING))"));
  EXPECT_THAT(error_, testing::HasSubstr("requires two arguments or more"));
}

TEST_F(InterpreterTest, Robust) {
  EXPECT_FALSE(Execute(R"(eval "local a = {}
      setmetatable(a,{__index=function() foo() end})
      return a")"));
  EXPECT_EQ("", ser_.res);
}

TEST_F(InterpreterTest, Unpack) {
  auto cb = [](Interpreter::CallArgs ca) {
    auto* reply = ca.translator;
    reply->OnInt(1);
  };
  intptr_.SetRedisFunc(cb);
  ASSERT_TRUE(lua_checkstack(lua(), 7000));
  bool res = Execute(R"(
local N = 7000

local stringTable = {}
for i = 1, N do
    stringTable[i] = "String " .. i
end
  return redis.pcall('func', unpack(stringTable))
)");

  ASSERT_TRUE(res) << error_;
  EXPECT_EQ("i(1)", ser_.res);
}

}  // namespace dfly
