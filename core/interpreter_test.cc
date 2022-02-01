// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/interpreter.h"

extern "C" {
#include <lauxlib.h>
#include <lua.h>
}

#include <absl/strings/str_cat.h>
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
    OnItem();
  }

  void OnString(std::string_view str) final {
    absl::StrAppend(&res, "str(", str, ") ");
    OnItem();
  }

  void OnDouble(double d) final {
    absl::StrAppend(&res, "d(", d, ") ");
    OnItem();
  }

  void OnInt(int64_t val) final {
    absl::StrAppend(&res, "i(", val, ") ");
    OnItem();
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

  void OnStatus(std::string_view str) {
    absl::StrAppend(&res, "status(", str, ") ");
  }

  void OnError(std::string_view str) {
    absl::StrAppend(&res, "err(", str, ") ");
  }

 private:
  void OnItem() {
  }
};

class InterpreterTest : public ::testing::Test {
 protected:
  InterpreterTest() {
  }

  lua_State* lua() {
    return intptr_.lua();
  }

  void RunInline(string_view buf, const char* name) {
    CHECK_EQ(0, luaL_loadbuffer(lua(), buf.data(), buf.size(), name));
    CHECK_EQ(0, lua_pcall(lua(), 0, 0, 0));
  }

  bool Serialize(string* err) {
    ser_.res.clear();
    bool res = intptr_.Serialize(&ser_, err);
    if (!ser_.res.empty())
      ser_.res.pop_back();
    return res;
  }

  bool Execute(string_view script);

  Interpreter intptr_;
  TestSerializer ser_;
  string error_;
};

bool InterpreterTest::Execute(string_view script) {
  char buf[48];

  return intptr_.Execute(script, buf, &error_) && Serialize(&error_);
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
}

TEST_F(InterpreterTest, Add) {
  string res1, res2;

  EXPECT_TRUE(intptr_.AddFunction("return 0", &res1));
  EXPECT_EQ(0, lua_gettop(lua()));
  EXPECT_FALSE(intptr_.AddFunction("foobar", &res2));
  EXPECT_THAT(res2, testing::HasSubstr("syntax error"));
  EXPECT_EQ(0, lua_gettop(lua()));
}

// Test cases taken from scripting.tcl
TEST_F(InterpreterTest, Execute) {
  EXPECT_TRUE(Execute("return 42"));
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

  EXPECT_TRUE(Execute("return {1,2,3,'ciao',{1,2}}"));
  EXPECT_EQ("[i(1) i(2) i(3) str(ciao) [i(1) i(2)]]", ser_.res);
}

}  // namespace dfly
