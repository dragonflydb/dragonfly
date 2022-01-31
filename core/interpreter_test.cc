// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/interpreter.h"

extern "C" {
#include <lauxlib.h>
#include <lua.h>
}

#include <gmock/gmock.h>

#include "base/gtest.h"
#include "base/logging.h"

namespace dfly {
using namespace std;

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

  Interpreter intptr_;
};

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

}  // namespace dfly
