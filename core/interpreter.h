// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string_view>

typedef struct lua_State lua_State;

namespace dfly {

class Interpreter {
 public:
  Interpreter();
  ~Interpreter();

  Interpreter(const Interpreter&) = delete;
  void operator=(const Interpreter&) = delete;

  // Note: We leak the state for now.
  // Production code should not access this method.
  lua_State* lua() {
    return lua_;
  }

  // returns false if an error happenned, sets error string into result.
  // otherwise, returns true and sets result to function id.
  bool AddFunction(std::string_view body, std::string* result);

  // Runs already added function f_id returned by a successful call to AddFunction().
  // Returns: true if the call succeeded, otherwise fills error and returns false.
  bool RunFunction(const char* f_id, std::string* err);

  // fp must point to buffer with at least 43 chars.
  // fp[42] will be set to '\0'.
  static void Fingerprint(std::string_view body, char* fp);

 private:
  lua_State* lua_;
};

}  // namespace dfly
