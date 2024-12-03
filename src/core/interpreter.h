// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/fixed_array.h>
#include <absl/types/span.h>

#include <functional>
#include <optional>
#include <string_view>

#include "util/fibers/synchronization.h"

typedef struct lua_State lua_State;

namespace dfly {

class ObjectExplorer {
 public:
  virtual ~ObjectExplorer() = default;

  virtual void OnBool(bool b) = 0;
  virtual void OnString(std::string_view str) = 0;
  virtual void OnDouble(double d) = 0;
  virtual void OnInt(int64_t val) = 0;
  virtual void OnArrayStart(unsigned len) = 0;
  virtual void OnArrayEnd() = 0;
  virtual void OnNil() = 0;
  virtual void OnStatus(std::string_view str) = 0;
  virtual void OnError(std::string_view str) = 0;

  virtual void OnMapStart(unsigned len) {
    OnArrayStart(len * 2);
  }

  virtual void OnMapEnd() {
    OnArrayEnd();
  }
};

class Interpreter {
 public:
  using SliceSpan = absl::Span<const std::string_view>;

  // Arguments received from redis.call
  struct CallArgs {
    // Full arguments, including cmd name.
    SliceSpan args;

    // Pointer to backing storage for args (excluding cmd name).
    // Moving can invalidate arg slice pointers. Moved by async to re-use buffer.
    std::string* buffer;

    ObjectExplorer* translator;

    bool async;        // async by acall
    bool error_abort;  // abort on errors (not pcall)

    // The function can request an abort due to an error, even if error_abort is false.
    // It happens when async cmds are flushed and result in an uncatched error.
    bool* requested_abort;
  };

  using RedisFunc = std::function<void(CallArgs)>;

  Interpreter();
  ~Interpreter();

  Interpreter(const Interpreter&) = delete;
  void operator=(const Interpreter&) = delete;

  Interpreter(Interpreter&&) = default;
  Interpreter& operator=(Interpreter&&) = default;

  // Note: We leak the state for now.
  // Production code should not access this method.
  lua_State* lua() {
    return lua_;
  }

  enum AddResult {
    ADD_OK = 0,
    ALREADY_EXISTS = 1,
    COMPILE_ERR = 2,
  };

  // Add function with sha and body to interpreter.
  AddResult AddFunction(std::string_view sha, std::string_view body, std::string* error);

  bool Exists(std::string_view sha) const;

  enum RunResult {
    RUN_OK = 0,
    NOT_EXISTS = 1,
    RUN_ERR = 2,
  };

  void SetGlobalArray(const char* name, SliceSpan args);

  // Runs already added function sha returned by a successful call to AddFunction().
  // Returns: true if the call succeeded, otherwise fills error and returns false.
  // sha must be 40 char length.
  RunResult RunFunction(std::string_view sha, std::string* err);

  // Checks whether the result is safe to serialize.
  // Should fit 2 conditions:
  // 1. Be the only value on the stack.
  // 2. Should have depth of no more than 128.
  bool IsResultSafe() const;

  void SerializeResult(ObjectExplorer* serializer);

  void ResetStack();

  void RunGC();

  // fp must point to buffer with at least 41 chars.
  // fp[40] will be set to '\0'.
  static void FuncSha1(std::string_view body, char* fp);

  static std::optional<std::string> DetectPossibleAsyncCalls(std::string_view body);

  template <typename U> void SetRedisFunc(U&& u) {
    redis_func_ = std::forward<U>(u);
  }

  // Invoke command with arguments from lua stack, given options and possibly custom explorer
  int RedisGenericCommand(bool raise_error, bool async, ObjectExplorer* explorer = nullptr);

 private:
  // Returns true if function was successfully added,
  // otherwise returns false and sets the error.
  bool AddInternal(const char* f_id, std::string_view body, std::string* error);
  bool IsTableSafe() const;

  static int RedisCallCommand(lua_State* lua);
  static int RedisPCallCommand(lua_State* lua);
  static int RedisACallCommand(lua_State* lua);
  static int RedisAPCallCommand(lua_State* lua);

  std::optional<absl::FixedArray<std::string_view, 4>> PrepareArgs();
  bool CallRedisFunction(bool raise_error, bool async, ObjectExplorer* explorer, SliceSpan args);

  lua_State* lua_;
  unsigned cmd_depth_ = 0;
  RedisFunc redis_func_;
  std::string buffer_;
  char name_buffer_[32];  // backing storage for cmd name
};

// Manages an internal interpreter pool. This allows multiple connections residing on the same
// thread to run multiple lua scripts in parallel.
class InterpreterManager {
 public:
  struct Stats {
    Stats& operator+=(const Stats& other);

    uint64_t used_bytes = 0;
    uint64_t interpreter_cnt = 0;
    uint64_t blocked_cnt = 0;
  };

 public:
  InterpreterManager(unsigned num) : waker_{}, available_{}, storage_{} {
    // We pre-allocate the backing storage during initialization and
    // start storing pointers to slots in the available vector.
    storage_.reserve(num);
  }

  // Borrow interpreter. Always return it after usage.
  Interpreter* Get();
  void Return(Interpreter*);

  // Clear all interpreters, keeps capacity. Waits until all are returned.
  void Reset();

  // Run on all unused interpreters. Those are marked as used at once, so the callback can preempt
  void Alter(std::function<void(Interpreter*)> modf);

  static Stats& tl_stats();

 private:
  util::fb2::EventCount waker_, reset_ec_;
  std::vector<Interpreter*> available_;
  std::vector<Interpreter> storage_;

  util::fb2::Mutex reset_mu_;  // Acts as a singleton.

  unsigned return_untracked_ = 0;  // Number of returned interpreters during reset.
};

}  // namespace dfly
