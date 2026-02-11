// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

#include "facade/facade_types.h"
#include "server/common_types.h"
#include "server/error_types.h"
#include "server/stats.h"
#include "util/fibers/fibers.h"
#include "util/fibers/synchronization.h"

namespace dfly {

using CompactObjType = unsigned;
class GlobMatcher;

// Dependent on ExpirePeriod representation of the value.
constexpr int64_t kMaxExpireDeadlineSec = (1u << 28) - 1;  // 8.5 years
constexpr int64_t kMaxExpireDeadlineMs = kMaxExpireDeadlineSec * 1000;

using facade::ArgS;
using facade::CmdArgList;
using facade::CmdArgVec;
using facade::MutableSlice;
using facade::OpResult;

using StringVec = std::vector<std::string>;

class CommandId;
class Transaction;
class EngineShard;
struct ConnectionState;
class Interpreter;
class Namespaces;

struct LockTagOptions {
  bool enabled = false;
  char open_locktag = '{';
  char close_locktag = '}';
  unsigned skip_n_end_delimiters = 0;
  std::string prefix;

  // Returns the tag according to the rules defined by this options object.
  std::string_view Tag(std::string_view key) const;

  static const LockTagOptions& instance();
};

enum class GlobalState : uint8_t {
  ACTIVE,
  LOADING,
  SHUTTING_DOWN,
  TAKEN_OVER,
};

std::ostream& operator<<(std::ostream& os, const GlobalState& state);

enum class TimeUnit : uint8_t { SEC, MSEC };

enum ExpireFlags {
  EXPIRE_ALWAYS = 0,
  EXPIRE_NX = 1 << 0,  // Set expiry only when key has no expiry
  EXPIRE_XX = 1 << 2,  // Set expiry only when the key has expiry
  EXPIRE_GT = 1 << 3,  // GT: Set expiry only when the new expiry is greater than current one
  EXPIRE_LT = 1 << 4,  // LT: Set expiry only when the new expiry is less than current one
};

bool ParseHumanReadableBytes(std::string_view str, int64_t* num_bytes);
bool ParseDouble(std::string_view src, double* value);

const char* RdbTypeName(unsigned type);

// Globally used atomics for memory readings
inline std::atomic_uint64_t used_mem_current{0};
inline std::atomic_uint64_t rss_mem_current{0};
// Current value of --maxmemory flag
inline std::atomic_uint64_t max_memory_limit{0};

inline Namespaces* namespaces = nullptr;

// version 5.11 maps to 511 etc.
// set upon server start.
inline unsigned kernel_version = 0;

const char* GlobalStateName(GlobalState gs);

struct ScanOpts {
  ~ScanOpts();  // because of forward declaration
  ScanOpts() = default;
  ScanOpts(ScanOpts&& other) = default;

  bool Matches(std::string_view val_name) const;
  static OpResult<ScanOpts> TryFrom(CmdArgList args, bool allow_novalues = false);

  std::unique_ptr<GlobMatcher> matcher;
  size_t limit = 10;
  std::optional<CompactObjType> type_filter;
  unsigned bucket_id = UINT_MAX;
  enum class Mask {
    Volatile,   // volatile, keys that have ttl
    Permanent,  // permanent, keys that do not have ttl
    Accessed,   // accessed, the key has been accessed since the last load/flush event, or the last
                // time a flag was reset.
    Untouched,  // untouched, the key has not been accessed/touched.
  };
  std::optional<Mask> mask;
  size_t min_malloc_size = 0;
  bool novalues = false;
};

// I use relative time from Feb 1, 2023 in seconds.
constexpr uint64_t kMemberExpiryBase = 1675209600;

inline uint32_t MemberTimeSeconds(uint64_t now_ms) {
  return (now_ms / 1000) - kMemberExpiryBase;
}

// Helper class used to guarantee atomicity between serialization of buckets
class ABSL_LOCKABLE ThreadLocalMutex {
 public:
  ThreadLocalMutex();
  ~ThreadLocalMutex();

  void lock() ABSL_EXCLUSIVE_LOCK_FUNCTION();
  void unlock() ABSL_UNLOCK_FUNCTION();

 private:
  EngineShard* shard_;
  util::fb2::CondVarAny cond_var_;
  bool flag_ = false;
  util::fb2::detail::FiberInterface* locked_fiber_{nullptr};
};

// Replacement of std::SharedLock that allows -Wthread-safety
template <typename Mutex> class ABSL_SCOPED_LOCKABLE SharedLock {
 public:
  explicit SharedLock(Mutex& m) ABSL_EXCLUSIVE_LOCK_FUNCTION(m) : m_(m) {
    m_.lock_shared();
    is_locked_ = true;
  }

  ~SharedLock() ABSL_UNLOCK_FUNCTION() {
    if (is_locked_) {
      m_.unlock_shared();
    }
  }

  void unlock() ABSL_UNLOCK_FUNCTION() {
    m_.unlock_shared();
    is_locked_ = false;
  }

 private:
  Mutex& m_;
  bool is_locked_;
};

// Ensures availability of an interpreter for EVAL-like commands and it's automatic release.
// If it's part of MULTI, the preborrowed interpreter is returned, otherwise a new is acquired.
struct BorrowedInterpreter {
  BorrowedInterpreter(Transaction* tx, ConnectionState* state);

  ~BorrowedInterpreter();

  // Give up ownership of the interpreter, it must be returned manually.
  Interpreter* Release() && {
    assert(owned_);
    owned_ = false;
    return interpreter_;
  }

  operator Interpreter*() {
    return interpreter_;
  }

 private:
  Interpreter* interpreter_ = nullptr;
  bool owned_ = false;
};

// A single threaded latch that passes a waiter fiber if its count is 0.
// Fibers that increase/decrease the count do not wait on the latch.
class LocalLatch {
 public:
  void lock() {
    ++mutating_;
  }

  void unlock();

  void Wait();

  bool IsBlocked() const {
    return mutating_ > 0;
  }

 private:
  util::fb2::CondVarAny cond_var_;
  size_t mutating_ = 0;
};

}  // namespace dfly
