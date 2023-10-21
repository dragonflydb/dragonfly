// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <absl/types/span.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

#include "core/fibers.h"
#include "facade/facade_types.h"
#include "facade/op_status.h"

namespace dfly {

enum class ListDir : uint8_t { LEFT, RIGHT };

// Dependent on ExpirePeriod representation of the value.
constexpr int64_t kMaxExpireDeadlineSec = (1u << 28) - 1;

using DbIndex = uint16_t;
using ShardId = uint16_t;
using LSN = uint64_t;
using TxId = uint64_t;
using TxClock = uint64_t;

using facade::ArgS;
using facade::CmdArgList;
using facade::CmdArgVec;
using facade::MutableSlice;
using facade::OpResult;

using ArgSlice = absl::Span<const std::string_view>;
using StringVec = std::vector<std::string>;

// keys are RDB_TYPE_xxx constants.
using RdbTypeFreqMap = absl::flat_hash_map<unsigned, size_t>;

constexpr DbIndex kInvalidDbId = DbIndex(-1);
constexpr ShardId kInvalidSid = ShardId(-1);
constexpr DbIndex kMaxDbId = 1024;  // Reasonable starting point.

class CommandId;
class Transaction;
class EngineShard;

struct KeyLockArgs {
  static bool IsLockHashTagEnabled();

  // Before acquiring and releasing keys, one must "normalize" them via GetLockKey().
  static std::string_view GetLockKey(std::string_view key);

  DbIndex db_index = 0;
  ArgSlice args;
  unsigned key_step = 1;
};

// Describes key indices.
struct KeyIndex {
  unsigned start;
  unsigned end;   // does not include this index (open limit).
  unsigned step;  // 1 for commands like mget. 2 for commands like mset.

  // if index is non-zero then adds another key index (usually 0).
  // relevant for for commands like ZUNIONSTORE/ZINTERSTORE for destination key.
  std::optional<uint16_t> bonus{};

  static KeyIndex Empty() {
    return KeyIndex{0, 0, 0, std::nullopt};
  }

  static KeyIndex Range(unsigned start, unsigned end, unsigned step = 1) {
    return KeyIndex{start, end, step, std::nullopt};
  }

  bool HasSingleKey() const {
    return !bonus && (start + step >= end);
  }

  unsigned num_args() const {
    return end - start + bool(bonus);
  }
};

struct DbContext {
  DbIndex db_index = 0;
  uint64_t time_now_ms = 0;
};

struct OpArgs {
  EngineShard* shard;
  const Transaction* tx;
  DbContext db_cntx;

  OpArgs() : shard(nullptr), tx(nullptr) {
  }

  OpArgs(EngineShard* s, const Transaction* tx, const DbContext& cntx)
      : shard(s), tx(tx), db_cntx(cntx) {
  }
};

// Record non auto journal command with own txid and dbid.
void RecordJournal(const OpArgs& op_args, std::string_view cmd, ArgSlice args,
                   uint32_t shard_cnt = 1, bool multi_commands = false);

// Record non auto journal command finish. Call only when command translates to multi commands.
void RecordJournalFinish(const OpArgs& op_args, uint32_t shard_cnt);

// Record expiry in journal with independent transaction. Must be called from shard thread holding
// key.
void RecordExpiry(DbIndex dbid, std::string_view key);

// Trigger journal write to sink, no journal record will be added to journal.
// Must be called from shard thread of journal to sink.
void TriggerJournalWriteToSink();

struct TieredStats {
  size_t tiered_reads = 0;
  size_t tiered_writes = 0;

  size_t storage_capacity = 0;

  // how much was reserved by actively stored items.
  size_t storage_reserved = 0;
  size_t aborted_write_cnt = 0;
  size_t flush_skip_cnt = 0;

  TieredStats& operator+=(const TieredStats&);
};

struct SearchStats {
  size_t used_memory = 0;
  size_t num_indices = 0;
  size_t num_entries = 0;

  SearchStats& operator+=(const SearchStats&);
};

enum class GlobalState : uint8_t {
  ACTIVE,
  LOADING,
  SAVING,
  SHUTTING_DOWN,
  TAKEN_OVER,
};

enum class TimeUnit : uint8_t { SEC, MSEC };

inline void ToUpper(const MutableSlice* val) {
  for (auto& c : *val) {
    c = absl::ascii_toupper(c);
  }
}

inline void ToLower(const MutableSlice* val) {
  for (auto& c : *val) {
    c = absl::ascii_tolower(c);
  }
}

bool ParseHumanReadableBytes(std::string_view str, int64_t* num_bytes);
bool ParseDouble(std::string_view src, double* value);
const char* ObjTypeName(int type);

const char* RdbTypeName(unsigned type);

// Cached values, updated frequently to represent the correct state of the system.
extern std::atomic_uint64_t used_mem_peak;
extern std::atomic_uint64_t used_mem_current;
extern size_t max_memory_limit;

// malloc memory stats.
int64_t GetMallocCurrentCommitted();

// version 5.11 maps to 511 etc.
// set upon server start.
extern unsigned kernel_version;

const char* GlobalStateName(GlobalState gs);

template <typename RandGen> std::string GetRandomHex(RandGen& gen, size_t len) {
  static_assert(std::is_same<uint64_t, decltype(gen())>::value);
  std::string res(len, '\0');
  size_t indx = 0;

  for (size_t i = 0; i < len / 16; ++i) {  // 2 chars per byte
    absl::numbers_internal::FastHexToBufferZeroPad16(gen(), res.data() + indx);
    indx += 16;
  }

  if (indx < res.size()) {
    char buf[32];
    absl::numbers_internal::FastHexToBufferZeroPad16(gen(), buf);

    for (unsigned j = 0; indx < res.size(); indx++, j++) {
      res[indx] = buf[j];
    }
  }

  return res;
}

// AggregateValue is a thread safe utility to store the first
// truthy value;
template <typename T> struct AggregateValue {
  bool operator=(T val) {
    std::lock_guard l{mu_};
    if (!bool(current_) && bool(val)) {
      current_ = val;
    }
    return bool(val);
  }

  T operator*() {
    std::lock_guard l{mu_};
    return current_;
  }

  operator bool() {
    return bool(**this);
  }

 private:
  Mutex mu_{};
  T current_{};
};

// Thread safe utility to store the first non null error.
using AggregateError = AggregateValue<std::error_code>;

// Thread safe utility to store the first non OK status.
using AggregateStatus = AggregateValue<facade::OpStatus>;
static_assert(bool(facade::OpStatus::OK) == false,
              "Default intitialization should be a falsy OK value");

// Simple wrapper interface around atomic cancellation flag.
struct Cancellation {
  Cancellation() : flag_{false} {
  }

  void Cancel() {
    flag_.store(true, std::memory_order_relaxed);
  }

  bool IsCancelled() const {
    return flag_.load(std::memory_order_relaxed);
  }

 protected:
  std::atomic_bool flag_;
};

// Error wrapper, that stores error_code and optional string message.
class GenericError {
 public:
  GenericError() = default;
  GenericError(std::error_code ec) : ec_{ec}, details_{} {
  }
  GenericError(std::string details) : ec_{}, details_{std::move(details)} {
  }
  GenericError(std::error_code ec, std::string details) : ec_{ec}, details_{std::move(details)} {
  }

  operator std::error_code() const;
  operator bool() const;

  std::string Format() const;  // Get string representation of error.

 private:
  std::error_code ec_;
  std::string details_;
};

// Thread safe utility to store the first non null generic error.
using AggregateGenericError = AggregateValue<GenericError>;

// Context is a utility for managing error reporting and cancellation for complex tasks.
//
// When submitting an error with `Error`, only the first is stored (as in aggregate values).
// Then a special error handler is run, if present, and the context is cancelled. The error handler
// is run in a separate handler to free up the caller.
//
// Manual cancellation with `Cancel` is simulated by reporting an `errc::operation_canceled` error.
// This allows running the error handler and representing this scenario as an error.
class Context : protected Cancellation {
 public:
  using ErrHandler = std::function<void(const GenericError&)>;

  Context() = default;
  Context(ErrHandler err_handler) : Cancellation{}, err_{}, err_handler_{std::move(err_handler)} {
  }

  ~Context();

  void Cancel();  // Cancels the context by submitting an `errc::operation_canceled` error.
  using Cancellation::IsCancelled;
  const Cancellation* GetCancellation() const;

  GenericError GetError();

  // Report an error by submitting arguments for GenericError.
  // If this is the first error that occured, then the error handler is run
  // and the context is cancelled.
  template <typename... T> GenericError ReportError(T... ts) {
    return ReportErrorInternal(GenericError{std::forward<T>(ts)...});
  }

  // Wait for error handler to stop, reset error and cancellation flag, assign new error handler.
  void Reset(ErrHandler handler);

  // Atomically replace the error handler if no error is present, and return the
  // current stored error. This function can be used to transfer cleanup responsibility safely
  //
  // Beware, never do this manually in two steps. If you check for cancellation,
  // set the error handler and initialize resources, then the new error handler
  // will never run if the context was cancelled between the first two steps.
  GenericError SwitchErrorHandler(ErrHandler handler);

  // If any error handler is running, wait for it to stop.
  void JoinErrorHandler();

 private:
  // Report error.
  GenericError ReportErrorInternal(GenericError&& err);

 private:
  GenericError err_;
  Mutex mu_;

  ErrHandler err_handler_;
  Fiber err_handler_fb_;
};

struct ScanOpts {
  std::string_view pattern;
  size_t limit = 10;
  std::string_view type_filter;
  unsigned bucket_id = UINT_MAX;

  bool Matches(std::string_view val_name) const;
  static OpResult<ScanOpts> TryFrom(CmdArgList args);
};

// I use relative time from Feb 1, 2023 in seconds.
constexpr uint64_t kMemberExpiryBase = 1675209600;

inline uint32_t MemberTimeSeconds(uint64_t now_ms) {
  return (now_ms / 1000) - kMemberExpiryBase;
}

}  // namespace dfly
