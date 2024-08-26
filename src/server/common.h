// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/random/random.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <absl/types/span.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

#include "base/logging.h"
#include "core/compact_object.h"
#include "facade/facade_types.h"
#include "facade/op_status.h"
#include "util/fibers/fibers.h"
#include "util/fibers/synchronization.h"

namespace dfly {

enum class ListDir : uint8_t { LEFT, RIGHT };

// Dependent on ExpirePeriod representation of the value.
constexpr int64_t kMaxExpireDeadlineSec = (1u << 28) - 1;  // 8.5 years
constexpr int64_t kMaxExpireDeadlineMs = kMaxExpireDeadlineSec * 1000;

using LSN = uint64_t;
using TxId = uint64_t;
using TxClock = uint64_t;

using facade::ArgS;
using facade::CmdArgList;
using facade::CmdArgVec;
using facade::MutableSlice;
using facade::OpResult;

using StringVec = std::vector<std::string>;

// keys are RDB_TYPE_xxx constants.
using RdbTypeFreqMap = absl::flat_hash_map<unsigned, size_t>;

class CommandId;
class Transaction;
class EngineShard;

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

struct TieredStats {
  uint64_t total_stashes = 0;
  uint64_t total_fetches = 0;
  uint64_t total_cancels = 0;
  uint64_t total_deletes = 0;
  uint64_t total_defrags = 0;
  uint64_t total_uploads = 0;
  uint64_t total_registered_buf_allocs = 0;
  uint64_t total_heap_buf_allocs = 0;

  // How many times the system did not perform Stash call (disjoint with total_stashes).
  uint64_t total_stash_overflows = 0;
  uint64_t total_offloading_steps = 0;
  uint64_t total_offloading_stashes = 0;

  size_t allocated_bytes = 0;
  size_t capacity_bytes = 0;

  uint32_t pending_read_cnt = 0;
  uint32_t pending_stash_cnt = 0;

  uint64_t small_bins_cnt = 0;
  uint64_t small_bins_entries_cnt = 0;
  size_t small_bins_filling_bytes = 0;
  size_t cold_storage_bytes = 0;

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
  SHUTTING_DOWN,
  TAKEN_OVER,
};

std::ostream& operator<<(std::ostream& os, const GlobalState& state);

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

const char* RdbTypeName(unsigned type);

// Cached values, updated frequently to represent the correct state of the system.
extern std::atomic_uint64_t used_mem_peak;
extern std::atomic_uint64_t used_mem_current;
extern std::atomic_uint64_t rss_mem_current;
extern std::atomic_uint64_t rss_mem_peak;

extern size_t max_memory_limit;

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
  util::fb2::Mutex mu_{};
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

  GenericError GetError() const;

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

  GenericError err_;
  ErrHandler err_handler_;
  util::fb2::Fiber err_handler_fb_;

  // We use regular mutexes to be able to call ReportError directly from I/O callbacks.
  mutable std::mutex err_mu_;  // protects err_ and err_handler_
};

struct ScanOpts {
  std::optional<std::string_view> pattern;
  size_t limit = 10;
  std::optional<CompactObjType> type_filter;
  unsigned bucket_id = UINT_MAX;

  bool Matches(std::string_view val_name) const;
  static OpResult<ScanOpts> TryFrom(CmdArgList args);
};

// I use relative time from Feb 1, 2023 in seconds.
constexpr uint64_t kMemberExpiryBase = 1675209600;

inline uint32_t MemberTimeSeconds(uint64_t now_ms) {
  return (now_ms / 1000) - kMemberExpiryBase;
}

struct MemoryBytesFlag {
  uint64_t value = 0;
};

bool AbslParseFlag(std::string_view in, dfly::MemoryBytesFlag* flag, std::string* err);
std::string AbslUnparseFlag(const dfly::MemoryBytesFlag& flag);

using RandomPick = std::uint32_t;

class PicksGenerator {
 public:
  virtual RandomPick Generate() = 0;
  virtual ~PicksGenerator() = default;
};

class NonUniquePicksGenerator : public PicksGenerator {
 public:
  /* The generated value will be within the closed-open interval [0, max_range) */
  NonUniquePicksGenerator(RandomPick max_range);

  RandomPick Generate() override;

 private:
  const RandomPick max_range_;
  absl::BitGen bitgen_{};
};

/*
 * Generates unique index in O(1).
 *
 * picks_count specifies the number of random indexes to be generated.
 * In other words, this is the number of times the Generate() function is called.
 *
 * The class uses Robert Floyd's sampling algorithm
 * https://dl.acm.org/doi/pdf/10.1145/30401.315746
 * */
class UniquePicksGenerator : public PicksGenerator {
 public:
  /* The generated value will be within the closed-open interval [0, max_range) */
  UniquePicksGenerator(std::uint32_t picks_count, RandomPick max_range);

  RandomPick Generate() override;

 private:
  RandomPick current_random_limit_;
  std::uint32_t remaining_picks_count_;
  std::unordered_set<RandomPick> picked_indexes_;
  absl::BitGen bitgen_{};
};

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

extern size_t serialization_max_chunk_size;

}  // namespace dfly
