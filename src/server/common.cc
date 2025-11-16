// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/common.h"

#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <fast_float/fast_float.h>

#include <system_error>

extern "C" {
#include "redis/rdb.h"
}

#include "base/flags.h"
#include "base/logging.h"
#include "core/compact_object.h"
#include "core/interpreter.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/journal/journal.h"
#include "server/server_state.h"
#include "server/transaction.h"

// We've generalized "hashtags" so that users can specify custom delimiter and closures, see below.
// If I had a time machine, I'd rename this to lock_on_tags.
ABSL_FLAG(bool, lock_on_hashtags, false,
          "When true, locks are done in the {hashtag} level instead of key level. Hashtag "
          "extraction can be further configured with locktag_* flags.");

// We would have used `char` instead of `string`, but that's impossible.
ABSL_FLAG(
    std::string, locktag_delimiter, "",
    "If set, this char is used to extract a lock tag by looking at delimiters, like hash tags. If "
    "unset, regular hashtag extraction is done (with {}). Must be used with --lock_on_hashtags");

ABSL_FLAG(unsigned, locktag_skip_n_end_delimiters, 0,
          "How many closing tag delimiters should we skip when extracting lock tags. 0 for no "
          "skipping. For example, when delimiter is ':' and this flag is 2, the locktag for "
          "':a:b:c:d:e' will be 'a:b:c'.");

ABSL_FLAG(std::string, locktag_prefix, "",
          "Only keys with this prefix participate in tag extraction.");

namespace dfly {

using namespace std;
using namespace util;

namespace {

// Thread-local cache with static linkage.
thread_local std::optional<LockTagOptions> locktag_lock_options;

}  // namespace

void TEST_InvalidateLockTagOptions() {
  locktag_lock_options = nullopt;  // For test main thread
  CHECK(shard_set != nullptr);
  shard_set->pool()->AwaitBrief(
      [](ShardId shard, ProactorBase* proactor) { locktag_lock_options = nullopt; });
}

const LockTagOptions& LockTagOptions::instance() {
  if (!locktag_lock_options.has_value()) {
    string delimiter = absl::GetFlag(FLAGS_locktag_delimiter);
    if (delimiter.empty()) {
      delimiter = "{}";
    } else if (delimiter.size() == 1) {
      delimiter += delimiter;  // Copy delimiter (e.g. "::") so that it's easier to use below
    } else {
      LOG(ERROR) << "Invalid value for locktag_delimiter - must be a single char";
      exit(-1);
    }

    locktag_lock_options = {
        .enabled = absl::GetFlag(FLAGS_lock_on_hashtags),
        .open_locktag = delimiter[0],
        .close_locktag = delimiter[1],
        .skip_n_end_delimiters = absl::GetFlag(FLAGS_locktag_skip_n_end_delimiters),
        .prefix = absl::GetFlag(FLAGS_locktag_prefix),
    };
  }

  return *locktag_lock_options;
}

std::string_view LockTagOptions::Tag(std::string_view key) const {
  if (!absl::StartsWith(key, prefix)) {
    return key;
  }

  const size_t start = key.find(open_locktag);
  if (start == key.npos) {
    return key;
  }

  size_t end = start;
  for (unsigned i = 0; i <= skip_n_end_delimiters; ++i) {
    size_t next = end + 1;
    end = key.find(close_locktag, next);
    if (end == key.npos || end == next) {
      return key;
    }
  }

  return key.substr(start + 1, end - start - 1);
}

const char* GlobalStateName(GlobalState s) {
  switch (s) {
    case GlobalState::ACTIVE:
      return "ACTIVE";
    case GlobalState::LOADING:
      return "LOADING";
    case GlobalState::SHUTTING_DOWN:
      return "SHUTTING DOWN";
    case GlobalState::TAKEN_OVER:
      return "TAKEN OVER";
  }
  ABSL_UNREACHABLE();
}

const char* RdbTypeName(unsigned type) {
  switch (type) {
    case RDB_TYPE_STRING:
      return "string";
    case RDB_TYPE_LIST:
      return "list";
    case RDB_TYPE_SET:
      return "set";
    case RDB_TYPE_ZSET:
      return "zset";
    case RDB_TYPE_HASH:
      return "hash";
    case RDB_TYPE_STREAM_LISTPACKS:
      return "stream";
  }
  return "other";
}

bool ParseDouble(string_view src, double* value) {
  if (src.empty())
    return false;

  if (absl::EqualsIgnoreCase(src, "-inf")) {
    *value = -HUGE_VAL;
  } else if (absl::EqualsIgnoreCase(src, "+inf")) {
    *value = HUGE_VAL;
  } else {
    fast_float::from_chars_result result = fast_float::from_chars(src.data(), src.end(), *value);
    // nan double could be sent as "nan" with any case.
    if (int(result.ec) != 0 || result.ptr != src.end() || isnan(*value))
      return false;
  }
  return true;
}

#define ADD(x) (x) += o.x

TieredStats& TieredStats::operator+=(const TieredStats& o) {
  static_assert(sizeof(TieredStats) == 160);

  ADD(total_stashes);
  ADD(total_fetches);
  ADD(total_cancels);
  ADD(total_deletes);
  ADD(total_defrags);
  ADD(total_uploads);
  ADD(total_heap_buf_allocs);
  ADD(total_registered_buf_allocs);

  ADD(allocated_bytes);
  ADD(capacity_bytes);

  ADD(pending_read_cnt);
  ADD(pending_stash_cnt);

  ADD(small_bins_cnt);
  ADD(small_bins_entries_cnt);
  ADD(small_bins_filling_bytes);
  ADD(total_stash_overflows);
  ADD(cold_storage_bytes);
  ADD(total_offloading_steps);
  ADD(total_offloading_stashes);

  ADD(clients_throttled);
  ADD(total_clients_throttled);
  return *this;
}

SearchStats& SearchStats::operator+=(const SearchStats& o) {
  static_assert(sizeof(SearchStats) == 24);
  ADD(used_memory);
  ADD(num_entries);

  // Different shards could have inconsistent num_indices values during concurrent operations.
  // This can happen on concurrent index creation.
  // We use max to ensure that the total num_indices is the maximum of all shards.
  num_indices = std::max(num_indices, o.num_indices);
  return *this;
}

#undef ADD

OpResult<ScanOpts> ScanOpts::TryFrom(CmdArgList args) {
  ScanOpts scan_opts;

  for (unsigned i = 0; i < args.size(); i += 2) {
    if (i + 1 == args.size()) {
      return facade::OpStatus::SYNTAX_ERR;
    }

    string opt = absl::AsciiStrToUpper(ArgS(args, i));
    if (opt == "COUNT") {
      if (!absl::SimpleAtoi(ArgS(args, i + 1), &scan_opts.limit)) {
        return facade::OpStatus::INVALID_INT;
      }
      if (scan_opts.limit == 0)
        scan_opts.limit = 1;
    } else if (opt == "MATCH") {
      string_view pattern = ArgS(args, i + 1);
      if (pattern != "*")
        scan_opts.matcher.reset(new GlobMatcher{pattern, true});
    } else if (opt == "TYPE") {
      CompactObjType obj_type = ObjTypeFromString(ArgS(args, i + 1));
      if (obj_type == kInvalidCompactObjType) {
        return facade::OpStatus::SYNTAX_ERR;
      }
      scan_opts.type_filter = obj_type;
    } else if (opt == "BUCKET") {
      if (!absl::SimpleAtoi(ArgS(args, i + 1), &scan_opts.bucket_id)) {
        return facade::OpStatus::INVALID_INT;
      }
    } else if (opt == "ATTR") {
      string_view mask = ArgS(args, i + 1);
      if (mask == "v") {
        scan_opts.mask = ScanOpts::Mask::Volatile;
      } else if (mask == "p") {
        scan_opts.mask = ScanOpts::Mask::Permanent;
      } else if (mask == "a") {
        scan_opts.mask = ScanOpts::Mask::Accessed;
      } else if (mask == "u") {
        scan_opts.mask = ScanOpts::Mask::Untouched;
      } else {
        return facade::OpStatus::SYNTAX_ERR;
      }
    } else if (opt == "MINMSZ") {
      if (!absl::SimpleAtoi(ArgS(args, i + 1), &scan_opts.min_malloc_size)) {
        return facade::OpStatus::INVALID_INT;
      }
    } else {
      return facade::OpStatus::SYNTAX_ERR;
    }
  }
  return scan_opts;
}

bool ScanOpts::Matches(std::string_view val_name) const {
  return !matcher || matcher->Matches(val_name);
}

GenericError::operator std::error_code() const {
  return ec_;
}

GenericError::operator bool() const {
  return bool(ec_) || !details_.empty();
}

std::string GenericError::Format() const {
  if (!ec_ && details_.empty())
    return "";

  if (details_.empty())
    return ec_.message();
  else if (!ec_)
    return details_;
  else
    return absl::StrCat(ec_.message(), ": ", details_);
}

ExecutionState::~ExecutionState() {
  DCHECK(!err_handler_fb_.IsJoinable());
  err_handler_fb_.JoinIfNeeded();
}

GenericError ExecutionState::GetError() const {
  std::lock_guard lk(err_mu_);
  return err_;
}

void ExecutionState::ReportCancelError() {
  ReportError(std::make_error_code(errc::operation_canceled), "ExecutionState cancelled");
}

void ExecutionState::Reset(ErrHandler handler) {
  fb2::Fiber fb;

  unique_lock lk{err_mu_};
  err_ = {};
  err_handler_ = std::move(handler);
  state_.store(State::RUN, std::memory_order_relaxed);
  fb.swap(err_handler_fb_);
  lk.unlock();
  fb.JoinIfNeeded();
}

GenericError ExecutionState::SwitchErrorHandler(ErrHandler handler) {
  std::lock_guard lk{err_mu_};
  if (!err_) {
    // No need to check for the error handler - it can't be running
    // if no error is set.
    err_handler_ = std::move(handler);
  }
  return err_;
}

void ExecutionState::JoinErrorHandler() {
  fb2::Fiber fb;
  unique_lock lk{err_mu_};
  fb.swap(err_handler_fb_);
  lk.unlock();
  fb.JoinIfNeeded();
}

GenericError ExecutionState::ReportErrorInternal(GenericError&& err) {
  if (IsCancelled()) {
    LOG_IF(INFO, err != errc::operation_canceled) << err.Format();
    return {};
  }
  lock_guard lk{err_mu_};
  if (err_)
    return err_;

  err_ = std::move(err);

  // This context is either new or was Reset, where the handler was joined
  CHECK(!err_handler_fb_.IsJoinable());

  LOG(WARNING) << "ReportError: " << err_.Format();

  // We can move err_handler_ because it should run at most once.
  if (err_handler_)
    err_handler_fb_ = fb2::Fiber("report_internal_error", std::move(err_handler_), err_);
  state_.store(State::ERROR, std::memory_order_relaxed);
  return err_;
}

std::ostream& operator<<(std::ostream& os, const GlobalState& state) {
  return os << GlobalStateName(state);
}

ThreadLocalMutex::ThreadLocalMutex() {
  shard_ = EngineShard::tlocal();
}

ThreadLocalMutex::~ThreadLocalMutex() {
  DCHECK_EQ(EngineShard::tlocal(), shard_);
}

void ThreadLocalMutex::lock() {
  if (ServerState::tlocal()->serialization_max_chunk_size != 0) {
    DCHECK_EQ(EngineShard::tlocal(), shard_);
    util::fb2::NoOpLock noop_lk_;
    if (locked_fiber_ != nullptr) {
      DCHECK(util::fb2::detail::FiberActive() != locked_fiber_);
    }
    cond_var_.wait(noop_lk_, [this]() { return !flag_; });
    flag_ = true;
    DCHECK_EQ(locked_fiber_, nullptr);
    locked_fiber_ = util::fb2::detail::FiberActive();
  }
}

void ThreadLocalMutex::unlock() {
  if (ServerState::tlocal()->serialization_max_chunk_size != 0) {
    DCHECK_EQ(EngineShard::tlocal(), shard_);
    flag_ = false;
    cond_var_.notify_one();
    locked_fiber_ = nullptr;
  }
}

BorrowedInterpreter::BorrowedInterpreter(Transaction* tx, ConnectionState* state) {
  // Ensure squashing ignores EVAL. We can't run on a stub context, because it doesn't have our
  // preborrowed interpreter (which can't be shared on multiple threads).
  CHECK(!state->squashing_info);

  if (auto borrowed = state->exec_info.preborrowed_interpreter; borrowed) {
    // Ensure a preborrowed interpreter is only set for an already running MULTI transaction.
    CHECK_EQ(state->exec_info.state, ConnectionState::ExecInfo::EXEC_RUNNING);

    interpreter_ = borrowed;
  } else {
    // A scheduled transaction occupies a place in the transaction queue and holds locks,
    // preventing other transactions from progressing. Blocking below can deadlock!
    CHECK(!tx->IsScheduled());

    interpreter_ = ServerState::tlocal()->BorrowInterpreter();
    owned_ = true;
  }
}

BorrowedInterpreter::~BorrowedInterpreter() {
  if (owned_)
    ServerState::tlocal()->ReturnInterpreter(interpreter_);
}

void LocalLatch::unlock() {
  DCHECK_GT(mutating_, 0u);
  --mutating_;
  if (mutating_ == 0) {
    cond_var_.notify_all();
  }
}

void LocalLatch::Wait() {
  util::fb2::NoOpLock noop_lk_;
  cond_var_.wait(noop_lk_, [this]() { return mutating_ == 0; });
}

}  // namespace dfly
