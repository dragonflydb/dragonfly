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
#include "redis/util.h"
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
#include "strings/human_readable.h"

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

atomic_uint64_t used_mem_peak(0);
atomic_uint64_t used_mem_current(0);
atomic_uint64_t rss_mem_current(0);
atomic_uint64_t rss_mem_peak(0);

unsigned kernel_version = 0;
size_t max_memory_limit = 0;
Namespaces* namespaces = nullptr;

size_t FetchRssMemory(io::StatusData sdata) {
  return sdata.vm_rss + sdata.hugetlb_pages;
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

bool ParseHumanReadableBytes(std::string_view str, int64_t* num_bytes) {
  if (str.empty())
    return false;

  const char* cstr = str.data();
  bool neg = (*cstr == '-');
  if (neg) {
    cstr++;
  }
  char* end;
  double d = strtod(cstr, &end);

  if (end == cstr)  // did not succeed to advance
    return false;

  int64 scale = 1;
  switch (*end) {
    // Considers just the first character after the number
    // so it matches: 1G, 1GB, 1GiB and 1Gigabytes
    // NB: an int64 can only go up to <8 EB.
    case 'E':
    case 'e':
      scale <<= 10;  // Fall through...
      ABSL_FALLTHROUGH_INTENDED;
    case 'P':
    case 'p':
      scale <<= 10;
      ABSL_FALLTHROUGH_INTENDED;
    case 'T':
    case 't':
      scale <<= 10;
      ABSL_FALLTHROUGH_INTENDED;
    case 'G':
    case 'g':
      scale <<= 10;
      ABSL_FALLTHROUGH_INTENDED;
    case 'M':
    case 'm':
      scale <<= 10;
      ABSL_FALLTHROUGH_INTENDED;
    case 'K':
    case 'k':
      scale <<= 10;
      ABSL_FALLTHROUGH_INTENDED;
    case 'B':
    case 'b':
    case '\0':
      break;  // To here.
    default:
      return false;
  }
  d *= scale;
  if (int64_t(d) > kint64max || d < 0)
    return false;

  *num_bytes = static_cast<int64>(d + 0.5);
  if (neg) {
    *num_bytes = -*num_bytes;
  }
  return true;
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
  static_assert(sizeof(TieredStats) == 144);

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
  return *this;
}

SearchStats& SearchStats::operator+=(const SearchStats& o) {
  static_assert(sizeof(SearchStats) == 24);
  ADD(used_memory);
  ADD(num_entries);

  DCHECK(num_indices == 0 || num_indices == o.num_indices);
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
      else if (scan_opts.limit > 4096)
        scan_opts.limit = 4096;
    } else if (opt == "MATCH") {
      string_view pattern = ArgS(args, i + 1);
      if (pattern != "*")
        scan_opts.pattern = pattern;
    } else if (opt == "TYPE") {
      auto obj_type = ObjTypeFromString(ArgS(args, i + 1));
      if (!obj_type) {
        return facade::OpStatus::SYNTAX_ERR;
      }
      scan_opts.type_filter = obj_type;
    } else if (opt == "BUCKET") {
      if (!absl::SimpleAtoi(ArgS(args, i + 1), &scan_opts.bucket_id)) {
        return facade::OpStatus::INVALID_INT;
      }
    } else {
      return facade::OpStatus::SYNTAX_ERR;
    }
  }
  return scan_opts;
}

bool ScanOpts::Matches(std::string_view val_name) const {
  if (!pattern)
    return true;
  return stringmatchlen(pattern->data(), pattern->size(), val_name.data(), val_name.size(), 0) == 1;
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

Context::~Context() {
  DCHECK(!err_handler_fb_.IsJoinable());
  err_handler_fb_.JoinIfNeeded();
}

GenericError Context::GetError() const {
  std::lock_guard lk(err_mu_);
  return err_;
}

const Cancellation* Context::GetCancellation() const {
  return this;
}

void Context::Cancel() {
  ReportError(std::make_error_code(errc::operation_canceled), "Context cancelled");
}

void Context::Reset(ErrHandler handler) {
  fb2::Fiber fb;

  unique_lock lk{err_mu_};
  err_ = {};
  err_handler_ = std::move(handler);
  Cancellation::flag_.store(false, std::memory_order_relaxed);
  fb.swap(err_handler_fb_);
  lk.unlock();
  fb.JoinIfNeeded();
}

GenericError Context::SwitchErrorHandler(ErrHandler handler) {
  std::lock_guard lk{err_mu_};
  if (!err_) {
    // No need to check for the error handler - it can't be running
    // if no error is set.
    err_handler_ = std::move(handler);
  }
  return err_;
}

void Context::JoinErrorHandler() {
  fb2::Fiber fb;
  unique_lock lk{err_mu_};
  fb.swap(err_handler_fb_);
  lk.unlock();
  fb.JoinIfNeeded();
}

GenericError Context::ReportErrorInternal(GenericError&& err) {
  lock_guard lk{err_mu_};
  if (err_)
    return err_;

  err_ = std::move(err);

  // This context is either new or was Reset, where the handler was joined
  CHECK(!err_handler_fb_.IsJoinable());

  DVLOG(1) << "ReportError: " << err_.Format();

  // We can move err_handler_ because it should run at most once.
  if (err_handler_)
    err_handler_fb_ = fb2::Fiber("report_internal_error", std::move(err_handler_), err_);
  Cancellation::Cancel();
  return err_;
}

bool AbslParseFlag(std::string_view in, dfly::MemoryBytesFlag* flag, std::string* err) {
  int64_t val;
  if (dfly::ParseHumanReadableBytes(in, &val) && val >= 0) {
    flag->value = val;
    return true;
  }

  *err = "Use human-readable format, eg.: 500MB, 1G, 1TB";
  return false;
}

std::string AbslUnparseFlag(const dfly::MemoryBytesFlag& flag) {
  return strings::HumanReadableNumBytes(flag.value);
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

void LocalBlockingCounter::unlock() {
  DCHECK(mutating_ > 0);
  --mutating_;
  if (mutating_ == 0) {
    cond_var_.notify_all();
  }
}

void LocalBlockingCounter::Wait() {
  util::fb2::NoOpLock noop_lk_;
  cond_var_.wait(noop_lk_, [this]() { return mutating_ == 0; });
}

}  // namespace dfly
