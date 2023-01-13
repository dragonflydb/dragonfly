// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/common.h"

#include <absl/strings/charconv.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <mimalloc.h>

#include <system_error>

extern "C" {
#include "redis/object.h"
#include "redis/rdb.h"
#include "redis/util.h"
#include "redis/zmalloc.h"
}
#include "base/logging.h"
#include "core/compact_object.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/journal/journal.h"
#include "server/server_state.h"

namespace dfly {

using namespace std;

thread_local ServerState ServerState::state_;

atomic_uint64_t used_mem_peak(0);
atomic_uint64_t used_mem_current(0);
unsigned kernel_version = 0;
size_t max_memory_limit = 0;

ServerState::ServerState() {
  CHECK(mi_heap_get_backing() == mi_heap_get_default());

  mi_heap_t* tlh = mi_heap_new();
  init_zmalloc_threadlocal(tlh);
  data_heap_ = tlh;
}

ServerState::~ServerState() {
}

void ServerState::Init() {
  gstate_ = GlobalState::ACTIVE;
}

void ServerState::Shutdown() {
  gstate_ = GlobalState::SHUTTING_DOWN;
  interpreter_.reset();
}

Interpreter& ServerState::GetInterpreter() {
  if (!interpreter_) {
    interpreter_.emplace();
  }

  return interpreter_.value();
}

const char* GlobalStateName(GlobalState s) {
  switch (s) {
    case GlobalState::ACTIVE:
      return "ACTIVE";
    case GlobalState::LOADING:
      return "LOADING";
    case GlobalState::SAVING:
      return "SAVING";
    case GlobalState::SHUTTING_DOWN:
      return "SHUTTING DOWN";
  }
  ABSL_INTERNAL_UNREACHABLE;
}

const char* ObjTypeName(int type) {
  switch (type) {
    case OBJ_STRING:
      return "string";
    case OBJ_LIST:
      return "list";
    case OBJ_SET:
      return "set";
    case OBJ_ZSET:
      return "zset";
    case OBJ_HASH:
      return "hash";
    case OBJ_STREAM:
      return "stream";
    case OBJ_JSON:
      return "ReJSON-RL";
    default:
      LOG(ERROR) << "Unsupported type " << type;
  }
  return "invalid";
};

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
    absl::from_chars_result result = absl::from_chars(src.data(), src.end(), *value);
    if (int(result.ec) != 0 || result.ptr != src.end() || isnan(*value))
      return false;
  }
  return true;
}

void OpArgs::RecordJournal(string_view key, ArgSlice args) const {
  auto journal = shard->journal();
  CHECK(journal);
  journal->RecordEntry(txid, journal::Op::COMMAND, db_cntx.db_index, 1, make_pair(key, args));
}

#define ADD(x) (x) += o.x

TieredStats& TieredStats::operator+=(const TieredStats& o) {
  static_assert(sizeof(TieredStats) == 48);

  ADD(tiered_reads);
  ADD(tiered_writes);
  ADD(storage_capacity);
  ADD(storage_reserved);
  ADD(aborted_write_cnt);
  ADD(flush_skip_cnt);

  return *this;
}

OpResult<ScanOpts> ScanOpts::TryFrom(CmdArgList args) {
  ScanOpts scan_opts;

  for (unsigned i = 0; i < args.size(); i += 2) {
    ToUpper(&args[i]);
    string_view opt = ArgS(args, i);
    if (i + 1 == args.size()) {
      return facade::OpStatus::SYNTAX_ERR;
    }

    if (opt == "COUNT") {
      if (!absl::SimpleAtoi(ArgS(args, i + 1), &scan_opts.limit)) {
        return facade::OpStatus::INVALID_INT;
      }
      if (scan_opts.limit == 0)
        scan_opts.limit = 1;
      else if (scan_opts.limit > 4096)
        scan_opts.limit = 4096;
    } else if (opt == "MATCH") {
      scan_opts.pattern = ArgS(args, i + 1);
      if (scan_opts.pattern == "*")
        scan_opts.pattern = string_view{};
    } else if (opt == "TYPE") {
      ToLower(&args[i + 1]);
      scan_opts.type_filter = ArgS(args, i + 1);
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
  if (pattern.empty())
    return true;
  return stringmatchlen(pattern.data(), pattern.size(), val_name.data(), val_name.size(), 0) == 1;
}

GenericError::operator std::error_code() const {
  return ec_;
}

GenericError::operator bool() const {
  return bool(ec_);
}

std::string GenericError::Format() const {
  if (!ec_)
    return "";

  if (details_.empty())
    return ec_.message();
  else
    return absl::StrCat(ec_.message(), ":", details_);
}

Context::~Context() {
  JoinErrorHandler();
}

GenericError Context::GetError() {
  std::lock_guard lk(mu_);
  return err_;
}

const Cancellation* Context::GetCancellation() const {
  return this;
}

void Context::Cancel() {
  ReportError(std::make_error_code(errc::operation_canceled), "Context cancelled");
}

void Context::Reset(ErrHandler handler) {
  std::lock_guard lk{mu_};
  JoinErrorHandler();
  err_ = {};
  err_handler_ = std::move(handler);
  Cancellation::flag_.store(false, std::memory_order_relaxed);
}

GenericError Context::SwitchErrorHandler(ErrHandler handler) {
  std::lock_guard lk{mu_};
  if (!err_) {
    // No need to check for the error handler - it can't be running
    // if no error is set.
    err_handler_ = std::move(handler);
  }
  return err_;
}

void Context::JoinErrorHandler() {
  if (err_handler_fb_.IsJoinable())
    err_handler_fb_.Join();
}

GenericError Context::ReportErrorInternal(GenericError&& err) {
  std::lock_guard lk{mu_};
  if (err_)
    return err_;
  err_ = std::move(err);

  // This context is either new or was Reset, where the handler was joined
  CHECK(!err_handler_fb_.IsJoinable());

  if (err_handler_)
    err_handler_fb_ = util::fibers_ext::Fiber{err_handler_, err_};

  Cancellation::Cancel();
  return err_;
}

}  // namespace dfly
