// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/common.h"

#include <absl/random/random.h>
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
#include "core/glob_matcher.h"
#include "core/interpreter.h"
#include "facade/cmd_arg_parser.h"
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

OpResult<ScanOpts> ScanOpts::TryFrom(CmdArgList args, bool allow_novalues) {
  ScanOpts scan_opts;
  facade::CmdArgParser parser(args);

  while (parser.HasNext()) {
    std::string_view pattern;
    std::string_view type_str;

    if (parser.Check("NOVALUES")) {
      if (!allow_novalues) {
        return facade::OpStatus::SYNTAX_ERR;
      }
      scan_opts.novalues = true;
    } else if (parser.Check("COUNT", &scan_opts.limit)) {
      if (scan_opts.limit == 0)
        scan_opts.limit = 1;
    } else if (parser.Check("MATCH", &pattern)) {
      if (pattern != "*")
        scan_opts.matcher.reset(new GlobMatcher{pattern, true});
    } else if (parser.Check("TYPE", &type_str)) {
      CompactObjType obj_type = ObjTypeFromString(type_str);
      if (obj_type == kInvalidCompactObjType) {
        return facade::OpStatus::SYNTAX_ERR;
      }
      scan_opts.type_filter = obj_type;
    } else if (parser.Check("BUCKET", &scan_opts.bucket_id)) {
      // no-op
    } else if (parser.Check("ATTR")) {
      scan_opts.mask =
          parser.MapNext("v", ScanOpts::Mask::Volatile, "p", ScanOpts::Mask::Permanent, "a",
                         ScanOpts::Mask::Accessed, "u", ScanOpts::Mask::Untouched);
    } else if (parser.Check("MINMSZ", &scan_opts.min_malloc_size)) {
      // no-op
    } else
      return facade::OpStatus::SYNTAX_ERR;
  }  // while

  // Check for parsing errors (e.g. missing values or invalid integers)
  if (auto err = parser.TakeError()) {
    if (err.type == facade::CmdArgParser::INVALID_INT) {
      return facade::OpStatus::INVALID_INT;
    }
    return facade::OpStatus::SYNTAX_ERR;
  }

  return scan_opts;
}

bool ScanOpts::Matches(std::string_view val_name) const {
  return !matcher || matcher->Matches(val_name);
}

std::ostream& operator<<(std::ostream& os, const GlobalState& state) {
  return os << GlobalStateName(state);
}

ScanOpts::~ScanOpts() {
}

BorrowedInterpreter::BorrowedInterpreter(Transaction* tx, ConnectionState* state) {
  // Ensure squashing ignores EVAL. We can't run on a stub context, because it doesn't have our
  // preborrowed interpreter (which can't be shared on multiple threads).
  CHECK(!tx->IsSquashedStub());

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

}  // namespace dfly
