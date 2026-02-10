// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/sync_primitives.h"

#include <absl/random/random.h>
#include <absl/strings/match.h>

#include <optional>

#include "base/flags.h"
#include "base/logging.h"
#include "server/engine_shard_set.h"
#include "server/server_state.h"
#include "util/fibers/proactor_base.h"

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

namespace {

// Thread-local cache with static linkage.
thread_local std::optional<LockTagOptions> locktag_lock_options;

}  // namespace

void TEST_InvalidateLockTagOptions() {
  locktag_lock_options = nullopt;  // For test main thread
  CHECK(shard_set != nullptr);
  shard_set->pool()->AwaitBrief(
      [](ShardId shard, util::ProactorBase* proactor) { locktag_lock_options = nullopt; });
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
