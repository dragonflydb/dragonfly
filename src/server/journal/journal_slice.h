// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "base/ring_buffer.h"
#include "core/fibers.h"
#include "core/uring.h"
#include "server/common.h"
#include "server/journal/types.h"
#include "util/fibers/synchronization.h"

namespace util {
namespace fb2 {
class LinuxFile;
}  // namespace fb2
}  // namespace util

namespace dfly {
namespace journal {

// Journal slice is present for both shards and io threads.
class JournalSlice {
 public:
  JournalSlice();
  ~JournalSlice();

  void Init(unsigned index);

  std::error_code Open(std::string_view dir);

  std::error_code Close();

  // This is always the LSN of the *next* journal entry.
  LSN cur_lsn() const {
    return lsn_;
  }

  std::error_code status() const {
    return status_ec_;
  }

  // Whether the file-based journaling is open.
  bool IsOpen() const {
    return bool(shard_file_);
  }

  void AddLogRecord(const Entry& entry, bool await);

  uint32_t RegisterOnChange(ChangeCallback cb);
  void UnregisterOnChange(uint32_t);
  bool HasRegisteredCallbacks() const {
    std::shared_lock lk(cb_mu_);
    return !change_cb_arr_.empty();
  }

 private:
  struct RingItem;

  std::string shard_path_;
  std::unique_ptr<LinuxFile> shard_file_;
  std::optional<base::RingBuffer<RingItem>> ring_buffer_;

  mutable util::SharedMutex cb_mu_;
  std::vector<std::pair<uint32_t, ChangeCallback>> change_cb_arr_ ABSL_GUARDED_BY(cb_mu_);

  size_t file_offset_ = 0;
  LSN lsn_ = 1;

  uint32_t slice_index_ = UINT32_MAX;
  uint32_t next_cb_id_ = 1;
  std::error_code status_ec_;

  bool lameduck_ = false;
};

}  // namespace journal
}  // namespace dfly
