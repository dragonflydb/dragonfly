// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <optional>
#include <string_view>

#include "base/ring_buffer.h"
#include "core/uring.h"
#include "server/common.h"
#include "server/journal/types.h"

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
    return !change_cb_arr_.empty();
  }

 private:
  struct RingItem;

  std::string shard_path_;
  std::unique_ptr<LinuxFile> shard_file_;
  std::optional<base::RingBuffer<RingItem>> ring_buffer_;

  util::SharedMutex cb_mu_;
  std::vector<std::pair<uint32_t, ChangeCallback>> change_cb_arr_;

  size_t file_offset_ = 0;
  LSN lsn_ = 1;

  uint32_t slice_index_ = UINT32_MAX;
  uint32_t next_cb_id_ = 1;
  std::error_code status_ec_;

  bool lameduck_ = false;
};

}  // namespace journal
}  // namespace dfly
