// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <boost/fiber/condition_variable.hpp>
#include <boost/fiber/fiber.hpp>
#include <optional>
#include <string_view>

#include "server/common.h"
#include "util/uring/uring_file.h"

namespace dfly {
namespace journal {

class JournalShard {
 public:
  JournalShard();
  ~JournalShard();

  std::error_code Open(const std::string_view dir, unsigned index);

  std::error_code Close();

  LSN cur_lsn() const {
    return lsn_;
  }

  std::error_code status() const {
    return status_ec_;
  }

  bool IsOpen() const {
    return bool(shard_file_);
  }

  void AddLogRecord(TxId txid, unsigned opcode);

 private:
  std::string shard_path_;
  std::unique_ptr<util::uring::LinuxFile> shard_file_;

  size_t file_offset_ = 0;
  LSN lsn_ = 1;

  unsigned shard_index_ = -1;

  std::error_code status_ec_;

  bool lameduck_ = false;
};

}  // namespace journal
}  // namespace dfly
