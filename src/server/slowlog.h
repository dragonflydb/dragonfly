// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <boost/circular_buffer.hpp>
#include <string>
#include <vector>

#include "base/integral_types.h"
#include "server/common.h"
namespace dfly {

constexpr size_t kMaximumSlowlogArgCount = 31;  // 32 - 1 for the command name
constexpr size_t kMaximumSlowlogArgLength = 128;

struct SlowLogEntry {
  uint32_t entry_id;
  uint64_t unix_timestamp;
  uint64_t execution_time_micro;
  size_t original_length;
  // a vector of pairs of argument and extra bytes if the argument was truncated
  std::vector<std::pair<std::string, uint32_t>> cmd_args;
  std::string client_ip;
  std::string client_name;
};

class SlowLogShard {
 public:
  SlowLogShard(){};

  boost::circular_buffer<SlowLogEntry>& Entries() {
    return log_entries_;
  }

  void Add(const std::string_view command_name, CmdArgList args, const std::string_view client_name,
           const std::string_view client_ip, uint64_t execution_time, uint64_t unix_timestamp);
  void Reset();
  void ChangeLength(size_t new_length);
  size_t Length() const;

 private:
  uint32_t slowlog_entry_id_ = 0;

  // TODO: to replace with base::RingBuffer because circular_buffer does not seem to support
  // move semantics.
  boost::circular_buffer<SlowLogEntry> log_entries_;
};
}  // namespace dfly
