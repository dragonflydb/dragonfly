// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <chrono>
#include <deque>
#include <string>

#include "base/flags.h"
#include "server/conn_context.h"

ABSL_DECLARE_FLAG(size_t, acllog_max_len);

namespace dfly::acl {

class AclLog {
 public:
  explicit AclLog();

  enum class Reason { COMMAND, AUTH };

  struct LogEntry {
    std::string username;
    std::string client_info;
    std::string object;
    Reason reason;
    using TimePoint = std::chrono::time_point<std::chrono::system_clock>;
    TimePoint entry_creation = TimePoint::max();

    friend bool operator<(const LogEntry& lhs, const LogEntry& rhs) {
      return lhs.entry_creation < rhs.entry_creation;
    }
  };

  void Add(const ConnectionContext& cntx, std::string object, Reason reason,
           std::string tried_to_auth = "");
  void Reset();

  using LogType = std::deque<LogEntry>;

  LogType GetLog(size_t number_of_entries) const;

  void SetTotalEntries(size_t total_entries) {
    total_entries_allowed_.store(total_entries, std::memory_order_relaxed);
  }

 private:
  LogType log_;
  std::atomic<size_t> total_entries_allowed_;
};

}  // namespace dfly::acl
