// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <chrono>
#include <deque>
#include <string>

namespace dfly {

class ConnectionContext;

namespace acl {

class AclLog {
 public:
  explicit AclLog();

  enum class Reason { COMMAND, AUTH, KEY, PUB_SUB };

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

  void SetTotalEntries(size_t total_entries);

 private:
  LogType log_;
  size_t total_entries_allowed_;
};

}  // namespace acl
}  // namespace dfly
