// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <chrono>
#include <deque>
#include <string>

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
    TimePoint entry_creation;
  };

  void Add(std::string username, std::string client_info, std::string object, Reason reason);
  void Reset();

  using LogType = std::deque<LogEntry>;

  LogType GetLog() const;

 private:
  LogType log_;
  const size_t total_entries_allowed_;
};

}  // namespace dfly::acl
