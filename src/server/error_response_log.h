// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <deque>
#include <string>
#include <vector>

#include "facade/facade_types.h"

namespace dfly {

class ErrorResponseLog {
 public:
  ErrorResponseLog();

  using LogEntry = std::string;

  void Add(std::string_view command, facade::CmdArgList args, std::string_view reason,
           std::string_view kind);

  void Clear();

  std::deque<LogEntry> GetLogs() const;

 private:
  std::deque<LogEntry> logs_;
  size_t total_entries_allowed_;
};

}  // namespace dfly
