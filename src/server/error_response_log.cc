// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/error_response_log.h"

#include "absl/strings/str_cat.h"
#include "base/flags.h"

ABSL_FLAG(size_t, error_response_log_max_len, 32,
          "Specify the number of log entries. Logs are kept locally for each thread "
          "and therefore the total number of entries are acllog_max_len * threads");

namespace dfly {

ErrorResponseLog::ErrorResponseLog()
    : total_entries_allowed_(absl::GetFlag(FLAGS_error_response_log_max_len)) {
}

void ErrorResponseLog::Add(std::string_view command, facade::CmdArgList args,
                           std::string_view reason, std::string_view kind) {
  if (logs_.size() == total_entries_allowed_) {
    logs_.pop_back();
  }

  LogEntry entry = "Command and args:";
  absl::StrAppend(&entry, " ", command);

  for (auto arg : args) {
    absl::StrAppend(&entry, " ", facade::ToSV(arg));
  }

  absl::StrAppend(&entry, " failed with reason: ", reason);
  if (!kind.empty()) {
    absl::StrAppend(&entry, " and error kind: ", kind);
  }

  logs_.push_front(std::move(entry));
}

void ErrorResponseLog::Clear() {
  logs_.clear();
}

std::deque<ErrorResponseLog::LogEntry> ErrorResponseLog::GetLogs() const {
  return logs_;
}
}  // namespace dfly
