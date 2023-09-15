// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/acl/acl_log.h"

#include <chrono>

#include "absl/flags/internal/flag.h"
#include "base/flags.h"
#include "base/logging.h"

ABSL_FLAG(size_t, acllog_max_len, 32,
          "Specify the number of log entries. Logs are kept locally for each proactor "
          "and therefore the total number of entries are acllog_max_len * proactors");

namespace dfly::acl {

AclLog::AclLog() : total_entries_allowed_(absl::GetFlag(FLAGS_acllog_max_len)) {
}

void AclLog::Add(std::string username, std::string client_info, std::string object, Reason reason) {
  if (total_entries_allowed_ == 0) {
    return;
  }

  if (log_.size() == total_entries_allowed_) {
    log_.pop_back();
  }

  using clock = std::chrono::system_clock;
  LogEntry entry = {std::move(username), std::move(client_info), std::move(object), reason,
                    clock::now()};
  log_.push_front(std::move(entry));
}

void AclLog::Reset() {
  log_.clear();
}

AclLog::LogType AclLog::GetLog() const {
  return log_;
}

}  // namespace dfly::acl
