// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/acl/acl_log.h"

#include <chrono>
#include <iterator>

#include "base/flags.h"
#include "base/logging.h"
#include "facade/dragonfly_connection.h"
#include "server/conn_context.h"

ABSL_FLAG(uint32_t, acllog_max_len, 32,
          "Specify the number of log entries. Logs are kept locally for each thread "
          "and therefore the total number of entries are acllog_max_len * threads");

namespace dfly::acl {

AclLog::AclLog() : total_entries_allowed_(absl::GetFlag(FLAGS_acllog_max_len)) {
}

void AclLog::Add(const ConnectionContext& cntx, std::string object, Reason reason,
                 std::string tried_to_auth) {
  if (total_entries_allowed_ == 0) {
    return;
  }

  if (log_.size() == total_entries_allowed_) {
    log_.pop_back();
  }

  std::string username;
  // We can't use a conditional here because the result is the common type which is a const-ref
  if (tried_to_auth.empty()) {
    username = cntx.authed_username;
  } else {
    username = std::move(tried_to_auth);
  }

  std::string client_info = cntx.conn()->GetClientInfo();
  using clock = std::chrono::system_clock;
  LogEntry entry = {std::move(username), std::move(client_info), std::move(object), reason,
                    clock::now()};
  log_.push_front(std::move(entry));
}

void AclLog::Reset() {
  log_.clear();
}

AclLog::LogType AclLog::GetLog(size_t number_of_entries) const {
  auto start = log_.begin();
  auto end = log_.size() <= number_of_entries ? log_.end() : std::next(start, number_of_entries);
  return {start, end};
}

void AclLog::SetTotalEntries(size_t total_entries) {
  if (log_.size() > total_entries) {
    log_.erase(std::next(log_.begin(), total_entries), log_.end());
  }

  total_entries_allowed_ = total_entries;
}

}  // namespace dfly::acl
