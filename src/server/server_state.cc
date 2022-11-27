// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/server_state.h"

#include "base/logging.h"
#include "server/conn_context.h"

namespace dfly {

void MonitorsRepo::Add(ConnectionContext* connection) {
  VLOG(1) << "register connection "
          << " at address 0x" << std::hex << (const void*)connection << " for thread "
          << util::ProactorBase::GetIndex();

  monitors_.push_back(connection);
}

void MonitorsRepo::Send(const std::string& msg) {
  if (!monitors_.empty()) {
    VLOG(1) << "thread " << util::ProactorBase::GetIndex() << " sending monitor message '" << msg
            << "' for " << monitors_.size();
    for (auto monitor_conn : monitors_) {
      monitor_conn->SendMonitorMsg(msg);
    }
  }
}

void MonitorsRepo::Remove(const ConnectionContext* conn) {
  auto it = std::find_if(monitors_.begin(), monitors_.end(),
                         [&conn](const auto& val) { return val == conn; });
  if (it != monitors_.end()) {
    VLOG(1) << "removing connection 0x" << std::hex << (const void*)conn << " releasing token";
    monitors_.erase(it);
  } else {
    VLOG(1) << "no connection 0x" << std::hex << (const void*)conn
            << " found in the registered list here";
  }
}

void MonitorsRepo::NotifyChangeCount(bool added) {
  if (added) {
    ++global_count_;
  } else {
    DCHECK(global_count_ > 0);
    --global_count_;
  }
}

}  // end of namespace dfly
