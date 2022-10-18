// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/server_state.h"

#include "base/logging.h"
#include "server/conn_context.h"

namespace dfly {

MonitorsRepo::MonitorInfo::MonitorInfo(ConnectionContext* conn)
    : connection(conn), token(conn->conn_state.monitor->borrow_token),
      thread_id(util::ProactorBase::GetIndex()) {
}

void MonitorsRepo::MonitorInfo::Send(std::string_view msg, std::uint32_t tid,
                                     util::fibers_ext::BlockingCounter borrows) {
  if (tid == thread_id && connection) {
    VLOG(1) << "thread " << tid << " sending monitor message '" << msg << "'";
    token.Inc();
    connection->SendMonitorMsg(msg, borrows);
    VLOG(1) << "thread " << tid << " successfully finish to send the message";
  }
}

void MonitorsRepo::Add(const MonitorInfo& info) {
  VLOG(1) << "register connection "
          << " at address 0x" << std::hex << (const void*)info.connection << " for thread "
          << info.thread_id;

  monitors_.push_back(info);
}

void MonitorsRepo::Send(std::string_view msg, util::fibers_ext::BlockingCounter borrows,
                        std::uint32_t tid) {
  std::for_each(monitors_.begin(), monitors_.end(),
                [msg, tid, borrows](auto& monitor_conn) { monitor_conn.Send(msg, tid, borrows); });
}

void MonitorsRepo::Release(std::uint32_t tid) {
  std::for_each(monitors_.begin(), monitors_.end(), [tid](auto& monitor_conn) {
    if (monitor_conn.thread_id == tid) {
      VLOG(1) << "thread " << tid << " releasing token";
      monitor_conn.token.Dec();
    }
  });
}

void MonitorsRepo::Remove(const ConnectionContext* conn) {
  auto it = std::find_if(monitors_.begin(), monitors_.end(),
                         [&conn](const auto& val) { return val.connection == conn; });
  if (it != monitors_.end()) {
    VLOG(1) << "removing connection 0x" << std::hex << (const void*)conn << " releasing token";
    monitors_.erase(it);
  } else {
    VLOG(1) << "no connection 0x" << std::hex << (const void*)conn
            << " found in the registered list here";
  }
}

}  // end of namespace dfly
