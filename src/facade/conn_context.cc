// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/conn_context.h"

#include "absl/flags/internal/flag.h"
#include "base/flags.h"
#include "base/logging.h"
#include "facade/dragonfly_connection.h"
#include "facade/reply_builder.h"

ABSL_RETIRED_FLAG(bool, experimental_new_io, true,
                  "Use new replying code - should "
                  "reduce latencies for pipelining");  // TODO remove in 1/2/25

namespace facade {
ConnectionContext::ConnectionContext(Connection* owner) : owner_(owner) {
  conn_closing = false;
  req_auth = false;
  replica_conn = false;
  authenticated = false;
  async_dispatch = false;
  sync_dispatch = false;
  journal_emulated = false;
  paused = false;
  blocked = false;

  subscriptions = 0;
}

size_t ConnectionContext::UsedMemory() const {
  return dfly::HeapSize(authed_username) + dfly::HeapSize(acl_commands);
}

}  // namespace facade
