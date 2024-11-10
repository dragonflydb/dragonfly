// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/conn_context.h"

#include "absl/flags/internal/flag.h"
#include "base/flags.h"
#include "base/logging.h"
#include "facade/dragonfly_connection.h"
#include "facade/reply_builder.h"

namespace facade {

ConnectionContext::ConnectionContext(::io::Sink* stream, Connection* owner) : owner_(owner) {
  if (owner) {
    protocol_ = owner->protocol();
  }

  if (stream) {
    switch (protocol_) {
      case Protocol::NONE:
        LOG(DFATAL) << "Invalid protocol";
        break;
      case Protocol::REDIS: {
        rbuilder_.reset(new RedisReplyBuilder(stream));
        break;
      }
      case Protocol::MEMCACHE:
        rbuilder_.reset(new MCReplyBuilder(stream));
        break;
    }
  }

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
  return dfly::HeapSize(rbuilder_) + dfly::HeapSize(authed_username) + dfly::HeapSize(acl_commands);
}

}  // namespace facade
