// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/conn_context.h"

#include "absl/flags/internal/flag.h"
#include "base/flags.h"
#include "facade/dragonfly_connection.h"
#include "facade/reply_builder.h"

ABSL_FLAG(bool, experimental_new_io, true,
          "Use new replying code - should "
          "reduce latencies for pipelining");

namespace facade {

ConnectionContext::ConnectionContext(::io::Sink* stream, Connection* owner) : owner_(owner) {
  if (owner) {
    protocol_ = owner->protocol();
  }

  if (stream) {
    switch (protocol_) {
      case Protocol::REDIS: {
        RedisReplyBuilder* rb = absl::GetFlag(FLAGS_experimental_new_io)
                                    ? new RedisReplyBuilder2(stream)
                                    : new RedisReplyBuilder(stream);
        rbuilder_.reset(rb);
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

void ConnectionContext::SendError(std::string_view str, std::string_view type) {
  rbuilder_->SendError(str, type);
}

void ConnectionContext::SendError(ErrorReply error) {
  rbuilder_->SendError(error);
}

void ConnectionContext::SendError(OpStatus status) {
  rbuilder_->SendError(status);
}

}  // namespace facade
