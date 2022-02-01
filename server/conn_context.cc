// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/conn_context.h"

#include "base/logging.h"
#include "server/dragonfly_connection.h"

namespace dfly {

ConnectionContext::ConnectionContext(::io::Sink* stream, Connection* owner) : owner_(owner) {
  switch (owner->protocol()) {
    case Protocol::REDIS:
      rbuilder_.reset(new RedisReplyBuilder(stream));
      break;
    case Protocol::MEMCACHE:
      rbuilder_.reset(new MCReplyBuilder(stream));
      break;
  }
}

Protocol ConnectionContext::protocol() const {
  return owner_->protocol();
}

RedisReplyBuilder* ConnectionContext::operator->() {
  CHECK(Protocol::REDIS == owner_->protocol());
  RedisReplyBuilder* b = static_cast<RedisReplyBuilder*>(rbuilder_.get());
  return b;
}

}  // namespace dfly
