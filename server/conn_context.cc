// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/conn_context.h"

#include "server/dragonfly_connection.h"

namespace dfly {

ConnectionContext::ConnectionContext(::io::Sink* stream, Connection* owner)
    : ReplyBuilder(owner->protocol(), stream), owner_(owner) {
}

Protocol ConnectionContext::protocol() const {
  return owner_->protocol();
}

}  // namespace dfly
