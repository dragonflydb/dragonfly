// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
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
