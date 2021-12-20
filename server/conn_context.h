// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/common_types.h"
#include "server/reply_builder.h"

namespace dfly {

class Connection;
class EngineShardSet;
class CommandId;

struct ConnectionState {
  enum Mask : uint32_t {
    ASYNC_DISPATCH = 1,  // whether a command is handled via async dispatch.
    CONN_CLOSING = 2,    // could be because of unrecoverable error or planned action.
  };

  uint32_t mask = 0;  // A bitmask of Mask values.

  bool IsClosing() const {
    return mask & CONN_CLOSING;
  }

  bool IsRunViaDispatch() const {
    return mask & ASYNC_DISPATCH;
  }
};

class ConnectionContext : public ReplyBuilder {
 public:
  ConnectionContext(::io::Sink* stream, Connection* owner);

  // TODO: to introduce proper accessors.
  Transaction* transaction = nullptr;
  const CommandId* cid = nullptr;
  EngineShardSet* shard_set = nullptr;

  Connection* owner() {
    return owner_;
  }

  Protocol protocol() const;

  ConnectionState conn_state;

 private:
  Connection* owner_;
};

}  // namespace dfly
