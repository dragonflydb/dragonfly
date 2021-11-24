// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include "server/reply_builder.h"
#include "server/common_types.h"

namespace dfly {

class Connection;
class EngineShardSet;
class CommandId;

class ConnectionContext : public ReplyBuilder {
 public:
  ConnectionContext(::io::Sink* stream, Connection* owner);

  // TODO: to introduce proper accessors.
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
