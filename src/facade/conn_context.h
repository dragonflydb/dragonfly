// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_set.h>

#include <string_view>

namespace facade {

class Connection;

class ConnectionContext {
 public:
  explicit ConnectionContext(Connection* owner) : owner_(owner) {
    conn_closing = false;
    req_auth = false;
    replica_conn = false;
    authenticated = false;
    async_dispatch = false;
    sync_dispatch = false;
    paused = false;
    blocked = false;

    subscriptions = 0;
  }

  virtual ~ConnectionContext() {
  }

  Connection* conn() {
    return owner_;
  }

  const Connection* conn() const {
    return owner_;
  }

  virtual size_t UsedMemory() const {
    return 0;
  }

  // Noop.
  virtual void Unsubscribe(std::string_view channel) {
  }

  virtual void OnSocketError(uint32_t epoll_mask){};

  // connection state / properties.
  bool conn_closing : 1;
  bool req_auth : 1;
  bool replica_conn : 1;  // whether it's a replica connection on the master side.
  bool authenticated : 1;

  // Dispatch-state flags Clarification: their names come from the V1 producer/consumer model:
  //   sync_dispatch  - the producer (I/O) fiber is running a command inline (in its context).
  //   async_dispatch - the consumer (AsyncFiber) is running a command / admin message.
  //
  // For both V1+V2 loops - together, the two flags serve two purposes.:
  //   1. Inflight-dispatch tracking: IsCurrentlyDispatching() == (sync_dispatch || async_dispatch).
  //      DispatchTracker reads it so CLIENT PAUSE / REPLTAKEOVER / cluster migration / shutdown
  //      wait for an in-flight command to finish.
  //   2. Flush-Before-Block: async_dispatch=true (only) makes MainService::DispatchCommand flush
  //      buffered pipeline replies before a blocking command (e.g. BLPOP) parks the fiber.
  //
  // V2 note:
  // - These flags are on-spot markers raised around a single call, NOT a durable "this connection
  //   is executing" state.
  // - "Executing" is broader than running the command body: a V2 async command's reply is written
  //   later, when its suspended reply coroutine is resumed. ReplyBatch keeps parsed_head_ on the
  //   command until after SendReply() returns, so that reply-write window is covered by
  //   HasInFlightCommands() rather than by these flags.
  // - A V2 async command clears the flags as soon as the dispatch returns, while it may still be
  //   in flight (its reply not yet written).
  // - The only durable in-flight signal there is Connection::HasInFlightCommands().
  // - Because the flags are read cross-fiber (DispatchTracker) but only at a fiber suspension
  //   point, IsCurrentlyDispatching() may be transiently false mid-dispatch without any observer
  //   seeing the gap.
  bool async_dispatch : 1;
  bool sync_dispatch : 1;

  bool paused = false;  // whether this connection is paused due to CLIENT PAUSE
  // whether it's blocked on blocking commands like BLPOP, needs to be addressable
  bool blocked = false;

  // How many async subscription sources are active: monitor and/or pubsub - at most 2.
  uint8_t subscriptions;

 private:
  Connection* owner_;
};

}  // namespace facade
