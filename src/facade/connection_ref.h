// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <memory>

namespace facade {

class Connection;

// Weak reference to a connection, invalidated upon connection close.
// Used to dispatch async operations for the connection without worrying about pointer lifetime.
struct ConnectionRef {
 public:
  // Get residing thread of connection. Thread-safe.
  unsigned LastKnownThreadId() const {
    return last_known_thread_id_;
  }
  // Get pointer to connection if still valid, nullptr if expired.
  // Can only be called from connection's thread. Validity is guaranteed
  // only until the next suspension point.
  Connection* Get() const;

  // Returns true if the reference expired. Thread-safe.
  bool IsExpired() const;

  // Returns client id.Thread-safe.
  uint32_t GetClientId() const;

  bool operator<(const ConnectionRef& other) const;
  bool operator==(const ConnectionRef& other) const;

 private:
  friend class Connection;

  ConnectionRef(const std::shared_ptr<Connection>& ptr, unsigned thread_id, uint32_t client_id);

  std::weak_ptr<Connection> ptr_;
  unsigned last_known_thread_id_;
  uint32_t client_id_;
};

}  // namespace facade
