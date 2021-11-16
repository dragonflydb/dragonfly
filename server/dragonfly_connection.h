// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include "util/connection.h"

namespace dfly {

class Service;

class Connection : public util::Connection {
 public:
  Connection(Service* service);
  ~Connection();

  using error_code = std::error_code;
  using ShutdownCb = std::function<void()>;
  using ShutdownHandle = unsigned;

  ShutdownHandle RegisterShutdownHook(ShutdownCb cb);
  void UnregisterShutdownHook(ShutdownHandle id);

 protected:
  void OnShutdown() override;

 private:
  void HandleRequests() final;

  void InputLoop(util::FiberSocketBase* peer);
  void DispatchFiber(util::FiberSocketBase* peer);

  Service* service_;

  struct Shutdown;
  std::unique_ptr<Shutdown> shutdown_;
};

}  // namespace dfly
