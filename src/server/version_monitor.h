// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include "util/fibers/fibers.h"
#include "util/fibers/pool.h"
#include "util/http/http_client.h"

namespace dfly {

class VersionMonitor {
 public:
  void Run(util::ProactorPool* proactor_pool);

  void Shutdown();

 private:
  struct SslDeleter {
    void operator()(SSL_CTX* ssl) {
      if (ssl) {
        util::http::TlsClient::FreeContext(ssl);
      }
    }
  };

  using SslPtr = std::unique_ptr<SSL_CTX, SslDeleter>;
  void RunTask(SslPtr);

  bool IsVersionOutdated(std::string_view remote, std::string_view current) const;

  util::fb2::Fiber version_fiber_;
  util::fb2::Done monitor_ver_done_;
};

}  // namespace dfly
