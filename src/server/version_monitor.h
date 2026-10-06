// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <string>
#include <utility>
#include <vector>

#include "util/fibers/fibers.h"
#include "util/fibers/pool.h"
#include "util/http/http_client.h"

namespace dfly {

class VersionMonitor {
 public:
  void Run(util::ProactorPool* proactor_pool);

  void Shutdown();

  using HeaderList = std::vector<std::pair<std::string_view, std::string>>;

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

  // Returns the anonymous deployment info headers sent with each version check.
  HeaderList BuildInfoHeaders() const;

  util::fb2::Fiber version_fiber_;
  util::fb2::Done monitor_ver_done_;

  // Static deployment info, collected once in Run().
  std::string platform_;  // User-Agent comment: "<arch>; <io backend>; <cloud>".
  HeaderList static_headers_;
};

}  // namespace dfly
