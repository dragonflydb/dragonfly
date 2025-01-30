// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#ifdef DFLY_USE_SSL
#include <openssl/ssl.h>
#endif

namespace facade {

#ifdef DFLY_USE_SSL
enum class TlsContextRole { SERVER, CLIENT };

SSL_CTX* CreateSslCntx(TlsContextRole role);

void PrintSSLError();

#define DFLY_SSL_CHECK(condition)               \
  if (!(condition)) {                           \
    LOG(ERROR) << "OpenSSL Error: " #condition; \
    PrintSSLError();                            \
    exit(17);                                   \
  }

#endif

}  // namespace facade
