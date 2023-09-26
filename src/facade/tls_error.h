// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

namespace facade {

void PrintSSLError();

}

#define DFLY_SSL_CHECK(condition)               \
  if (!(condition)) {                           \
    LOG(ERROR) << "OpenSSL Error: " #condition; \
    PrintSSLError();                            \
    exit(17);                                   \
  }
