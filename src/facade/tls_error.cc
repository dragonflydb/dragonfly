#include "facade/tls_error.h"

#include <openssl/err.h>

#include <string_view>

#include "base/logging.h"

#ifdef DFLY_USE_SSL

void facade::PrintSSLError() {
  ERR_print_errors_cb(
      [](const char* str, size_t len, void* u) {
        LOG(ERROR) << std::string_view(str, len);
        return 1;
      },
      nullptr);
}

#else

void facade::PrintSSLError() {
}

#endif
