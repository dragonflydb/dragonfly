// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "tls_helpers.h"

#include <openssl/err.h>

#ifdef DFLY_USE_SSL
#include <openssl/ssl.h>
#endif

#include <string>

#include "absl/functional/bind_front.h"
#include "base/flags.h"
#include "base/logging.h"

ABSL_FLAG(std::string, tls_cert_file, "", "cert file for tls connections");
ABSL_FLAG(std::string, tls_key_file, "", "key file for tls connections");
ABSL_FLAG(std::string, tls_ca_cert_file, "", "ca signed certificate to validate tls connections");
ABSL_FLAG(std::string, tls_ca_cert_dir, "",
          "ca signed certificates directory. Use c_rehash before, read description in "
          "https://www.openssl.org/docs/man3.0/man1/c_rehash.html");

namespace facade {

#ifdef DFLY_USE_SSL

// Creates the TLS context. Returns nullptr if the TLS configuration is invalid.
// To connect: openssl s_client -state -crlf -connect 127.0.0.1:6380
SSL_CTX* CreateSslCntx(TlsContextRole role) {
  using absl::GetFlag;
  const auto& tls_key_file = GetFlag(FLAGS_tls_key_file);
  if (tls_key_file.empty()) {
    LOG(ERROR) << "To use TLS, a server certificate must be provided with the --tls_key_file flag!";
    return nullptr;
  }

  SSL_CTX* ctx;

  if (role == TlsContextRole::SERVER) {
    ctx = SSL_CTX_new(TLS_server_method());
  } else {
    ctx = SSL_CTX_new(TLS_client_method());
  }
  unsigned mask = SSL_VERIFY_NONE;

  if (SSL_CTX_use_PrivateKey_file(ctx, tls_key_file.c_str(), SSL_FILETYPE_PEM) != 1) {
    LOG(ERROR) << "Failed to load TLS key";
    return nullptr;
  }
  const auto& tls_cert_file = GetFlag(FLAGS_tls_cert_file);

  if (!tls_cert_file.empty()) {
    // TO connect with redis-cli you need both tls-key-file and tls-cert-file
    // loaded. Use `redis-cli --tls -p 6380 --insecure  PING` to test
    if (SSL_CTX_use_certificate_chain_file(ctx, tls_cert_file.c_str()) != 1) {
      LOG(ERROR) << "Failed to load TLS certificate";
      return nullptr;
    }
  }

  const auto tls_ca_cert_file = GetFlag(FLAGS_tls_ca_cert_file);
  const auto tls_ca_cert_dir = GetFlag(FLAGS_tls_ca_cert_dir);
  if (!tls_ca_cert_file.empty() || !tls_ca_cert_dir.empty()) {
    const auto* file = tls_ca_cert_file.empty() ? nullptr : tls_ca_cert_file.data();
    const auto* dir = tls_ca_cert_dir.empty() ? nullptr : tls_ca_cert_dir.data();
    if (SSL_CTX_load_verify_locations(ctx, file, dir) != 1) {
      LOG(ERROR) << "Failed to load TLS verify locations (CA cert file or CA cert dir)";
      return nullptr;
    }
    mask = SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
  }

  DFLY_SSL_CHECK(1 == SSL_CTX_set_cipher_list(ctx, "DEFAULT"));

  SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);

  SSL_CTX_set_options(ctx, SSL_OP_DONT_INSERT_EMPTY_FRAGMENTS);

  SSL_CTX_set_verify(ctx, mask, NULL);

  DFLY_SSL_CHECK(1 == SSL_CTX_set_dh_auto(ctx, 1));

  return ctx;
}

void PrintSSLError() {
  ERR_print_errors_cb(
      [](const char* str, size_t len, void* u) {
        LOG(ERROR) << std::string_view(str, len);
        return 1;
      },
      nullptr);
}

#endif
}  // namespace facade
