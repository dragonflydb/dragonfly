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
#include "facade/facade_types.h"

ABSL_FLAG(std::string, tls_cert_file, "", "cert file for tls connections");
ABSL_FLAG(std::string, tls_key_file, "", "key file for tls connections");
ABSL_FLAG(std::string, tls_ca_cert_file, "", "ca signed certificate to validate tls connections");
ABSL_FLAG(std::string, tls_ca_cert_dir, "",
          "ca signed certificates directory. Use c_rehash before, read description in "
          "https://www.openssl.org/docs/man3.0/man1/c_rehash.html");
ABSL_FLAG(std::string, tls_ciphers, "DEFAULT:!MEDIUM", "TLS ciphers configuration for tls1.2");
ABSL_FLAG(std::string, tls_cipher_suites, "", "TLS ciphers configuration for tls1.3");
ABSL_FLAG(bool, tls_prefer_server_ciphers, false,
          "If true, prefer server ciphers over client ciphers");
ABSL_FLAG(bool, tls_session_caching, false, "If true enables session caching and tickets");
ABSL_FLAG(size_t, tls_session_cache_size, 20 * 1024, "Size of the cache for tls sessions");
ABSL_FLAG(size_t, tls_session_cache_timeout, 300, "Timeout for each session/ticket");

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

  if (!GetFlag(FLAGS_tls_ciphers).empty()) {
    DFLY_SSL_CHECK(1 == SSL_CTX_set_cipher_list(ctx, GetFlag(FLAGS_tls_ciphers).c_str()));
  }

  // Relevant only for TLS 1.3 connections.
  if (!GetFlag(FLAGS_tls_cipher_suites).empty()) {
    SSL_CTX_set_ciphersuites(ctx, GetFlag(FLAGS_tls_cipher_suites).c_str());
  }

  SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);

  SSL_CTX_set_options(ctx, SSL_OP_DONT_INSERT_EMPTY_FRAGMENTS);

  SSL_CTX_set_verify(ctx, mask, NULL);

  DFLY_SSL_CHECK(1 == SSL_CTX_set_dh_auto(ctx, 1));

  if (GetFlag(FLAGS_tls_prefer_server_ciphers)) {
    SSL_CTX_set_options(ctx, SSL_OP_CIPHER_SERVER_PREFERENCE);
  }

  if (GetFlag(FLAGS_tls_session_caching)) {
    SSL_CTX_set_session_cache_mode(ctx, SSL_SESS_CACHE_SERVER);
    SSL_CTX_sess_set_cache_size(ctx, GetFlag(FLAGS_tls_session_cache_size));
    SSL_CTX_set_timeout(ctx, GetFlag(FLAGS_tls_session_cache_timeout));
    SSL_CTX_set_session_id_context(ctx, (const unsigned char*)"dragonfly", 9);
  }

  SSL_CTX_set_info_callback(ctx, [](const SSL* ssl, int where, int ret) {
    // When we skip the handshake we never reach this state.
    if (where & SSL_CB_HANDSHAKE_START) {
      ++tl_facade_stats->conn_stats.handshakes_started;
    }
    // When we skip the handshake, we never reach this state.
    if (where & SSL_CB_HANDSHAKE_DONE) {
      ++tl_facade_stats->conn_stats.handshakes_completed;
    }
  });

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
