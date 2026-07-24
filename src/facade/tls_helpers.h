// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#ifdef DFLY_USE_SSL
#include <openssl/ssl.h>
#endif

#include <optional>
#include <string>

namespace facade {

enum class TlsContextRole { SERVER, CLIENT };

struct TlsCertInfo {
  std::string subject;     // Common name / subject DN.
  std::string issuer;      // Issuer DN.
  std::string not_before;  // Start of validity period (ASN1_TIME formatted).
  std::string not_after;   // End of validity period (ASN1_TIME formatted).
};

#ifdef DFLY_USE_SSL
// Extracts TLS certificate metadata from an already-loaded X509 certificate.
// Returns std::nullopt if the certificate is null or cannot be parsed.
std::optional<TlsCertInfo> ParseTlsCertInfo(const X509* cert);

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
