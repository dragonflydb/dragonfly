// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/dragonfly_listener.h"

#ifdef DFLY_USE_SSL
#include <openssl/ssl.h>
#endif

#include "base/flags.h"
#include "base/logging.h"
#include "facade/dragonfly_connection.h"
#include "facade/service_interface.h"
#include "util/proactor_pool.h"

using namespace std;

ABSL_FLAG(uint32_t, conn_threads, 0, "Number of threads used for handing server connections");
ABSL_FLAG(bool, tls, false, "");
ABSL_FLAG(bool, conn_use_incoming_cpu, false,
          "If true uses incoming cpu of a socket in order to distribute"
          " incoming connections");

ABSL_FLAG(string, tls_client_cert_file, "", "cert file for tls connections");
ABSL_FLAG(string, tls_client_key_file, "", "key file for tls connections");

#if 0
enum TlsClientAuth {
  CL_AUTH_NO = 0,
  CL_AUTH_YES = 1,
  CL_AUTH_OPTIONAL = 2,
};

facade::ConfigEnum tls_auth_clients_enum[] = {
    {"no", CL_AUTH_NO},
    {"yes", CL_AUTH_YES},
    {"optional", CL_AUTH_OPTIONAL},
};

static int tls_auth_clients_opt = CL_AUTH_YES;

CONFIG_enum(tls_auth_clients, "yes", "", tls_auth_clients_enum, tls_auth_clients_opt);
#endif

namespace facade {

using namespace util;
using absl::GetFlag;

namespace {

#ifdef DFLY_USE_SSL
// To connect: openssl s_client  -cipher "ADH:@SECLEVEL=0" -state -crlf  -connect 127.0.0.1:6380
static SSL_CTX* CreateSslCntx() {
  SSL_CTX* ctx = SSL_CTX_new(TLS_server_method());
  const auto& tls_client_key_file = GetFlag(FLAGS_tls_client_key_file);
  if (tls_client_key_file.empty()) {
    // To connect - use openssl s_client -cipher with either:
    // "AECDH:@SECLEVEL=0" or "ADH:@SECLEVEL=0" setting.
    CHECK_EQ(1, SSL_CTX_set_cipher_list(ctx, "aNULL"));

    // To allow anonymous ciphers.
    SSL_CTX_set_security_level(ctx, 0);

    // you can still connect with redis-cli with :
    // redis-cli --tls --insecure --tls-ciphers "ADH:@SECLEVEL=0"
    LOG(WARNING)
        << "tls-client-key-file not set, no keys are loaded and anonymous ciphers are enabled. "
        << "Do not use in production!";
  } else {  // tls_client_key_file is set.
    CHECK_EQ(1, SSL_CTX_use_PrivateKey_file(ctx, tls_client_key_file.c_str(), SSL_FILETYPE_PEM));
    const auto& tls_client_cert_file = GetFlag(FLAGS_tls_client_cert_file);

    if (!tls_client_cert_file.empty()) {
      // TO connect with redis-cli you need both tls-client-key-file and tls-client-cert-file
      // loaded. Use `redis-cli --tls -p 6380 --insecure  PING` to test

      CHECK_EQ(1, SSL_CTX_use_certificate_chain_file(ctx, tls_client_cert_file.c_str()));
    }
    CHECK_EQ(1, SSL_CTX_set_cipher_list(ctx, "DEFAULT"));
  }
  SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);

  SSL_CTX_set_options(ctx, SSL_OP_DONT_INSERT_EMPTY_FRAGMENTS);

  unsigned mask = SSL_VERIFY_NONE;

  // if (tls_auth_clients_opt)
  // mask |= SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
  SSL_CTX_set_verify(ctx, mask, NULL);

  CHECK_EQ(1, SSL_CTX_set_dh_auto(ctx, 1));

  return ctx;
}
#endif

bool ConfigureKeepAlive(int fd, unsigned interval_sec) {
  DCHECK_GT(interval_sec, 3u);

  int val = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &val, sizeof(val)) < 0)
    return false;

  val = interval_sec;
  if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &val, sizeof(val)) < 0)
    return false;

  /* Send next probes after the specified interval. Note that we set the
   * delay as interval / 3, as we send three probes before detecting
   * an error (see the next setsockopt call). */
  val = interval_sec / 3;
  if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &val, sizeof(val)) < 0)
    return false;

  /* Consider the socket in error state after three we send three ACK
   * probes without getting a reply. */
  val = 3;
  if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &val, sizeof(val)) < 0)
    return false;

  return true;
}

}  // namespace

Listener::Listener(Protocol protocol, ServiceInterface* si) : service_(si), protocol_(protocol) {

#ifdef DFLY_USE_SSL
  if (GetFlag(FLAGS_tls)) {
    OPENSSL_init_ssl(OPENSSL_INIT_SSL_DEFAULT, NULL);
    ctx_ = CreateSslCntx();
  }
#endif

  http_base_.reset(new HttpListener<>);
  http_base_->set_resource_prefix("http://static.dragonflydb.io/data-plane");
  si->ConfigureHttpHandlers(http_base_.get());
}

Listener::~Listener() {
#ifdef DFLY_USE_SSL
  SSL_CTX_free(ctx_);
#endif
}

util::Connection* Listener::NewConnection(ProactorBase* proactor) {
  return new Connection{protocol_, http_base_.get(), ctx_, service_};
}

error_code Listener::ConfigureServerSocket(int fd) {
  int val = 1;
  constexpr int kInterval = 300;  // 300 seconds is ok to start checking for liveness.

  if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val)) < 0) {
    LOG(WARNING) << "Could not set reuse addr on socket " << detail::SafeErrorMessage(errno);
  }
  bool success = ConfigureKeepAlive(fd, kInterval);

  LOG_IF(WARNING, !success) << "Could not configure keep alive " << detail::SafeErrorMessage(errno);

  return error_code{};
}

void Listener::PreShutdown() {
}

void Listener::PostShutdown() {
}

// We can limit number of threads handling dragonfly connections.
ProactorBase* Listener::PickConnectionProactor(LinuxSocketBase* sock) {
  util::ProactorPool* pp = pool();
  uint32_t total = GetFlag(FLAGS_conn_threads);
  uint32_t id = kuint32max;

  if (total == 0 || total > pp->size()) {
    total = pp->size();
  }

  if (GetFlag(FLAGS_conn_use_incoming_cpu)) {
    int fd = sock->native_handle();

    int cpu, napi_id;
    socklen_t len = sizeof(cpu);

    CHECK_EQ(0, getsockopt(fd, SOL_SOCKET, SO_INCOMING_CPU, &cpu, &len));
    CHECK_EQ(0, getsockopt(fd, SOL_SOCKET, SO_INCOMING_NAPI_ID, &napi_id, &len));
    VLOG(1) << "CPU/NAPI for connection " << fd << " is " << cpu << "/" << napi_id;

    vector<unsigned> ids = pool()->MapCpuToThreads(cpu);
    if (!ids.empty()) {
      id = ids.front();
    }
  }

  if (id == kuint32max) {
    id = next_id_.fetch_add(1, std::memory_order_relaxed);
  }

  return pp->at(id % total);
}

}  // namespace facade
