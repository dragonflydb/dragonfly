// Copyright 2022, DragonflyDB authors.  All rights reserved.
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

ABSL_FLAG(string, tls_cert_file, "", "cert file for tls connections");
ABSL_FLAG(string, tls_key_file, "", "key file for tls connections");

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
using util::detail::SafeErrorMessage;

using absl::GetFlag;

namespace {

#ifdef DFLY_USE_SSL
// To connect: openssl s_client  -cipher "ADH:@SECLEVEL=0" -state -crlf  -connect 127.0.0.1:6380
static SSL_CTX* CreateSslCntx() {
  SSL_CTX* ctx = SSL_CTX_new(TLS_server_method());
  const auto& tls_key_file = GetFlag(FLAGS_tls_key_file);
  if (tls_key_file.empty()) {
    // To connect - use openssl s_client -cipher with either:
    // "AECDH:@SECLEVEL=0" or "ADH:@SECLEVEL=0" setting.
    CHECK_EQ(1, SSL_CTX_set_cipher_list(ctx, "aNULL"));

    // To allow anonymous ciphers.
    SSL_CTX_set_security_level(ctx, 0);

    // you can still connect with redis-cli with :
    // redis-cli --tls --insecure --tls-ciphers "ADH:@SECLEVEL=0"
    LOG(WARNING) << "tls-key-file not set, no keys are loaded and anonymous ciphers are enabled. "
                 << "Do not use in production!";
  } else {  // tls_key_file is set.
    CHECK_EQ(1, SSL_CTX_use_PrivateKey_file(ctx, tls_key_file.c_str(), SSL_FILETYPE_PEM));
    const auto& tls_cert_file = GetFlag(FLAGS_tls_cert_file);

    if (!tls_cert_file.empty()) {
      // TO connect with redis-cli you need both tls-key-file and tls-cert-file
      // loaded. Use `redis-cli --tls -p 6380 --insecure  PING` to test

      CHECK_EQ(1, SSL_CTX_use_certificate_chain_file(ctx, tls_cert_file.c_str()));
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

namespace detail {

void CntMinHeap::Inc(unsigned key) {
  DCHECK_LT(key, cnts_.size());

  unsigned prev = cnts_[key]++;

  // Update min_key if needed.
  // Note - it could be converted into a log(n) operation by using a sorted map but I do not
  // see any point in doing that when our cnt_ is usually small.
  if (key == min_key_) {
    for (unsigned i = 0; i < cnts_.size(); ++i) {
      if (cnts_[i] <= prev) {
        min_key_ = i;
        break;
      }
    }
  }
}

void CntMinHeap::Dec(unsigned key) {
  DCHECK_LT(key, cnts_.size());
  unsigned cur = --cnts_[key];
  if (cur < MinCnt()) {
    min_key_ = key;
  }
}

}  // namespace detail

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
    LOG(WARNING) << "Could not set reuse addr on socket " << SafeErrorMessage(errno);
  }
  bool success = ConfigureKeepAlive(fd, kInterval);

  if (!success) {
    int myerr = errno;

    int socket_type;
    socklen_t length = sizeof(socket_type);

    // Ignore the error on UDS.
    if (getsockopt(fd, SOL_SOCKET, SO_DOMAIN, &socket_type, &length) != 0 ||
        socket_type != AF_UNIX) {
      LOG(WARNING) << "Could not configure keep alive " << SafeErrorMessage(myerr);
    }
  }

  return error_code{};
}

void Listener::PreAcceptLoop(util::ProactorBase* pb) {
  cnt_heap_.Init(pool()->size());
}

void Listener::PreShutdown() {
}

void Listener::PostShutdown() {
}

void Listener::OnConnectionStart(util::Connection* conn) {
  facade::Connection* facade_conn = static_cast<facade::Connection*>(conn);
  VLOG(1) << "Opening connection " << facade_conn->GetClientId();

  conn_cnt_.fetch_add(1, std::memory_order_relaxed);
}

void Listener::OnConnectionClose(util::Connection* conn) {
  // TODO: We do not account for connections migrated to other threads. This is a rare case.
  unsigned id = conn->socket()->proactor()->GetIndex();

  facade::Connection* facade_conn = static_cast<facade::Connection*>(conn);
  VLOG(1) << "Closing connection " << facade_conn->GetClientId();

  conn_cnt_.fetch_sub(1, std::memory_order_relaxed);

  absl::base_internal::SpinLockHolder lock{&mutex_};
  cnt_heap_.Dec(id);
}

// We can limit number of threads handling dragonfly connections.
ProactorBase* Listener::PickConnectionProactor(LinuxSocketBase* sock) {
  util::ProactorPool* pp = pool();
  uint32_t total = GetFlag(FLAGS_conn_threads);
  uint32_t res_id = kuint32max;

  if (total == 0 || total > pp->size()) {
    total = pp->size();
  }

  if (!sock->IsUDS()) {
    int fd = sock->native_handle();

    int cpu, napi_id;
    socklen_t len = sizeof(cpu);

    // I suspect that the advantage of using SO_INCOMING_NAPI_ID is that
    // we can also track the affinity changes during the lifetime of the process
    // i.e. when a different CPU is assigned to handle the RX traffic.
    CHECK_EQ(0, getsockopt(fd, SOL_SOCKET, SO_INCOMING_CPU, &cpu, &len));
    CHECK_EQ(0, getsockopt(fd, SOL_SOCKET, SO_INCOMING_NAPI_ID, &napi_id, &len));
    VLOG(1) << "CPU/NAPI for connection " << fd << " is " << cpu << "/" << napi_id;

    if (GetFlag(FLAGS_conn_use_incoming_cpu)) {
      const vector<unsigned>& ids = pool()->MapCpuToThreads(cpu);

      absl::base_internal::SpinLockHolder lock{&mutex_};
      unsigned cnt_cap = cnt_heap_.MinCnt();

      // Allow 20% or 5 more connections for affinity-thread than another
      // thread with smallest amount of connections.
      cnt_cap = max<unsigned>(cnt_cap + 5, cnt_cap * 1.2);

      for (auto id : ids) {
        if (cnt_heap_[id] < cnt_cap) {
          VLOG(1) << "using thread " << id << " for cpu " << cpu;
          res_id = id;
          break;
        }
      }

      if (res_id == kuint32max) {
        VLOG(1) << "choosing a thread with minimum conns " << cnt_heap_.MinKey() << " instead of "
                << cpu;
        res_id = cnt_heap_.MinKey();
      }
      cnt_heap_.Inc(res_id);
    }
  }

  if (res_id == kuint32max) {
    res_id = next_id_.fetch_add(1, std::memory_order_relaxed) % total;
  }

  return pp->at(res_id);
}

}  // namespace facade
