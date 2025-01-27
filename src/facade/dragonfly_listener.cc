// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/dragonfly_listener.h"

#include <openssl/err.h>

#include <memory>

#include "absl/functional/bind_front.h"
#include "facade/tls_error.h"

#ifdef DFLY_USE_SSL
#include <openssl/ssl.h>
#endif
#include "base/flags.h"
#include "base/logging.h"
#include "facade/dragonfly_connection.h"
#include "facade/service_interface.h"
#include "util/proactor_pool.h"

using namespace std;

ABSL_FLAG(uint32_t, conn_io_threads, 0, "Number of threads used for handing server connections");
ABSL_FLAG(uint32_t, conn_io_thread_start, 0, "Starting thread id for handling server connections");
ABSL_FLAG(bool, tls, false, "");
ABSL_FLAG(bool, conn_use_incoming_cpu, false,
          "If true uses incoming cpu of a socket in order to distribute"
          " incoming connections");

ABSL_FLAG(string, tls_cert_file, "", "cert file for tls connections");
ABSL_FLAG(string, tls_key_file, "", "key file for tls connections");
ABSL_FLAG(string, tls_ca_cert_file, "", "ca signed certificate to validate tls connections");
ABSL_FLAG(string, tls_ca_cert_dir, "",
          "ca signed certificates directory. Use c_rehash before, read description in "
          "https://www.openssl.org/docs/man3.0/man1/c_rehash.html");
ABSL_FLAG(uint32_t, tcp_keepalive, 300,
          "the period in seconds of inactivity after which keep-alives are triggerred,"
          "the duration until an inactive connection is terminated is twice the specified time");
ABSL_FLAG(uint32_t, tcp_user_timeout, 0,
          "the maximum period in milliseconds that transimitted data may stay unacknowledged "
          "before TCP aborts the connection. 0 means OS default timeout");

ABSL_DECLARE_FLAG(bool, primary_port_http_enabled);

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

// Creates the TLS context. Returns nullptr if the TLS configuration is invalid.
// To connect: openssl s_client -state -crlf -connect 127.0.0.1:6380
SSL_CTX* CreateSslServerCntx() {
  const auto& tls_key_file = GetFlag(FLAGS_tls_key_file);
  if (tls_key_file.empty()) {
    LOG(ERROR) << "To use TLS, a server certificate must be provided with the --tls_key_file flag!";
    return nullptr;
  }

  SSL_CTX* ctx = SSL_CTX_new(TLS_server_method());
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
#endif

bool ConfigureKeepAlive(int fd) {
  int val = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &val, sizeof(val)) < 0)
    return false;

  val = absl::GetFlag(FLAGS_tcp_keepalive);
#ifdef __APPLE__
  if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPALIVE, &val, sizeof(val)) < 0)
    return false;
#else
  if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &val, sizeof(val)) < 0)
    return false;
#endif

  /* Send next probes after the specified interval. Note that we set the
   * delay as interval / 3, as we send three probes before detecting
   * an error (see the next setsockopt call). */
  val = std::max(val / 3, 1);
  if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &val, sizeof(val)) < 0)
    return false;

  /* Consider the socket in error state after three we send three ACK
   * probes without getting a reply. */
  val = 3;
  if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &val, sizeof(val)) < 0)
    return false;

  return true;
}

struct ListenerStats {
  size_t tls_allocated_bytes = 0;
  uint64_t refused_conn_maxclients_reached_cnt = 0;
};

thread_local ListenerStats listener_tl_stats;
atomic_int ssl_init_refcount = 0;

void* OverriddenSSLMalloc(size_t size, const char* file, int line) {
  void* res = mi_malloc(size);
  listener_tl_stats.tls_allocated_bytes += mi_malloc_usable_size(res);
  return res;
}

void* OverriddenSSLRealloc(void* addr, size_t size, const char* file, int line) {
  size_t prev_size = mi_malloc_usable_size(addr);
  void* res = mi_realloc(addr, size);
  listener_tl_stats.tls_allocated_bytes += mi_malloc_usable_size(res);
  listener_tl_stats.tls_allocated_bytes -= prev_size;
  return res;
}

void OverriddenSSLFree(void* addr, const char* file, int line) {
  listener_tl_stats.tls_allocated_bytes -= mi_malloc_usable_size(addr);
  mi_free(addr);
}

}  // namespace

Listener::Listener(Protocol protocol, ServiceInterface* si, Role role)
    : service_(si), protocol_(protocol) {
#ifdef DFLY_USE_SSL
  if (ssl_init_refcount.fetch_add(1) == 0) {
    CRYPTO_set_mem_functions(&OverriddenSSLMalloc, &OverriddenSSLRealloc, &OverriddenSSLFree);
  }

  // Always initialise OpenSSL so we can enable TLS at runtime.
  OPENSSL_init_ssl(OPENSSL_INIT_SSL_DEFAULT, nullptr);
  if (!ReconfigureTLS()) {
    exit(-1);
  }
#endif
  role_ = role;
  // We only set the HTTP interface for:
  // 1. Privileged users (on privileged listener)
  // 2. Main listener (if enabled)
  const bool is_main_enabled = GetFlag(FLAGS_primary_port_http_enabled);
  if (IsPrivilegedInterface() || (IsMainInterface() && is_main_enabled)) {
    http_base_ = std::make_unique<HttpListener<>>();
    http_base_->set_resource_prefix("http://static.dragonflydb.io/data-plane");
    si->ConfigureHttpHandlers(http_base_.get(), IsPrivilegedInterface());
  }
}

Listener::~Listener() {
#ifdef DFLY_USE_SSL
  SSL_CTX_free(ctx_);

  if (ssl_init_refcount.fetch_sub(1) == 1) {
    OPENSSL_cleanup();
  }
#endif
}

util::Connection* Listener::NewConnection(ProactorBase* proactor) {
  return new Connection{protocol_, http_base_.get(), ctx_, service_};
}

error_code Listener::ConfigureServerSocket(int fd) {
  int val = 1;

  if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val)) < 0) {
    LOG(WARNING) << "Could not set reuse addr on socket " << SafeErrorMessage(errno);
  }
  bool success = ConfigureKeepAlive(fd);

#ifdef __linux__
  int user_timeout = absl::GetFlag(FLAGS_tcp_user_timeout);
  if (setsockopt(fd, IPPROTO_TCP, TCP_USER_TIMEOUT, &user_timeout, sizeof(int)) < 0) {
    LOG(WARNING) << "Could not set user timeout on socket " << SafeErrorMessage(errno);
  }
#endif

  if (!success) {
#ifndef __APPLE__
    int myerr = errno;

    int socket_type;
    socklen_t length = sizeof(socket_type);

    // Ignore the error on UDS.
    if (getsockopt(fd, SOL_SOCKET, SO_DOMAIN, &socket_type, &length) != 0 ||
        socket_type != AF_UNIX) {
      LOG(WARNING) << "Could not configure keep alive " << SafeErrorMessage(myerr);
    }
#endif
  }

  return error_code{};
}

bool Listener::ReconfigureTLS() {
  SSL_CTX* prev_ctx = ctx_;
  if (GetFlag(FLAGS_tls)) {
    SSL_CTX* ctx = CreateSslServerCntx();
    if (!ctx) {
      return false;
    }
    ctx_ = ctx;
  } else {
    ctx_ = nullptr;
  }

  if (prev_ctx) {
    // SSL_CTX is reference counted so if other connections have a reference
    // to the context it won't be freed yet.
    SSL_CTX_free(prev_ctx);
  }

  return true;
}

size_t Listener::TLSUsedMemoryThreadLocal() {
  return listener_tl_stats.tls_allocated_bytes;
}

uint64_t Listener::RefusedConnectionMaxClientsCount() {
  return listener_tl_stats.refused_conn_maxclients_reached_cnt;
}

void Listener::PreAcceptLoop(util::ProactorBase* pb) {
}

bool Listener::IsPrivilegedInterface() const {
  return role_ == Role::PRIVILEGED;
}

bool Listener::IsMainInterface() const {
  return role_ == Role::MAIN;
}

void Listener::PreShutdown() {
  // Iterate on all connections and allow them to finish their commands for
  // a short period.
  // Executed commands can be visible in snapshots or replicas, but if we close the client
  // connections too fast we might not send the acknowledgment for those commands.
  // This shouldn't take a long time: All clients should reject incoming commands
  // at this stage since we're in SHUTDOWN mode.
  // If a command is running for too long we give up and proceed.
  DispatchTracker tracker{{this}};
  tracker.TrackAll();

  if (!tracker.Wait(absl::Milliseconds(10))) {
    LOG(WARNING) << "Some commands are still being dispatched but didn't conclude in time. "
                    "Proceeding in shutdown.";
  }
}

void Listener::PostShutdown() {
}

void Listener::OnConnectionStart(util::Connection* conn) {
  facade::Connection* facade_conn = static_cast<facade::Connection*>(conn);
  VLOG(1) << "Opening connection " << facade_conn->GetClientId();

  facade_conn->OnConnectionStart();
}

void Listener::OnConnectionClose(util::Connection* conn) {
  Connection* facade_conn = static_cast<Connection*>(conn);
  VLOG(1) << "Closing connection " << facade_conn->GetClientId();
}

void Listener::OnMaxConnectionsReached(util::FiberSocketBase* sock) {
  listener_tl_stats.refused_conn_maxclients_reached_cnt++;
  sock->Write(io::Buffer("-ERR max number of clients reached\r\n"));
}

// We can limit number of threads handling dragonfly connections.
ProactorBase* Listener::PickConnectionProactor(util::FiberSocketBase* sock) {
  util::ProactorPool* pp = pool();

  uint32_t res_id = kuint32max;

  if (!sock->IsUDS()) {
    int fd = sock->native_handle();

    int cpu, napi_id;
    socklen_t len = sizeof(cpu);

    // I suspect that the advantage of using SO_INCOMING_NAPI_ID is that
    // we can also track the affinity changes during the lifetime of the process
    // i.e. when a different CPU is assigned to handle the RX traffic.
    // On some distributions (WSL1, for example), SO_INCOMING_CPU is not supported.
    if (0 == getsockopt(fd, SOL_SOCKET, SO_INCOMING_CPU, &cpu, &len)) {
      VLOG(1) << "CPU for connection " << fd << " is " << cpu;
      // Avoid CHECKINGing success, it sometimes fail on WSL
      // https://github.com/dragonflydb/dragonfly/issues/2090
      if (0 == getsockopt(fd, SOL_SOCKET, SO_INCOMING_NAPI_ID, &napi_id, &len)) {
        VLOG(1) << "NAPI for connection " << fd << " is " << napi_id;
      }

      if (GetFlag(FLAGS_conn_use_incoming_cpu)) {
        // We choose a thread that is running on the incoming CPU. Usually there is
        // a single thread per CPU. SO_INCOMING_CPU returns the CPU that the kernel
        // uses to steer the packets to. In order to make
        // conn_use_incoming_cpu effective, we should make sure that the receive packets are
        // steered to enough CPUs. This can be done by setting the RPS mask in
        // /sys/class/net/<dev>/queues/rx-<n>/rps_cpus. For more details, see
        // https://docs.kernel.org/networking/scaling.html#rps-configuration
        // Please note that if conn_use_incoming_cpu is true, connections will be handled only
        // on the CPUs that handle the softirqs for the incoming packets.
        // To avoid imbalance in CPU load, RPS tuning is strongly advised.
        const vector<unsigned>& ids = pool()->MapCpuToThreads(cpu);
        if (!ids.empty()) {
          res_id = ids[0];
        }
      }
    }
  }

  if (res_id == kuint32max) {
    uint32_t total = GetFlag(FLAGS_conn_io_threads);
    uint32_t start = GetFlag(FLAGS_conn_io_thread_start) % pp->size();

    if (total == 0 || total + start > pp->size()) {
      total = pp->size() - start;
    }

    res_id = start + (next_id_.fetch_add(1, std::memory_order_relaxed) % total);
  }

  return pp->at(res_id);
}

DispatchTracker::DispatchTracker(absl::Span<facade::Listener* const> listeners,
                                 facade::Connection* issuer, bool ignore_paused,
                                 bool ignore_blocked)
    : listeners_{listeners.begin(), listeners.end()},
      issuer_{issuer},
      ignore_paused_{ignore_paused},
      ignore_blocked_{ignore_blocked} {
}

void DispatchTracker::TrackOnThread() {
  for (auto* listener : listeners_) {
    listener->TraverseConnectionsOnThread(
        [this](unsigned thread_index, util::Connection* conn) { Handle(thread_index, conn); },
        UINT32_MAX, nullptr);
  }
}

bool DispatchTracker::Wait(absl::Duration duration) {
  bool res = bc_->WaitFor(absl::ToChronoMilliseconds(duration));
  if (!res && ignore_blocked_) {
    // We track all connections again because a connection might became blocked between the time
    // we call tracking the last time.
    bc_ = BlockingCounter{0};
    TrackAll();
    res = bc_->WaitFor(absl::ToChronoMilliseconds(duration));
  }
  return res;
}

void DispatchTracker::TrackAll() {
  for (auto* listener : listeners_)
    listener->TraverseConnections(absl::bind_front(&DispatchTracker::Handle, this));
}

void DispatchTracker::Handle(unsigned thread_index, util::Connection* conn) {
  if (auto* fconn = static_cast<facade::Connection*>(conn); fconn != issuer_)
    fconn->SendCheckpoint(bc_, ignore_paused_, ignore_blocked_);
}

}  // namespace facade
