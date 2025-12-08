// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/coordinator.h"

#include "base/logging.h"
#include "facade/redis_parser.h"
#include "facade/socket_utils.h"
#include "server/cluster/cluster_config.h"

using namespace std;
using namespace facade;

namespace dfly::cluster {

class Coordinator::CrossShardRequest {
 public:
  CrossShardRequest(std::string cmd, Coordinator::RespCB cb, uint32_t total_shards)
      : command_(std::move(cmd)), cb_(std::move(cb)), shard_processed_(total_shards) {
  }

  const std::string& GetCommand() const {
    return command_;
  }

  template <class... Args> void Exec(Args&&... args) {
    cb_(std::forward<Args>(args)...);
    if (shard_processed_.fetch_sub(1, std::memory_order_relaxed) == 1) {
      future_.Resolve(GenericError{});
    }
  }

  util::fb2::Future<GenericError>& GetFuture() {
    return future_;
  }

 private:
  std::string command_;
  Coordinator::RespCB cb_;
  util::fb2::Future<GenericError> future_;
  std::atomic_uint32_t shard_processed_;
};

class Coordinator::CrossShardClient : public ProtocolClient {
 public:
  CrossShardClient(std::string host, uint16_t port) : ProtocolClient(std::move(host), port) {
  }

  using ProtocolClient::CloseSocket;
  ~CrossShardClient() {
    exec_st_.Cancel();
    waker_.notifyAll();
    CloseSocket();
    send_fb_.Join();
    resp_fb_.Join();
  }

  [[nodiscard]] bool Init() {
    VLOG(1) << "Resolving host DNS to " << server().Description();
    if (error_code ec = ResolveHostDns(); ec) {
      LOG(WARNING) << "Could not resolve host DNS to " << server().Description() << ": "
                   << ec.message();
      exec_st_.ReportError(GenericError(ec, "Could not resolve host dns."));
      return false;
    }
    VLOG(1) << "Start coordinator connection to " << server().Description();
    auto timeout = 3000ms;  // TODO add flag;
    if (auto ec = ConnectAndAuth(timeout, &exec_st_); ec) {
      LOG(WARNING) << "Couldn't connect to " << server().Description() << ": " << ec.message()
                   << ", socket state: " << GetSocketInfo(Sock()->native_handle());
      exec_st_.ReportError(GenericError(ec, "Couldn't connect to source."));
      return false;
    }

    ResetParser(RedisParser::Mode::CLIENT);
    send_fb_ = util::fb2::Fiber("CSS_SendFb", &CrossShardClient::SendFb, this);
    resp_fb_ = util::fb2::Fiber("CSS_RespFb", &CrossShardClient::RespFb, this);
    return true;
  }

  void Cancel() {
    exec_st_.Cancel();
    ShutdownSocket();
  }

  void EnqueueCommand(CrossShardRequestPtr req) {
    std::lock_guard lk(mu_);
    send_queue_.push(req);
    resp_queue_.push(req);
    ready_to_send_ = true;
    waker_.notifyAll();
  }

  void SendFb() {
    while (!exec_st_.IsCancelled()) {
      waker_.await([this] { return exec_st_.IsCancelled() || ready_to_send_; });
      if (exec_st_.IsCancelled())
        return;
      std::lock_guard lk(mu_);
      while (!send_queue_.empty()) {
        if (auto ec = ProtocolClient::SendCommand(send_queue_.front()->GetCommand()); ec) {
          exec_st_.ReportError(GenericError(
              ec, absl::StrCat("Coordinator could not send command to : ", server().Description(),
                               "socket state: ", GetSocketInfo(Sock()->native_handle()))));
          // TODO reinit connection.
          break;
        }
        send_queue_.pop();
      }
      ready_to_send_ = false;
    }
  }

  void RespFb() {
    while (!exec_st_.IsCancelled()) {
      waker_.await([this] { return exec_st_.IsCancelled() || ready_to_send_; });
      if (exec_st_.IsCancelled())
        return;
      std::lock_guard lk(mu_);
      constexpr auto timeout = 3000;  // TODO add flag and add usage in ReadRespReply.
      while (!resp_queue_.empty()) {
        auto resp = TakeRespReply(timeout);
        if (!resp) {
          LOG(WARNING) << "Error reading response from " << server().Description() << ": "
                       << resp.error()
                       << ", socket state: " + GetSocketInfo(Sock()->native_handle());

          // TODO make all requests fail in this case.
          // TODO reinit connection.
          LOG(FATAL) << "Coordinator RespFb read error, not implemented recovery yet.";
          break;
        }
        resp_queue_.front()->Exec(*resp);
        resp_queue_.pop();
      }
    }
  }

 private:
  std::queue<std::shared_ptr<CrossShardRequest>> send_queue_;
  std::queue<std::shared_ptr<CrossShardRequest>> resp_queue_;

  util::fb2::Fiber send_fb_;
  util::fb2::Fiber resp_fb_;
  util::fb2::EventCount waker_;

  mutable util::fb2::Mutex mu_;
  std::atomic_bool ready_to_send_ = false;
};

Coordinator& Coordinator::Current() {
  static Coordinator instance;
  return instance;
}

std::shared_ptr<Coordinator::CrossShardClient> Coordinator::GetClient(const std::string& host,
                                                                      uint16_t port) {
  for (const auto& client : clients_) {
    if (client->GetHost() == host && client->GetPort() == port) {
      return client;
    }
  }
  auto new_client = std::make_shared<CrossShardClient>(host, port);
  if (new_client->Init()) {
    clients_.emplace_back(new_client);
    return new_client;
  }
  return nullptr;
}

util::fb2::Future<GenericError> Coordinator::DispatchAll(std::string command, RespCB cb) {
  auto cluster_config = ClusterConfig::Current();
  if (!cluster_config) {
    VLOG(2) << "No cluster config found for coordinator plan creation.";
    LOG(FATAL) << "No cluster config, not implemented logic yet.";
    return {};
  }
  VLOG(2) << "Dispatching command to all shards: " << command;
  auto shards_config = cluster_config->GetConfig();

  auto shard_request = std::make_shared<CrossShardRequest>(std::move(command), std::move(cb),
                                                           shards_config.size() - 1);

  for (const auto& shard : shards_config) {
    if (shard.master.id == cluster_config->MyId()) {
      continue;
    }
    const auto& client = GetClient(shard.master.ip, shard.master.port);
    if (!client) {
      VLOG(1) << "Could not get coordinator client for " << shard.master.ip << ":"
              << shard.master.port;
      cb({});  // TODO add error propagation.
      LOG(FATAL) << "No error processing, not implemented logic yet.";
      return {};
    }
    client->EnqueueCommand(shard_request);
  }
  return shard_request->GetFuture();
}

}  // namespace dfly::cluster
