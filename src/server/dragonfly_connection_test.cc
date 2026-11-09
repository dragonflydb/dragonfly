// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "facade/dragonfly_connection.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/match.h>
#include <absl/strings/str_split.h>
#include <arpa/inet.h>
#include <gmock/gmock.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <deque>
#include <optional>
#include <string>
#include <string_view>
#include <thread>

#include "base/flags.h"
#include "facade/dragonfly_listener.h"
#include "facade/facade_test.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/test_utils.h"
#include "util/accept_server.h"

ABSL_DECLARE_FLAG(bool, enable_resp_io_loop_v2);
ABSL_DECLARE_FLAG(bool, enable_shared_read_buffer);
ABSL_DECLARE_FLAG(bool, enable_pipeline_squashing_v2);
ABSL_DECLARE_FLAG(bool, multi_exec_squash);

namespace dfly {
namespace {

using namespace std::chrono_literals;
using testing::HasSubstr;
using testing::Optional;
using testing::StartsWith;

// Some if the tests in this test suite must send malformed RESP bytes and verify the ordered raw
// replies followed by EOF. We won't use ProtocolClient (protocol_client.cc) here - it does not
// expose its raw socket. Keep this minimal loopback RawRespClient client local and simple rather
// than expanding ProtocolClient for test-only behavior.
class RawRespClient {
 public:
  explicit RawRespClient(uint16_t port) {
    fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0)
      return;

    timeval timeout{.tv_sec = 2, .tv_usec = 0};
    if (setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) != 0)
      return;

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
      close(fd_);
      fd_ = -1;
    }
  }

  ~RawRespClient() {
    if (fd_ >= 0)
      close(fd_);
  }

  bool SendAll(std::string_view data) {
    while (!data.empty()) {
      ssize_t sent = send(fd_, data.data(), data.size(), 0);
      if (sent <= 0)
        return false;
      data.remove_prefix(sent);
    }
    return true;
  }

  std::optional<std::string> ReadLine() {
    std::string line;
    char byte = 0;
    while (true) {
      ssize_t received = recv(fd_, &byte, 1, 0);
      if (received != 1)
        return std::nullopt;
      line += byte;
      if (absl::EndsWith(line, "\r\n"))
        return line;
    }
  }

  std::optional<std::string> ReadUntilContains(std::string_view needle, size_t max_lines) {
    for (size_t i = 0; i < max_lines; ++i) {
      auto line = ReadLine();
      if (!line.has_value() || absl::StrContains(*line, needle))
        return line;
    }
    return std::nullopt;
  }

  bool ReadEof() {
    char byte = 0;
    return recv(fd_, &byte, 1, 0) == 0;
  }

  bool ShutdownWrite() const {
    return shutdown(fd_, SHUT_WR) == 0;
  }

  bool HasPendingData(std::chrono::milliseconds timeout) const {
    pollfd poll_fd{.fd = fd_, .events = POLLIN, .revents = 0};
    return poll(&poll_fd, 1, timeout.count()) > 0;
  }

  bool valid() const {
    return fd_ >= 0;
  }

 private:
  int fd_ = -1;
};

class DragonflyConnectionTest : public BaseFamilyTest {
 protected:
  void SetUp() override {
    absl::SetFlag(&FLAGS_enable_resp_io_loop_v2, true);
    absl::SetFlag(&FLAGS_enable_shared_read_buffer, UseSharedReadBuffer());
    BaseFamilyTest::SetUp();
    ASSERT_EQ(RunPrivileged({"ACL", "SETUSER", "default", "on", "nopass", "+@all", "~*", "&*"}),
              "OK");

    acceptor_ = std::make_unique<util::AcceptServer>(pp_.get(), /*break_on_int=*/false);
    pp_->at(0)->Await([this] {
      listener_ = new facade::Listener(facade::Protocol::REDIS, service_.get(),
                                       facade::Listener::Role::MAIN);
    });
    port_ = acceptor_->AddListener(0, listener_);
    acceptor_->Run();
  }

  void TearDown() override {
    acceptor_->Stop(/*wait=*/true);
    acceptor_.reset();
    BaseFamilyTest::TearDown();
  }

  bool WaitForBlockedConnection(std::string_view name) {
    return pp_->at(0)->Await([&] {
      return WaitUntilCondition(
          [&] {
            bool blocked = false;
            listener_->TraverseConnections([&](unsigned, util::Connection* connection) {
              auto* facade_connection = static_cast<facade::Connection*>(connection);
              if (facade_connection->GetName() == name) {
                blocked = static_cast<ConnectionContext*>(facade_connection->cntx())->blocked;
              }
            });
            return blocked;
          },
          2s);
    });
  }

  bool WaitForSyncDispatch(std::string_view name) {
    return pp_->at(0)->Await([&] {
      return WaitUntilCondition(
          [&] {
            bool dispatching = false;
            listener_->TraverseConnections([&](unsigned, util::Connection* connection) {
              auto* facade_connection = static_cast<facade::Connection*>(connection);
              if (facade_connection->GetName() == name) {
                dispatching =
                    static_cast<ConnectionContext*>(facade_connection->cntx())->sync_dispatch;
              }
            });
            return dispatching;
          },
          2s);
    });
  }

  bool SendMonitorMessage(std::string_view name, std::string message) {
    return pp_->at(0)->Await([&] {
      bool found = false;
      listener_->TraverseConnections([&](unsigned, util::Connection* connection) {
        auto* facade_connection = static_cast<facade::Connection*>(connection);
        if (facade_connection->GetName() == name) {
          facade_connection->SendMonitorMessageAsync(std::move(message));
          found = true;
        }
      });
      return found;
    });
  }

  virtual bool UseSharedReadBuffer() const {
    return false;
  }

  uint16_t port_ = 0;
  facade::Listener* listener_ = nullptr;
  std::unique_ptr<util::AcceptServer> acceptor_;
  absl::FlagSaver flag_saver_;
};

// Run V2 connection tests with private and shared read buffers.
class DragonflyV2ConnectionTest : public DragonflyConnectionTest,
                                  public testing::WithParamInterface<bool> {
 protected:
  bool UseSharedReadBuffer() const final {
    return GetParam();
  }
};

// A drained batch can expose an undispatched head while the queue remains over its byte limit.
TEST_P(DragonflyV2ConnectionTest, DrainsQueuedCommandsOverPipelineLimit) {
  const std::string original_limit =
      Run({"CONFIG", "GET", "pipeline_buffer_limit"}).GetVec()[1].GetString();
  for (bool squash : {true, false}) {
    SCOPED_TRACE(squash);
    absl::SetFlag(&FLAGS_enable_pipeline_squashing_v2, squash);
    absl::Cleanup restore_limit = [&] {
      EXPECT_EQ(Run({"CONFIG", "SET", "pipeline_buffer_limit", original_limit}), "OK");
    };

    RawRespClient client{port_};
    ASSERT_TRUE(client.valid());

    // Each PING ends a squash batch.
    ASSERT_TRUE(
        client.SendAll("CLIENT INFO\r\nCONFIG SET pipeline_buffer_limit 1\r\n"
                       "SET a b\r\nPING\r\nSET c d\r\nPING\r\n"));

    // Verify the whole pipeline was parsed before CONFIG lowers the limit, counting CLIENT INFO.
    ASSERT_THAT(client.ReadLine(), Optional(StartsWith("$")));
    ASSERT_THAT(client.ReadLine(), Optional(HasSubstr(" pipeline=6 ")));
    for (std::string_view reply : {"+OK\r\n", "+OK\r\n", "+PONG\r\n", "+OK\r\n", "+PONG\r\n"}) {
      ASSERT_EQ(client.ReadLine(), reply);
    }

    // The connection stays usable and both writes are visible without raising the limit.
    ASSERT_TRUE(client.SendAll("GET a\r\nGET c\r\nPING\r\n"));
    for (std::string_view reply : {"$1\r\n", "b\r\n", "$1\r\n", "d\r\n", "+PONG\r\n"}) {
      ASSERT_EQ(client.ReadLine(), reply);
    }
  }
}

// Completing one async reply must leave a completion subscription for the next pending head.
TEST_P(DragonflyV2ConnectionTest, WakesForNextAsyncReplyOverPipelineLimit) {
  absl::SetFlag(&FLAGS_enable_pipeline_squashing_v2, false);
  const std::string original_limit =
      Run({"CONFIG", "GET", "pipeline_buffer_limit"}).GetVec()[1].GetString();

  ASSERT_GE(shard_set->size(), 2u);
  const std::string first_key = "pipeline-async-first";
  std::string second_key = "pipeline-async-second";
  while (Shard(first_key, shard_set->size()) == Shard(second_key, shard_set->size()))
    second_key += "x";

  auto* exec_cid = service_->FindCmd("EXEC");
  boost::intrusive_ptr<Transaction> first_lock(new Transaction{exec_cid});
  boost::intrusive_ptr<Transaction> second_lock(new Transaction{exec_cid});
  absl::Cleanup restore_state = [&] {
    pp_->at(0)->Await([&] {
      first_lock->UnlockMulti(true);
      second_lock->UnlockMulti(true);
    });
    EXPECT_EQ(Run({"CONFIG", "SET", "pipeline_buffer_limit", original_limit}), "OK");
  };
  pp_->at(0)->Await([&] {
    std::string_view first_keys[] = {first_key};
    std::string_view second_keys[] = {second_key};
    auto* ns = &namespaces->GetDefaultNamespace();
    first_lock->StartMultiLockedAhead(ns, 0, CmdArgList{first_keys}, 1);
    second_lock->StartMultiLockedAhead(ns, 0, CmdArgList{second_keys}, 1);
  });

  RawRespClient client{port_};
  ASSERT_TRUE(client.valid());
  ASSERT_TRUE(client.SendAll("CLIENT INFO\r\nCONFIG SET pipeline_buffer_limit 1\r\nSET " +
                             first_key + " a\r\nSET " + second_key + " b\r\n"));

  // Verify both SETs were parsed before CONFIG lowers the limit; the count includes CLIENT INFO.
  ASSERT_THAT(client.ReadLine(), Optional(StartsWith("$")));
  ASSERT_THAT(client.ReadLine(), Optional(HasSubstr(" pipeline=4 ")));

  // CONFIG's batched reply flushes when backpressure parks behind the first locked SET.
  ASSERT_EQ(client.ReadLine(), "+OK\r\n");
  pp_->at(0)->Await([&] { first_lock->UnlockMulti(true); });

  // The next flush follows retirement of the first SET, while the second SET remains pending.
  ASSERT_EQ(client.ReadLine(), "+OK\r\n");
  pp_->at(0)->Await([&] { second_lock->UnlockMulti(true); });
  ASSERT_EQ(client.ReadLine(), "+OK\r\n");
}

// A valid pipelined prefix must execute and reply in order before the terminal protocol error.
// The malformed frame must then produce the error and close the connection.
TEST_P(DragonflyV2ConnectionTest, DrainsValidPipelineBeforeProtocolError) {
  RawRespClient client{port_};
  ASSERT_TRUE(client.valid());
  ASSERT_TRUE(
      client.SendAll("*1\r\n$4\r\nPING\r\n"
                     "*2\r\n$4\r\nECHO\r\n$12\r\nbefore-error\r\n"
                     "*x\r\n"));

  ASSERT_EQ(client.ReadLine(), "+PONG\r\n");
  ASSERT_EQ(client.ReadLine(), "$12\r\n");
  ASSERT_EQ(client.ReadLine(), "before-error\r\n");
  auto error = client.ReadLine();
  ASSERT_TRUE(error.has_value());
  EXPECT_THAT(*error, HasSubstr("Protocol error"));
  EXPECT_TRUE(client.ReadEof());
}

// An error without a valid prefix must be finalized instead of parking for more input.
TEST_P(DragonflyV2ConnectionTest, FinalizesProtocolErrorWithoutParsedCommands) {
  RawRespClient client{port_};
  ASSERT_TRUE(client.valid());
  ASSERT_TRUE(client.SendAll("*x\r\n"));

  auto error = client.ReadLine();
  ASSERT_TRUE(error.has_value());
  EXPECT_THAT(*error, HasSubstr("Protocol error"));
  EXPECT_TRUE(client.ReadEof());
}

// A terminal parser error must not discard an earlier blocking command or later valid prefix.
// After BLPOP is unblocked normally, its reply and PING's reply must precede the error and EOF.
TEST_P(DragonflyV2ConnectionTest, DrainsBlockingPrefixBeforeProtocolError) {
  RawRespClient client{port_};
  ASSERT_TRUE(client.valid());
  ASSERT_TRUE(
      client.SendAll("*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n"
                     "$20\r\nparser-error-blocked\r\n"));
  ASSERT_EQ(client.ReadLine(), "+OK\r\n");

  ASSERT_TRUE(
      client.SendAll("*3\r\n$5\r\nBLPOP\r\n$17\r\nparser-error-list\r\n$1\r\n0\r\n"
                     "*1\r\n$4\r\nPING\r\n"
                     "*x\r\n"));
  ASSERT_TRUE(WaitForBlockedConnection("parser-error-blocked"));

  EXPECT_THAT(Run({"RPUSH", "parser-error-list", "value"}), IntArg(1));
  ASSERT_EQ(client.ReadLine(), "*2\r\n");
  ASSERT_EQ(client.ReadLine(), "$17\r\n");
  ASSERT_EQ(client.ReadLine(), "parser-error-list\r\n");
  ASSERT_EQ(client.ReadLine(), "$5\r\n");
  ASSERT_EQ(client.ReadLine(), "value\r\n");
  ASSERT_EQ(client.ReadLine(), "+PONG\r\n");
  auto error = client.ReadLine();
  ASSERT_TRUE(error.has_value());
  EXPECT_THAT(*error, HasSubstr("Protocol error"));
  EXPECT_TRUE(client.ReadEof());
}

// Bytes following malformed RESP are not a valid prefix and must never be parsed or executed.
// The preceding SET succeeds; the trailing SET remains absent after the error closes the
// connection.
TEST_P(DragonflyV2ConnectionTest, DoesNotExecuteCommandsAfterProtocolError) {
  RawRespClient client{port_};
  ASSERT_TRUE(client.valid());
  ASSERT_TRUE(
      client.SendAll("*3\r\n$3\r\nSET\r\n$19\r\nprefix-before-error\r\n$5\r\nvalue\r\n"
                     "*x\r\n"
                     "*3\r\n$3\r\nSET\r\n$11\r\nafter-error\r\n$16\r\nmust-not-execute\r\n"));

  ASSERT_EQ(client.ReadLine(), "+OK\r\n");
  auto error = client.ReadLine();
  ASSERT_TRUE(error.has_value());
  EXPECT_THAT(*error, HasSubstr("Protocol error"));
  EXPECT_TRUE(client.ReadEof());

  EXPECT_EQ(Run({"GET", "prefix-before-error"}), "value");
  EXPECT_EQ(Run({"GET", "after-error"}).type, facade::RespExpr::NIL);
}

// An older control message must reply before a later parsed prefix and terminal protocol error.
TEST_P(DragonflyV2ConnectionTest, DrainsOlderControlMessageBeforePipelinePrefix) {
  RawRespClient client{port_};
  ASSERT_TRUE(client.valid());
  ASSERT_TRUE(
      client.SendAll("*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n"
                     "$13\r\ncontrol-order\r\n"));
  ASSERT_EQ(client.ReadLine(), "+OK\r\n");

  ASSERT_TRUE(SendMonitorMessage("control-order", "older-control-message"));
  ASSERT_TRUE(client.SendAll("*1\r\n$4\r\nPING\r\n*x\r\n"));
  ASSERT_EQ(client.ReadLine(), "+older-control-message\r\n");
  ASSERT_EQ(client.ReadLine(), "+PONG\r\n");
  auto error = client.ReadLine();
  ASSERT_TRUE(error.has_value());
  EXPECT_THAT(*error, HasSubstr("Protocol error"));
  EXPECT_TRUE(client.ReadEof());
}

// A synchronous command behind an async prefix returns WOULD_BLOCK until that prefix drains.
TEST_P(DragonflyV2ConnectionTest, DrainsWouldBlockCommandBeforeProtocolError) {
  RawRespClient client{port_};
  ASSERT_TRUE(client.valid());
  ASSERT_TRUE(
      client.SendAll("*3\r\n$3\r\nSET\r\n$11\r\nwould-block\r\n$5\r\nvalue\r\n"
                     "*1\r\n$5\r\nMULTI\r\n"
                     "*1\r\n$4\r\nPING\r\n"
                     "*1\r\n$4\r\nEXEC\r\n"
                     "*x\r\n"));

  ASSERT_EQ(client.ReadLine(), "+OK\r\n");
  ASSERT_EQ(client.ReadLine(), "+OK\r\n");
  ASSERT_EQ(client.ReadLine(), "+QUEUED\r\n");
  ASSERT_EQ(client.ReadLine(), "*1\r\n");
  ASSERT_EQ(client.ReadLine(), "+PONG\r\n");
  auto error = client.ReadLine();
  ASSERT_TRUE(error.has_value());
  EXPECT_THAT(*error, HasSubstr("Protocol error"));
  EXPECT_TRUE(client.ReadEof());
}

// A parser error received by the proactor callback must reject bytes after the malformed frame.
TEST_P(DragonflyV2ConnectionTest, CallbackParserErrorDoesNotExecuteLaterCommands) {
  RawRespClient client{port_};
  ASSERT_TRUE(client.valid());
  ASSERT_TRUE(
      client.SendAll("*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n"
                     "$14\r\ncallback-error\r\n"));
  ASSERT_EQ(client.ReadLine(), "+OK\r\n");
  ASSERT_TRUE(client.SendAll("*3\r\n$5\r\nDEBUG\r\n$7\r\nIOSTATS\r\n$2\r\nPS\r\n"));
  ASSERT_TRUE(WaitForSyncDispatch("callback-error"));

  ASSERT_TRUE(
      client.SendAll("*3\r\n$3\r\nSET\r\n$15\r\ncallback-prefix\r\n$5\r\nvalue\r\n"
                     "*x\r\n"
                     "*3\r\n$3\r\nSET\r\n$14\r\ncallback-after\r\n$16\r\nmust-not-execute\r\n"));
  auto error = client.ReadUntilContains("Protocol error", 256);
  ASSERT_TRUE(error.has_value());
  EXPECT_TRUE(client.ReadEof());
  EXPECT_EQ(Run({"GET", "callback-prefix"}), "value");
  EXPECT_EQ(Run({"GET", "callback-after"}).type, facade::RespExpr::NIL);
}

// A peer write-side shutdown must not prevent delivery of the terminal protocol error.
TEST_P(DragonflyV2ConnectionTest, TerminalParserErrorSurvivesPeerWriteShutdown) {
  RawRespClient client{port_};
  ASSERT_TRUE(client.valid());
  ASSERT_TRUE(
      client.SendAll("*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n"
                     "$15\r\neof-after-error\r\n"));
  ASSERT_EQ(client.ReadLine(), "+OK\r\n");

  ASSERT_TRUE(client.SendAll("*3\r\n$5\r\nDEBUG\r\n$7\r\nIOSTATS\r\n$2\r\nPS\r\n*x\r\n"));
  ASSERT_TRUE(WaitForSyncDispatch("eof-after-error"));
  ASSERT_TRUE(client.ShutdownWrite());

  auto error = client.ReadUntilContains("Protocol error", 256);
  ASSERT_TRUE(error.has_value());
  EXPECT_TRUE(client.ReadEof());
}

// A checkpoint sent during parser-error draining must wait for the command that was dispatching
// when the error latched.
TEST_P(DragonflyV2ConnectionTest, CheckpointWaitsForDispatchDuringParserErrorDrain) {
  RawRespClient client{port_};
  ASSERT_TRUE(client.valid());
  ASSERT_TRUE(
      client.SendAll("*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n"
                     "$16\r\ncheckpoint-drain\r\n"));
  ASSERT_EQ(client.ReadLine(), "+OK\r\n");

  ASSERT_TRUE(client.SendAll("*3\r\n$5\r\nDEBUG\r\n$7\r\nIOSTATS\r\n$2\r\nPS\r\n*x\r\n"));
  ASSERT_TRUE(WaitForSyncDispatch("checkpoint-drain"));
  ASSERT_FALSE(client.HasPendingData(100ms));
  facade::DispatchTracker tracker{{listener_}, nullptr, true, true};
  tracker.TrackAll();
  EXPECT_FALSE(tracker.Wait(absl::Milliseconds(100)));

  auto error = client.ReadUntilContains("Protocol error", 256);
  ASSERT_TRUE(error.has_value());
  EXPECT_TRUE(client.ReadEof());
  EXPECT_TRUE(tracker.Wait(absl::Milliseconds(100)));
}

// Control work produced after terminal draining starts must not enter dispatch_q_.
TEST_P(DragonflyV2ConnectionTest, RejectsControlMessagesAfterProtocolError) {
  RawRespClient client{port_};
  ASSERT_TRUE(client.valid());
  ASSERT_TRUE(
      client.SendAll("*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n"
                     "$11\r\ngated-drain\r\n"));
  ASSERT_EQ(client.ReadLine(), "+OK\r\n");
  ASSERT_TRUE(
      client.SendAll("*3\r\n$5\r\nBLPOP\r\n$16\r\ngated-drain-list\r\n$1\r\n0\r\n"
                     "*1\r\n$4\r\nPING\r\n"
                     "*x\r\n"));
  ASSERT_TRUE(WaitForBlockedConnection("gated-drain"));
  ASSERT_TRUE(SendMonitorMessage("gated-drain", "must-not-be-enqueued"));

  EXPECT_THAT(Run({"RPUSH", "gated-drain-list", "value"}), IntArg(1));
  ASSERT_EQ(client.ReadLine(), "*2\r\n");
  ASSERT_EQ(client.ReadLine(), "$16\r\n");
  ASSERT_EQ(client.ReadLine(), "gated-drain-list\r\n");
  ASSERT_EQ(client.ReadLine(), "$5\r\n");
  ASSERT_EQ(client.ReadLine(), "value\r\n");
  ASSERT_EQ(client.ReadLine(), "+PONG\r\n");
  auto error = client.ReadLine();
  ASSERT_TRUE(error.has_value());
  EXPECT_THAT(*error, HasSubstr("Protocol error"));
  EXPECT_TRUE(client.ReadEof());
}

INSTANTIATE_TEST_SUITE_P(PrivateAndSharedReadBuffer, DragonflyV2ConnectionTest,
                         testing::Values(false, true));

// Real main and admin listeners. No OpenSSL use: the last destroyed listener cleans it up.
class ClientKillTest : public BaseFamilyTest {
 protected:
  std::vector<facade::Listener*> CreateListeners() override {
    acceptor_ = std::make_unique<util::AcceptServer>(pp_.get(), /*break_on_int=*/false);
    auto make = [&](facade::Listener::Role role) {
      return pp_->at(0)->Await(
          [&] { return new facade::Listener(facade::Protocol::REDIS, service_.get(), role); });
    };
    auto* listener = make(facade::Listener::Role::MAIN);
    auto* admin = make(facade::Listener::Role::PRIVILEGED);
    port_ = acceptor_->AddListener(0, listener);
    admin_port_ = acceptor_->AddListener(0, admin);
    return {listener, admin};
  }

  void SetUp() override {
    BaseFamilyTest::SetUp();
    acceptor_->Run();
  }

  // The service shuts down before the listeners are freed.
  void TearDown() override {
    acceptor_->Stop(/*wait=*/true);
    BaseFamilyTest::TearDown();
  }

  std::unique_ptr<util::AcceptServer> acceptor_;
  uint16_t port_ = 0, admin_port_ = 0;
  absl::FlagSaver flag_saver_;
};

// Sends an inline command and returns the first line of the reply.
std::optional<std::string> Call(RawRespClient& client, std::string_view cmd) {
  return client.SendAll(absl::StrCat(cmd, "\r\n")) ? client.ReadLine() : std::nullopt;
}

std::string ClientId(RawRespClient& client) {
  std::string reply = Call(client, "CLIENT ID").value_or(":\r\n");
  return reply.substr(1, reply.size() - 3);
}

// Killing itself, a connection still gets the reply but runs no later command.
TEST_F(ClientKillTest, SelfClosesAfterReply) {
  auto send_until_eof = [&](std::string_view before, std::string_view after, bool legacy = false) {
    RawRespClient client{port_};
    EXPECT_TRUE(client.SendAll("CLIENT INFO\r\n"));
    client.ReadLine();
    std::vector<std::string> info =
        absl::StrSplit(client.ReadLine().value_or(""), absl::ByAnyChar(" ="));
    std::string kill = absl::StrCat("CLIENT KILL ID ", info.at(1), " SKIPME no\r\n");
    if (legacy)
      kill = absl::StrCat("CLIENT KILL ", info.at(3), "\r\n");
    EXPECT_TRUE(client.SendAll(absl::StrCat(before, kill, after)));
    std::string reply;
    while (auto line = client.ReadLine())
      reply += *line;
    EXPECT_TRUE(client.ReadEof()) << reply;
    return reply;
  };

  // A kill in the first packet must not keep the server side open until the client closes.
  RawRespClient first{port_};
  EXPECT_TRUE(first.SendAll(absl::StrCat("CLIENT KILL LADDR 127.0.0.1:", port_, " SKIPME no\r\n")));
  EXPECT_EQ(first.ReadLine(), ":1\r\n");
  ExpectConditionWithinTimeout(
      [&] { return GetMetrics().facade_stats.conn_stats.num_conns_main == 0; });

  // A connection reads the flag when it starts.
  for (bool v2 : {false, true}) {
    SCOPED_TRACE(v2);
    absl::SetFlag(&FLAGS_enable_resp_io_loop_v2, v2);
    EXPECT_EQ(send_until_eof("PING\r\n", "SET k1 1\r\nPING\r\n"), "+PONG\r\n:1\r\n");
    for (bool squash : {true, false}) {
      SCOPED_TRACE(squash);
      absl::SetFlag(&FLAGS_multi_exec_squash, squash);
      EXPECT_EQ(send_until_eof("MULTI\r\n", "PING\r\nSET k2 1\r\nEXEC\r\nSET k3 1\r\n"),
                "+OK\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n*3\r\n:1\r\n+PONG\r\n+OK\r\n");
    }
    EXPECT_EQ(send_until_eof("PING\r\n", "SET k4 1\r\n", /*legacy=*/true), "+PONG\r\n+OK\r\n");

    // A lone command is dispatched synchronously.
    EXPECT_EQ(send_until_eof("", ""), ":1\r\n");

    // Over the pipeline limit, the next command waits for the kill and must not run after it.
    EXPECT_EQ(Run({"CONFIG", "SET", "pipeline_buffer_limit", "1"}), "OK");
    EXPECT_EQ(send_until_eof("", "SET k5 1\r\n"), ":1\r\n");
    EXPECT_EQ(Run({"CONFIG", "SET", "pipeline_buffer_limit", "128MB"}), "OK");
    EXPECT_THAT(Run({"DEL", "k2"}), IntArg(1));
    EXPECT_THAT(Run({"DEL", "k1", "k3", "k4", "k5"}), IntArg(0));
  }
}

TEST_F(ClientKillTest, Filters) {
  RawRespClient old{port_}, admin{admin_port_}, u1{port_}, u1_other{port_}, u2{port_},
      monitor{port_}, caller{port_};
  const std::string old_id = ClientId(old);
  const time_t old_born = time(nullptr);
  const std::string admin_id = ClientId(admin);
  for (std::string_view user : {"u1", "u2"})
    Run({"ACL", "SETUSER", user, "ON", "nopass", "+@all", "~*", "&*"});
  for (RawRespClient* client : {&u1, &u1_other})
    ASSERT_EQ(Call(*client, "AUTH u1 x"), "+OK\r\n");
  ASSERT_EQ(Call(u2, "AUTH u2 x"), "+OK\r\n");
  std::deque<RawRespClient> subs;
  for (std::string_view cmd :
       {"SUBSCRIBE ch", "PSUBSCRIBE ch", "SSUBSCRIBE ch", "HELLO 3\r\nSUBSCRIBE ch"}) {
    RawRespClient& sub = subs.emplace_back(port_);
    ASSERT_TRUE(Call(sub, cmd) && sub.ReadUntilContains("subscribe", 64));
  }
  ASSERT_EQ(Call(monitor, "MONITOR"), "+OK\r\n");

  // A rejected command kills nothing; the counts below check it.
  EXPECT_THAT(Run({"CLIENT", "KILL", "TYPE", "normal", "MAXAGE", "0"}),
              ErrArg("maxage should be greater than 0"));
  ASSERT_THAT(Run({"CLIENT", "KILL", "ID", admin_id}),
              ErrArg("Killed 0 client(s), but unable to kill 1 admin client(s)."));
  const std::vector<std::string> admin_info =
      absl::StrSplit(Run({"CLIENT", "LIST", "ID", admin_id}).GetString(), absl::ByAnyChar(" ="));
  EXPECT_THAT(Run({"CLIENT", "KILL", "ADDR", admin_info.at(3)}), ErrArg("unable to kill 1 admin"));
  EXPECT_THAT(Run({"CLIENT", "KILL", "LADDR", absl::StrCat("127.0.0.1:", admin_port_)}),
              ErrArg("unable to kill 1 admin"));
  RawRespClient admin_replica{admin_port_};
  ASSERT_TRUE(Call(admin_replica, "REPLCONF CAPA dragonfly"));
  EXPECT_THAT(Run({"CLIENT", "KILL", "TYPE", "replica"}), ErrArg("unable to kill 1 admin client"));
  EXPECT_THAT(Run({"CLIENT", "KILL", "TYPE", "slave"}), ErrArg("unable to kill 1 admin client"));
  EXPECT_EQ(Call(admin, "CLIENT KILL TYPE replica"), ":1\r\n");
  EXPECT_THAT(Run({"CLIENT", "KILL", "USER", "u1"}), IntArg(2));
  EXPECT_TRUE(u1.ReadEof() && u1_other.ReadEof());
  EXPECT_THAT(Run({"CLIENT", "KILL", "TYPE", "pubsub"}), IntArg(4));

  // MAXAGE counts from the connection start, not from the last command.
  while (time(nullptr) <= old_born)
    absl::SleepFor(absl::Milliseconds(10));
  ASSERT_EQ(Call(old, "PING"), "+PONG\r\n");
  EXPECT_THAT(Run({"CLIENT", "KILL", "ID", old_id, "MAXAGE", "1", "LADDR", "1.2.3.4:5"}),
              IntArg(0));
  EXPECT_THAT(Run({"CLIENT", "KILL", "ID", old_id, "MAXAGE", "1"}), IntArg(1));

  // Pipelined: the second kill usually runs while the first one's victims are still closing.
  EXPECT_EQ(Call(caller, "CLIENT KILL TYPE normal\r\nCLIENT KILL TYPE normal"), ":2\r\n");
  EXPECT_EQ(caller.ReadLine(), ":0\r\n");
}

// A half-closed peer marks the connection closing, yet only a kill ends its blocked send.
TEST_F(ClientKillTest, KillsHalfClosedStuckSend) {
  RawRespClient v{port_};
  const std::string id = ClientId(v);
  Run({"SET", "big", std::string(64 << 20, 'x')});
  auto row = [&] { return Run({"CLIENT", "LIST", "ID", id}).GetString(); };
  ASSERT_TRUE(v.SendAll("GET big\r\n"));
  ExpectConditionWithinTimeout([&] { return absl::StrContains(row(), "phase=send"); });
  ASSERT_TRUE(v.ShutdownWrite());
  ExpectConditionWithinTimeout([&] { return absl::StrContains(row(), "flags=t"); });
  EXPECT_THAT(Run({"CLIENT", "KILL", "ID", id}), IntArg(1));
  ExpectConditionWithinTimeout([&] { return row().empty(); });
}

// Connections that move to another thread can not be borrowed.
TEST_F(ClientKillTest, KillDuringConnectionMigration) {
  std::atomic_bool stop = false;
  std::vector<std::thread> churn;
  for (unsigned i = 0; i < 8; ++i) {
    churn.emplace_back([&] {
      for (unsigned tid = 0; !stop;) {
        RawRespClient client{port_};
        const std::string id = ClientId(client);
        const std::string migrate = absl::StrCat("CLIENT MIGRATE ", id, " ");
        while (!stop && !id.empty() && Call(client, absl::StrCat(migrate, ++tid % 3))) {
        }
      }
    });
  }
  const std::string laddr = absl::StrCat("127.0.0.1:", port_);
  int64_t killed = 0;
  for (absl::Time end = absl::Now() + absl::Seconds(2); absl::Now() < end;) {
    auto reply = Run({"CLIENT", "KILL", "LADDR", laddr});
    EXPECT_THAT(reply, ArgType(RespExpr::INT64));
    killed += reply.GetInt().value_or(0);
    absl::SleepFor(absl::Milliseconds(1));
  }
  EXPECT_GT(killed, 0);
  stop = true;
  for (auto& thread : churn)
    thread.join();
}

}  // namespace
}  // namespace dfly
