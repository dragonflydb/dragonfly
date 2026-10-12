// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "facade/dragonfly_connection.h"

#include <absl/strings/match.h>
#include <arpa/inet.h>
#include <gmock/gmock.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <optional>
#include <string>
#include <string_view>

#include "base/flags.h"
#include "facade/dragonfly_listener.h"
#include "facade/facade_test.h"
#include "server/conn_context.h"
#include "server/test_utils.h"
#include "util/accept_server.h"

ABSL_DECLARE_FLAG(bool, enable_resp_io_loop_v2);
ABSL_DECLARE_FLAG(bool, enable_shared_read_buffer);

namespace dfly {
namespace {

using namespace std::chrono_literals;
using testing::HasSubstr;

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

// Run every V2 parser-error test with private and shared read buffers.
class DragonflyV2ConnectionTest : public DragonflyConnectionTest,
                                  public testing::WithParamInterface<bool> {
 protected:
  bool UseSharedReadBuffer() const final {
    return GetParam();
  }
};

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

// A command that cannot run asynchronously behind a blocked head must wait, then run in order.
TEST_P(DragonflyV2ConnectionTest, DrainsWouldBlockCommandBeforeProtocolError) {
  RawRespClient client{port_};
  ASSERT_TRUE(client.valid());
  ASSERT_TRUE(
      client.SendAll("*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n"
                     "$11\r\nwould-block\r\n"));
  ASSERT_EQ(client.ReadLine(), "+OK\r\n");

  ASSERT_TRUE(
      client.SendAll("*3\r\n$5\r\nBLPOP\r\n$16\r\nwould-block-list\r\n$1\r\n0\r\n"
                     "*1\r\n$5\r\nMULTI\r\n"
                     "*1\r\n$4\r\nPING\r\n"
                     "*1\r\n$4\r\nEXEC\r\n"
                     "*x\r\n"));
  ASSERT_TRUE(WaitForBlockedConnection("would-block"));

  EXPECT_THAT(Run({"RPUSH", "would-block-list", "value"}), IntArg(1));
  ASSERT_EQ(client.ReadLine(), "*2\r\n");
  ASSERT_EQ(client.ReadLine(), "$16\r\n");
  ASSERT_EQ(client.ReadLine(), "would-block-list\r\n");
  ASSERT_EQ(client.ReadLine(), "$5\r\n");
  ASSERT_EQ(client.ReadLine(), "value\r\n");
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

// A parser error found before a suspending dispatch must latch immediately. Bytes received while
// DEBUG IOSTATS PS holds the dispatch cannot be parsed as a later command.
TEST_P(DragonflyV2ConnectionTest, FiberParserErrorDoesNotExecuteLaterCommands) {
  RawRespClient client{port_};
  ASSERT_TRUE(client.valid());
  ASSERT_TRUE(
      client.SendAll("*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n"
                     "$11\r\nfiber-error\r\n"));
  ASSERT_EQ(client.ReadLine(), "+OK\r\n");

  ASSERT_TRUE(client.SendAll("*3\r\n$5\r\nDEBUG\r\n$7\r\nIOSTATS\r\n$2\r\nPS\r\n*x\r\n"));
  ASSERT_TRUE(WaitForSyncDispatch("fiber-error"));
  ASSERT_TRUE(
      client.SendAll("*3\r\n$3\r\nSET\r\n$11\r\nfiber-after\r\n$16\r\nmust-not-execute\r\n"));

  auto error = client.ReadUntilContains("Protocol error", 256);
  ASSERT_TRUE(error.has_value());
  EXPECT_TRUE(client.ReadEof());
  EXPECT_EQ(Run({"GET", "fiber-after"}).type, facade::RespExpr::NIL);
}

// A peer that closes its write side while earlier work drains must still receive the protocol
// error after the queued reply.
TEST_P(DragonflyV2ConnectionTest, ParserErrorReplySurvivesPeerWriteShutdown) {
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

}  // namespace
}  // namespace dfly
