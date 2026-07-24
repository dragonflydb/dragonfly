// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/streamer.h"

#include <sys/socket.h>

#include <boost/asio/ip/address.hpp>
#include <tuple>

#include "base/gtest.h"
#include "base/logging.h"
#include "server/execution_state.h"
#include "util/fibers/pool.h"

// Regression test for a use-after-free: the stable-sync
// JournalStreamer can be torn down while an async write to the replica socket is still
// in flight. ~JournalStreamer only DCHECKs that no write is in flight, and that DCHECK is
// itself skipped when the execution context is already in an error state (e.g. the replica
// disconnected) - so on that path the destructor frees pending_buf_ while the kernel still
// holds raw pointers into its strings via the outstanding AsyncWrite, and the completion
// callback still captures `this`. See streamer.cc:121-124 and dflycmd.cc's empty
// FlowInfo::~FlowInfo().
//
// The test shrinks the socket's send buffer so a large write cannot complete synchronously,
// destroys the streamer while that write is still outstanding and the context is in an error state,
// and then drains the peer so the kernel finally completes the write - invoking the
// completion callback on the already-destroyed streamer.

namespace dfly {

using namespace std;
using namespace util;

namespace {

// Exposes protected JournalStreamer internals needed to drive it without a full
// Journal/replication setup.
class TestJournalStreamer : public JournalStreamer {
 public:
  using JournalStreamer::JournalStreamer;
  using JournalStreamer::Write;

  void SetDest(FiberSocketBase* dest) {
    dest_ = dest;
  }

  size_t InFlightBytes() const {
    return inflight_bytes();
  }
};

constexpr uint32_t kRingDepth = 8;
constexpr size_t kPayloadSize = 16 << 20;  // Exceeds any realistic kernel socket buffer.

}  // namespace

class JournalStreamerTest : public testing::Test {
 protected:
  void SetUp() override {
    pp_.reset(fb2::Pool::IOUring(kRingDepth, 1));
    pp_->Run();
  }

  void TearDown() override {
    pp_->Stop();
  }

  unique_ptr<ProactorPool> pp_;
};

TEST_F(JournalStreamerTest, DestructorUseAfterFreeOnErrorWithInFlightWrite) {
  ProactorBase* proactor = pp_->at(0);

  unique_ptr<FiberSocketBase> listen_socket;
  unique_ptr<FiberSocketBase> server_socket;  // Accepted side; JournalStreamer's dest_.
  unique_ptr<FiberSocketBase> client_socket;  // "Replica" side; deliberately not read from.
  FiberSocketBase::endpoint_type listen_ep;

  error_code ec = proactor->AwaitBrief([&] {
    listen_socket.reset(proactor->CreateSocket());
    return listen_socket->Listen(0, 0);
  });
  ASSERT_FALSE(ec) << ec.message();

  auto address = boost::asio::ip::make_address("127.0.0.1");
  listen_ep = FiberSocketBase::endpoint_type{address, listen_socket->LocalEndpoint().port()};

  fb2::Fiber accept_fb = proactor->LaunchFiber("accept", [&] {
    auto accept_res = listen_socket->Accept();
    CHECK(accept_res) << accept_res.error();
    server_socket.reset(*accept_res);
    server_socket->SetProactor(proactor);
  });

  proactor->Await([&] {
    client_socket.reset(proactor->CreateSocket());
    error_code cec = client_socket->Connect(listen_ep);
    CHECK(!cec) << cec.message();
  });
  accept_fb.Join();

  // Shrink the outgoing buffer so a multi-megabyte write genuinely cannot complete
  // synchronously while nobody drains the peer.
  proactor->AwaitBrief([&] {
    int sndbuf = 1024;
    CHECK_EQ(0, setsockopt(server_socket->native_handle(), SOL_SOCKET, SO_SNDBUF, &sndbuf,
                           sizeof(sndbuf)));
  });

  ExecutionState cntx;
  auto* streamer = new TestJournalStreamer(&cntx, JournalStreamer::Config{});

  bool write_pending = proactor->Await([&] {
    streamer->SetDest(server_socket.get());
    streamer->Write(string(kPayloadSize, 'x'));
    return streamer->InFlightBytes() > 0;
  });
  ASSERT_TRUE(write_pending) << "write should not have completed synchronously";

  // Mirrors the production trigger: the replication flow enters an error state (e.g. a
  // replica disconnect) while a journal write is still outstanding.
  cntx.ReportError(make_error_code(errc::connection_reset));

  // Buggy path: ~JournalStreamer skips the in-flight DCHECK/drain because cntx_->IsError(), so
  // pending_buf_ (and the streamer itself) are freed while the write is still outstanding and
  // its completion lambda still captures `this`.
  proactor->Await([&] { delete streamer; });

  // Draining the peer lets the kernel finally complete the queued write, which invokes
  // OnCompletion() on the already-destroyed streamer - a use-after-free that crashes here.
  string buf(1 << 16, '\0');
  size_t total = 0;
  while (total < kPayloadSize) {
    auto res = proactor->Await([&] {
      return client_socket->Recv(
          io::MutableBytes{reinterpret_cast<uint8_t*>(buf.data()), buf.size()});
    });
    if (!res)
      break;
    total += *res;
  }

  proactor->Await([&] {
    std::ignore = listen_socket->Close();
    std::ignore = server_socket->Close();
    std::ignore = client_socket->Close();
  });
}

}  // namespace dfly
