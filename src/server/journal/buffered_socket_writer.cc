// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/buffered_socket_writer.h"

#include <absl/strings/str_cat.h>
#include <sys/socket.h>

#ifdef __linux__
#include <netinet/tcp.h>
#endif

#include "base/cycle_clock.h"
#include "base/flags.h"
#include "base/logging.h"
#include "server/server_state.h"

using facade::operator""_MB;

ABSL_FLAG(uint32_t, replication_timeout, 30000,
          "Time in milliseconds to wait for the replication writes being stuck.");

ABSL_FLAG(uint32_t, replication_stream_output_limit, 1_MB,
          "Time to wait for the replication output buffer go below the throttle limit");

namespace dfly {
using namespace util;
using namespace std;

namespace {

iovec IoVec(io::Bytes src) {
  return iovec{const_cast<uint8_t*>(src.data()), src.size()};
}

void LogTcpSocketDiagnostics(util::FiberSocketBase* dest) {
  if (!dest) {
    return;
  }

#ifdef __linux__
  // On Linux, we can get TCP diagnostics using getsockopt.
  int sockfd = dest->native_handle();
  if (sockfd < 0) {
    return;
  }

  struct tcp_info info;
  socklen_t info_len = sizeof(info);
  if (getsockopt(sockfd, IPPROTO_TCP, TCP_INFO, &info, &info_len) == 0) {
    LOG_EVERY_T(INFO, 1) << "TCP socket diagnostics - "
                         << "state: " << static_cast<int>(info.tcpi_state)
                         << ", ca_state: " << static_cast<int>(info.tcpi_ca_state)
                         << ", retransmits: " << static_cast<int>(info.tcpi_retransmits)
                         << ", probes: " << static_cast<int>(info.tcpi_probes)
                         << ", backoff: " << static_cast<int>(info.tcpi_backoff)
                         << ", options: " << static_cast<int>(info.tcpi_options)
                         << ", snd_wscale: " << static_cast<int>(info.tcpi_snd_wscale)
                         << ", rcv_wscale: " << static_cast<int>(info.tcpi_rcv_wscale)
                         << ", rto: " << info.tcpi_rto << ", ato: " << info.tcpi_ato
                         << ", snd_mss: " << info.tcpi_snd_mss << ", rcv_mss: " << info.tcpi_rcv_mss
                         << ", unacked: " << info.tcpi_unacked << ", sacked: " << info.tcpi_sacked
                         << ", lost: " << info.tcpi_lost << ", retrans: " << info.tcpi_retrans
                         << ", fackets: " << info.tcpi_fackets
                         << ", last_data_sent: " << info.tcpi_last_data_sent
                         << ", last_ack_sent: " << info.tcpi_last_ack_sent
                         << ", last_data_recv: " << info.tcpi_last_data_recv
                         << ", last_ack_recv: " << info.tcpi_last_ack_recv
                         << ", pmtu: " << info.tcpi_pmtu
                         << ", rcv_ssthresh: " << info.tcpi_rcv_ssthresh
                         << ", rtt: " << info.tcpi_rtt << ", rttvar: " << info.tcpi_rttvar
                         << ", snd_ssthresh: " << info.tcpi_snd_ssthresh
                         << ", snd_cwnd: " << info.tcpi_snd_cwnd << ", advmss: " << info.tcpi_advmss
                         << ", reordering: " << info.tcpi_reordering
                         << ", rcv_rtt: " << info.tcpi_rcv_rtt
                         << ", rcv_space: " << info.tcpi_rcv_space
                         << ", total_retrans: " << info.tcpi_total_retrans;
  } else {
    LOG_EVERY_T(INFO, 1) << "Failed to get TCP socket info: " << strerror(errno);
  }
#endif
}

}  // namespace

BufferedSocketWriter::BufferedSocketWriter(ExecutionState* cntx, Options opts)
    : cntx_(cntx),
      opts_(opts),
      output_limit_(absl::GetFlag(FLAGS_replication_stream_output_limit)) {
  last_async_write_time_ = base::CycleClock::Now();
}

BufferedSocketWriter::~BufferedSocketWriter() {
  if (!cntx_->IsError()) {
    DCHECK_EQ(in_flight_bytes_, 0u);
  }
}

void BufferedSocketWriter::Start(util::FiberSocketBase* dest) {
  CHECK(dest_ == nullptr && dest != nullptr);
  dest_ = dest;
}

std::string BufferedSocketWriter::FormatInternalState() const {
  uint64_t last_async_ms_ago =
      base::CycleClock::ToUsec(base::CycleClock::Now() - last_async_write_time_) / 1000;
  return absl::StrCat(
      "pending_buf_size:", pending_buf_.Size(), " in_flight_bytes:", in_flight_bytes_,
      " total_sent:", total_sent_, " throttle_count:", throttle_count_,
      " total_throttle_wait_usec:", total_throttle_wait_usec_,
      " throttle_waiters:", throttle_waiters_, " last_async_time_ms_ago:", last_async_ms_ago);
}

void BufferedSocketWriter::Write(std::string str) {
  DCHECK(!str.empty());
  DVLOG(3) << "Writing " << str.size() << " bytes";

  pending_buf_.Push(std::move(str));
  AsyncWrite(false);
}

void BufferedSocketWriter::Flush() {
  if (pending_buf_.Size() != 0) {
    AsyncWrite(true);
  }
}

void BufferedSocketWriter::FlushIfIdle(std::chrono::milliseconds period) {
  // We don't want to force the write if the last data was written recently.
  // Data needs to be stalled for `period` duration.
  const uint64_t period_cycles = base::CycleClock::FromUsec(period.count() * 1000);
  if (!pending_buf_.Size() || in_flight_bytes_ > 0 ||
      ((last_async_write_time_ + period_cycles) > base::CycleClock::Now())) {
    return;
  }

  AsyncWrite(true);
}

void BufferedSocketWriter::AsyncWrite(bool force_send) {
  // Can't write data until the previous AsyncWrite finished.
  if (in_flight_bytes_ > 0) {
    return;
  }

  if (!force_send && pending_buf_.FrontBufSize() < opts_.dispatch_threshold) {
    return;
  }

  const auto& cur_buf = pending_buf_.PrepareSendingBuf();

  in_flight_bytes_ = cur_buf.mem_size;
  total_sent_ += in_flight_bytes_;
  last_async_write_time_ = base::CycleClock::Now();

  ServerState::tlocal()->GetEgressThrottler().Record(in_flight_bytes_, false);

  const auto v_size = cur_buf.buf.size();
  absl::InlinedVector<iovec, 8> v(v_size);

  for (size_t i = 0; i < v_size; ++i) {
    const auto* uptr = reinterpret_cast<const uint8_t*>(cur_buf.buf[i].data());
    v[i] = IoVec(io::Bytes(uptr, cur_buf.buf[i].size()));
  }

  dest_->AsyncWrite(v.data(), v.size(),
                    [this, len = in_flight_bytes_](std::error_code ec) { OnCompletion(ec, len); });
}

void BufferedSocketWriter::OnCompletion(std::error_code ec, size_t len) {
  DCHECK_EQ(in_flight_bytes_, len);

  DVLOG(3) << "Completing " << in_flight_bytes_;
  in_flight_bytes_ = 0;
  pending_buf_.Pop();
  if (cntx_->IsRunning()) {
    if (ec) {
      // Enhanced error logging with socket diagnostics for master disconnects
      LOG_EVERY_T(INFO, 1) << "BufferedSocketWriter write error: " << ec.message()
                           << " (code: " << ec.value() << ", category: " << ec.category().name()
                           << ")";

      LogTcpSocketDiagnostics(dest_);

      cntx_->ReportError(ec);
    } else if (!pending_buf_.Empty()) {
      AsyncWrite(false);
    }
  }

  // notify Throttle or WaitForInflightToComplete that waits
  // for all the completions to finish.
  // Throttle can run from multiple fibers in the journal thread.
  // For example, from Heartbeat calling TriggerJournalWriteToSink to flush potential
  // expiration deletions and there are other cases as well.
  waker_.notifyAll();
}

void BufferedSocketWriter::Throttle() {
  if (!cntx_->IsRunning() || !IsStalled())
    return;

  ++throttle_count_;
  ++throttle_waiters_;

  const auto start = chrono::steady_clock::now();
  const auto next = start + chrono::milliseconds(absl::GetFlag(FLAGS_replication_timeout));
  auto log_start = start;
  size_t inflight_start = in_flight_bytes_;
  size_t sent_start = total_sent_;

  // Please note that Throttle is unfair. Specifically with several producers pushing data
  // to this writer, one of them may be stalled and the other will be able to
  // progress indefinitely. The stalled producer will be woken up only to verify again that the
  // other one succeeded to push data before it.
  // We currently do not solve this problem, but at least we will be more verbose about it.
  std::cv_status status = waker_.await_until(
      [&] {
        bool finished = !IsStalled() || !cntx_->IsRunning();
        if (finished)
          return finished;

        // Log every second that we are stalled and for how long.
        auto current = chrono::steady_clock::now();
        if (current - log_start > 1000ms) {
          log_start = current;
          LOG(WARNING) << "Waiting for "
                       << chrono::duration_cast<chrono::milliseconds>(current - start).count()
                       << "ms " << ThisFiber::GetName();
        }

        return false;
      },
      next);

  --throttle_waiters_;
  total_throttle_wait_usec_ +=
      chrono::duration_cast<chrono::microseconds>(chrono::steady_clock::now() - start).count();
  if (status == std::cv_status::timeout) {
    LOG(WARNING) << "Stream timed out, inflight bytes/sent start: " << inflight_start << "/"
                 << sent_start << ", end: " << in_flight_bytes_ << "/" << total_sent_;
    LogTcpSocketDiagnostics(dest_);
    cntx_->ReportError("BufferedSocketWriter write operation timeout");
  }
}

void BufferedSocketWriter::WaitForInflightToComplete(bool with_timeout) {
  const auto start = chrono::steady_clock::now();
  const auto max_timeout = start + chrono::milliseconds(absl::GetFlag(FLAGS_replication_timeout));
  while (in_flight_bytes_) {
    auto next = chrono::steady_clock::now() + 1s;
    std::cv_status status =
        waker_.await_until([this] { return this->in_flight_bytes_ == 0; }, next);
    LOG_IF(WARNING, status == std::cv_status::timeout)
        << "Waiting for inflight bytes " << in_flight_bytes_;

    if (next >= max_timeout) {
      LogTcpSocketDiagnostics(dest_);
      if (with_timeout) {
        cntx_->ReportError("BufferedSocketWriter write operation timeout");
        break;
      } else {
        LOG(WARNING) << "WaitForInflightToComplete timed out with " << in_flight_bytes_
                     << " inflight bytes remaining";
      }
    }
  }
}

bool BufferedSocketWriter::IsStalled() const {
  return pending_buf_.Size() >= output_limit_;
}

}  // namespace dfly
