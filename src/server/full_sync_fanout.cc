// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/full_sync_fanout.h"

#include <absl/time/clock.h>
#include <sys/socket.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <deque>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "base/flags.h"
#include "base/logging.h"
#include "util/fiber_socket_base.h"
#include "util/fibers/fibers.h"

ABSL_DECLARE_FLAG(uint32_t, replication_timeout);

namespace dfly {
namespace {

constexpr size_t kChunkSize = 256 * 1024;

std::error_code MemberDroppedError() {
  return std::make_error_code(std::errc::operation_canceled);
}

std::error_code NoMembersError() {
  return std::make_error_code(std::errc::broken_pipe);
}

int64_t NowNs() {
  return absl::GetCurrentTimeNanos();
}

}  // namespace

struct FullSyncFanout::BufferAccounting {
  std::atomic<size_t> bytes{0};
};

struct FullSyncFanout::Chunk {
  Chunk(std::shared_ptr<BufferAccounting> accounting, std::string data)
      : accounting(std::move(accounting)), data(std::move(data)) {
    accounted_bytes = sizeof(Chunk) + this->data.capacity() + 2 * sizeof(void*);
    this->accounting->bytes.fetch_add(accounted_bytes, std::memory_order_relaxed);
  }

  ~Chunk() {
    accounting->bytes.fetch_sub(accounted_bytes, std::memory_order_relaxed);
  }

  std::shared_ptr<BufferAccounting> accounting;
  std::string data;
  size_t accounted_bytes = 0;
};

struct FullSyncFanout::QueueEntry {
  std::shared_ptr<Chunk> chunk;
  io::AsyncResultCb on_done;
};

struct FullSyncFanout::Member {
  util::FiberSocketBase* socket = nullptr;
  std::deque<QueueEntry> pending;
  std::optional<QueueEntry> in_flight;
  size_t queued_bytes = 0;
  int64_t last_progress_ns = -1;
};

FullSyncFanout::FullSyncFanout(size_t per_member_limit_bytes, DropCallback on_member_drop)
    : per_member_limit_bytes_(std::max<size_t>(1, per_member_limit_bytes)),
      on_member_drop_(std::move(on_member_drop)),
      accounting_(std::make_shared<BufferAccounting>()) {
}

FullSyncFanout::~FullSyncFanout() {
  DCHECK(members_.empty());
}

void FullSyncFanout::AddMember(MemberId id, util::FiberSocketBase* socket) {
  DCHECK(socket);
  DCHECK(!members_.contains(id));

  auto member = std::make_unique<Member>();
  member->socket = socket;
  members_.emplace(id, std::move(member));
}

void FullSyncFanout::RemoveMember(MemberId id) {
  DropMember(id, MemberDroppedError(), false);
}

void FullSyncFanout::RemoveAllMembers() {
  std::vector<MemberId> ids;
  ids.reserve(members_.size());
  for (const auto& [id, _] : members_) {
    ids.push_back(id);
  }
  for (MemberId id : ids) {
    RemoveMember(id);
  }
}

bool FullSyncFanout::HasMember(MemberId id) const {
  return members_.contains(id);
}

bool FullSyncFanout::Empty() const {
  return members_.empty();
}

std::vector<std::shared_ptr<FullSyncFanout::Chunk>> FullSyncFanout::CopyChunks(const iovec* v,
                                                                               uint32_t len) {
  std::vector<std::shared_ptr<Chunk>> chunks;
  for (uint32_t i = 0; i < len; ++i) {
    const char* src = reinterpret_cast<const char*>(v[i].iov_base);
    size_t remaining = v[i].iov_len;
    while (remaining > 0) {
      size_t part_size = std::min(remaining, kChunkSize);
      chunks.push_back(std::make_shared<Chunk>(accounting_, std::string(src, part_size)));
      src += part_size;
      remaining -= part_size;
    }
  }
  return chunks;
}

bool FullSyncFanout::WaitForQueueSpace(MemberId id, size_t bytes) {
  DCHECK_GT(bytes, 0u);

  while (true) {
    auto it = members_.find(id);
    if (it == members_.end()) {
      return false;
    }

    Member& member = *it->second;
    if (member.queued_bytes + bytes <= per_member_limit_bytes_) {
      return true;
    }

    const int64_t timeout_ns = int64_t(absl::GetFlag(FLAGS_replication_timeout)) * 1'000'000LL;
    const int64_t now_ns = NowNs();
    if (member.last_progress_ns < 0 || member.last_progress_ns + timeout_ns <= now_ns) {
      LOG(INFO) << "Dropping full-sync fanout member " << id << " because its output queue of "
                << member.queued_bytes << " bytes made no progress for "
                << absl::GetFlag(FLAGS_replication_timeout) << " ms";
      DropMember(id, std::make_error_code(std::errc::timed_out), true);
      return false;
    }

    const int64_t remaining_ns = member.last_progress_ns + timeout_ns - now_ns;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::nanoseconds(remaining_ns);
    queue_progress_.await_until(
        [this, id, bytes] {
          auto it = members_.find(id);
          return it == members_.end() ||
                 it->second->queued_bytes + bytes <= per_member_limit_bytes_;
        },
        deadline);
  }
}

void FullSyncFanout::QueueForMember(MemberId id, const std::vector<std::shared_ptr<Chunk>>& chunks,
                                    io::AsyncResultCb on_done) {
  auto it = members_.find(id);
  if (it == members_.end()) {
    FinishMemberCallback(on_done, MemberDroppedError());
    return;
  }

  Member& member = *it->second;
  const bool was_idle = !member.in_flight && member.pending.empty();
  for (size_t i = 0; i < chunks.size(); ++i) {
    QueueEntry entry{.chunk = chunks[i], .on_done = {}};
    if (i + 1 == chunks.size()) {
      entry.on_done = std::move(on_done);
    }
    member.pending.emplace_back(std::move(entry));
    accounting_->bytes.fetch_add(sizeof(QueueEntry), std::memory_order_relaxed);
  }
  if (was_idle) {
    member.last_progress_ns = NowNs();
  }
}

io::Result<size_t> FullSyncFanout::WriteSome(const iovec* v, uint32_t len) {
  DCHECK(v);
  DCHECK_GT(len, 0u);

  size_t total_bytes = 0;
  for (uint32_t i = 0; i < len; ++i) {
    total_bytes += v[i].iov_len;
  }
  if (members_.empty()) {
    return nonstd::make_unexpected(NoMembersError());
  }
  if (total_bytes == 0) {
    return size_t{0};
  }

  std::vector<MemberId> candidates;
  std::vector<MemberId> recipients;
  candidates.reserve(members_.size());
  recipients.reserve(members_.size());

  for (const auto& [id, _] : members_) {
    candidates.push_back(id);
  }
  for (MemberId id : candidates) {
    if (WaitForQueueSpace(id, total_bytes)) {
      recipients.push_back(id);
    }
  }

  if (recipients.empty()) {
    return nonstd::make_unexpected(NoMembersError());
  }

  auto chunks = CopyChunks(v, len);
  for (MemberId id : recipients) {
    auto it = members_.find(id);
    if (it == members_.end()) {
      continue;
    }
    it->second->queued_bytes += total_bytes;
    QueueForMember(id, chunks, {});
  }
  for (MemberId id : recipients) {
    StartNext(id);
  }

  if (members_.empty()) {
    return nonstd::make_unexpected(NoMembersError());
  }
  return total_bytes;
}

void FullSyncFanout::WriteToMember(MemberId id, io::Bytes data, io::AsyncResultCb on_done) {
  if (data.empty()) {
    FinishMemberCallback(on_done, {});
    return;
  }

  if (!WaitForQueueSpace(id, data.size())) {
    FinishMemberCallback(on_done, MemberDroppedError());
    return;
  }

  iovec vec{.iov_base = const_cast<uint8_t*>(data.data()), .iov_len = data.size()};
  auto chunks = CopyChunks(&vec, 1);
  auto it = members_.find(id);
  if (it == members_.end()) {
    FinishMemberCallback(on_done, MemberDroppedError());
    return;
  }

  it->second->queued_bytes += data.size();
  QueueForMember(id, chunks, std::move(on_done));
  StartNext(id);
}

void FullSyncFanout::StartNext(MemberId id) {
  auto it = members_.find(id);
  if (it == members_.end()) {
    return;
  }

  Member& member = *it->second;
  if (member.in_flight || member.pending.empty()) {
    return;
  }

  member.in_flight.emplace(std::move(member.pending.front()));
  member.pending.pop_front();
  auto chunk = member.in_flight->chunk;
  auto self = shared_from_this();
  member.socket->AsyncWrite(io::Buffer(chunk->data), [self, id, chunk](std::error_code ec) mutable {
    // Socket completions run in the proactor dispatcher. Completion callbacks can acquire fiber
    // mutexes, so process them from a worker fiber instead.
    util::fb2::Fiber("full_sync_fanout_write_done", [self = std::move(self), id, ec] {
      self->OnWriteDone(id, ec);
    }).Detach();
  });
}

void FullSyncFanout::OnWriteDone(MemberId id, std::error_code ec) {
  auto it = members_.find(id);
  if (it == members_.end() || !it->second->in_flight) {
    return;
  }

  Member& member = *it->second;
  QueueEntry entry = std::move(*member.in_flight);
  member.in_flight.reset();
  DCHECK_GE(member.queued_bytes, entry.chunk->data.size());
  member.queued_bytes -= entry.chunk->data.size();
  accounting_->bytes.fetch_sub(sizeof(QueueEntry), std::memory_order_relaxed);

  if (ec) {
    FinishMemberCallback(entry.on_done, ec);
    DropMember(id, ec, true);
    return;
  }

  member.last_progress_ns = NowNs();
  queue_progress_.notifyAll();
  FinishMemberCallback(entry.on_done, {});

  it = members_.find(id);
  if (it == members_.end()) {
    return;
  }
  if (!it->second->in_flight && it->second->pending.empty()) {
    it->second->last_progress_ns = -1;
  }
  StartNext(id);
}

void FullSyncFanout::DropMember(MemberId id, std::error_code ec, bool notify_owner) {
  auto it = members_.find(id);
  if (it == members_.end()) {
    return;
  }

  std::unique_ptr<Member> member = std::move(it->second);
  members_.erase(it);

  queue_progress_.notifyAll();

  if (member->in_flight) {
    FinishMemberCallback(member->in_flight->on_done, ec);
    accounting_->bytes.fetch_sub(sizeof(QueueEntry), std::memory_order_relaxed);
  }
  for (auto& entry : member->pending) {
    FinishMemberCallback(entry.on_done, ec);
    accounting_->bytes.fetch_sub(sizeof(QueueEntry), std::memory_order_relaxed);
  }

  if (notify_owner && member->socket->IsOpen()) {
    std::ignore = member->socket->Shutdown(SHUT_RDWR);
  }
  if (notify_owner && on_member_drop_) {
    on_member_drop_(id);
  }
}

void FullSyncFanout::FinishMemberCallback(io::AsyncResultCb& callback, std::error_code ec) {
  if (!callback) {
    return;
  }
  auto cb = std::move(callback);
  cb(ec);
}

int64_t FullSyncFanout::LastWriteTime(MemberId id) const {
  auto it = members_.find(id);
  return it == members_.end() ? -1 : it->second->last_progress_ns;
}

size_t FullSyncFanout::UsedBytes() const {
  return accounting_->bytes.load(std::memory_order_relaxed);
}

}  // namespace dfly
