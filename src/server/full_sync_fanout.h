// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <system_error>
#include <unordered_map>
#include <vector>

#include "io/io.h"
#include "util/fibers/synchronization.h"

namespace util {
class FiberSocketBase;
}  // namespace util

namespace dfly {

// A per-shard asynchronous fanout sink for a full-sync batch. Snapshot data is copied once into
// shared chunks; each replica keeps references only to the chunks it has not sent yet. All methods
// run on the owning shard's proactor.
class FullSyncFanout : public io::Sink, public std::enable_shared_from_this<FullSyncFanout> {
 public:
  using MemberId = uint32_t;
  using DropCallback = std::function<void(MemberId)>;

  FullSyncFanout(size_t per_member_limit_bytes, DropCallback on_member_drop);
  ~FullSyncFanout() override;

  FullSyncFanout(const FullSyncFanout&) = delete;
  FullSyncFanout& operator=(const FullSyncFanout&) = delete;

  // The socket must remain on this shard for the member's lifetime.
  void AddMember(MemberId id, util::FiberSocketBase* socket);
  void RemoveMember(MemberId id);
  void RemoveAllMembers();

  bool HasMember(MemberId id) const;
  bool Empty() const;

  // Queue bytes after a member's shared stream. The callback runs once those bytes reach its
  // socket, or with an error if the member is dropped.
  void WriteToMember(MemberId id, io::Bytes data, io::AsyncResultCb on_done = {});

  // Queue bytes for all active members. If a queue reaches its limit, waits for recent output
  // progress; a member that remains blocked for replication_timeout is dropped.
  io::Result<size_t> WriteSome(const iovec* v, uint32_t len) final;

  // Returns the time of the last successful write progress, or -1 when this member has no output.
  int64_t LastWriteTime(MemberId id) const;

  // Counts shared payload storage once, plus per-member queue entries.
  size_t UsedBytes() const;

 private:
  struct BufferAccounting;
  struct Chunk;
  struct QueueEntry;
  struct Member;

  std::vector<std::shared_ptr<Chunk>> CopyChunks(const iovec* v, uint32_t len);
  bool WaitForQueueSpace(MemberId id, size_t bytes);
  void QueueForMember(MemberId id, const std::vector<std::shared_ptr<Chunk>>& chunks,
                      io::AsyncResultCb on_done);
  void StartNext(MemberId id);
  void OnWriteDone(MemberId id, std::error_code ec);
  void DropMember(MemberId id, std::error_code ec, bool notify_owner);
  void FinishMemberCallback(io::AsyncResultCb& callback, std::error_code ec);

  size_t per_member_limit_bytes_;
  DropCallback on_member_drop_;
  std::shared_ptr<BufferAccounting> accounting_;
  std::unordered_map<MemberId, std::unique_ptr<Member>> members_;
  util::fb2::EventCount queue_progress_;
};

}  // namespace dfly
