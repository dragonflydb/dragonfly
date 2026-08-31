// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/proactor_read_buffer.h"

#include <utility>

#include "base/logging.h"
#include "util/fibers/fibers.h"

namespace facade {

ProactorReadBuffer::ScopedBorrow::ScopedBorrow(ProactorReadBuffer* read_buffer)
    : proactor_read_buffer_(read_buffer) {
  DCHECK(proactor_read_buffer_);
}

ProactorReadBuffer::ScopedBorrow& ProactorReadBuffer::ScopedBorrow::operator=(
    ScopedBorrow&& other) noexcept {
  if (this != &other) {
    Release();
    proactor_read_buffer_ = std::exchange(other.proactor_read_buffer_, nullptr);
  }
  return *this;
}

io::IoBuf& ProactorReadBuffer::ScopedBorrow::buf() {
  assert(proactor_read_buffer_);
  return *proactor_read_buffer_->io_buf_;
}

void ProactorReadBuffer::ScopedBorrow::Release() {
  if (!proactor_read_buffer_)
    // This is possible by definition since the ScopedBorrow might be moved from.
    return;

  auto& io_buf = *proactor_read_buffer_->io_buf_;
  CHECK(proactor_read_buffer_->in_use_);
  DCHECK_EQ(io_buf.InputLen(), 0u);
  DCHECK_EQ(io_buf.AppendLen(), io_buf.Capacity());
  DCHECK_EQ(io_buf.Capacity(), proactor_read_buffer_->capacity_);
  DCHECK_EQ(util::fb2::FiberSwitchEpoch(), proactor_read_buffer_->switch_epoch_);
  io_buf.Clear();
  proactor_read_buffer_->in_use_ = false;
  proactor_read_buffer_->owner_conn_id_ = ProactorReadBuffer::kNoOwner;
  proactor_read_buffer_ = nullptr;
}

void ProactorReadBuffer::Init(size_t capacity) {
  CHECK(!io_buf_);  // can init only once.
  CHECK_GT(capacity, 0u);
  // IoBuf may round capacity up. Keep its actual allocation for later verification.
  io_buf_.emplace(capacity);
  capacity_ = io_buf_->Capacity();
}

void ProactorReadBuffer::Reset() {
  DCHECK(!in_use_);
  io_buf_.reset();
  capacity_ = 0;
  owner_conn_id_ = kNoOwner;
  switch_epoch_ = 0;
}

size_t ProactorReadBuffer::Capacity() const {
  DCHECK(io_buf_);
  return io_buf_->Capacity();
}

bool ProactorReadBuffer::in_use() const {
  DCHECK(io_buf_);
  return in_use_;
}

size_t ProactorReadBuffer::InputLen() const {
  DCHECK(io_buf_);
  return io_buf_->InputLen();
}

uint64_t ProactorReadBuffer::OwnerConnId() const {
  DCHECK(io_buf_);
  return owner_conn_id_;
}

std::optional<ProactorReadBuffer::ScopedBorrow> ProactorReadBuffer::TryBorrow(uint32_t conn_id) {
  CHECK(io_buf_);

  // An in-use buffer is reported to the caller so it can fail safely instead of corrupting input.
  if (in_use_)
    return std::nullopt;

  DCHECK_EQ(io_buf_->InputLen(), 0u);
  DCHECK_EQ(io_buf_->AppendLen(), io_buf_->Capacity());
  DCHECK_EQ(io_buf_->Capacity(), capacity_);

  in_use_ = true;
  owner_conn_id_ = conn_id;
  switch_epoch_ = util::fb2::FiberSwitchEpoch();
  return ScopedBorrow{this};
}

}  // namespace facade
