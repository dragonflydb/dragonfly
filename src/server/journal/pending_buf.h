// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/inlined_vector.h>

#include <deque>
#include <numeric>

namespace dfly {

class PendingBuf {
 public:
  struct Buf {
    size_t mem_size = 0;
    absl::InlinedVector<std::string, 8> buf;

#ifdef UIO_MAXIOV
    static constexpr size_t kMaxBufSize = UIO_MAXIOV;
#else
    static constexpr size_t kMaxBufSize = 1024;
#endif
  };

  PendingBuf() : bufs_(1) {
  }

  bool Empty() const {
    return std::all_of(bufs_.begin(), bufs_.end(), [](const auto& b) { return b.buf.empty(); });
  }

  void Push(std::string str) {
    DCHECK(!bufs_.empty());
    if (bufs_.back().buf.size() == Buf::kMaxBufSize) {
      bufs_.emplace_back();
    }
    auto& fron_buf = bufs_.back();

    fron_buf.mem_size += str.size();
    fron_buf.buf.push_back(std::move(str));
  }

  // should be called to get the next buffer for sending
  const Buf& PrepareSendingBuf() {
    // Adding to the buffer ensures that future `Push()`es will not modify the in-flight buffer
    if (bufs_.size() == 1) {
      bufs_.emplace_back();
    }
    return bufs_.front();
  }

  // should be called when the buf from PrepareSendingBuf() method was sent
  void Pop() {
    DCHECK(bufs_.size() >= 2);
    bufs_.pop_front();
  }

  size_t Size() const {
    return std::accumulate(bufs_.begin(), bufs_.end(), 0,
                           [](size_t s, const auto& b) { return s + b.mem_size; });
  }

 private:
  std::deque<Buf> bufs_;
};

}  // namespace dfly
