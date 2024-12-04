// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/inlined_vector.h>

#include <algorithm>
#include <deque>

namespace dfly {

class PendingBuf {
 public:
  struct Buf {
    size_t mem_size = 0;
    absl::InlinedVector<std::string, 8> buf;

#ifdef UIO_MAXIOV
    static constexpr size_t max_buf_size = UIO_MAXIOV;
#elif
    static constexpr size_t max_buf_size = 1024;
#endif
  };

  PendingBuf() : bufs_(1) {
  }

  bool empty() const {
    return std::all_of(bufs_.begin(), bufs_.end(), [](const auto& b) { return b.buf.empty(); });
  }

  void push(std::string str) {
    DCHECK(!bufs_.empty());
    if (bufs_.back().buf.size() == Buf::max_buf_size) {
      bufs_.emplace_back();
    }
    auto& fron_buf = bufs_.back();

    fron_buf.mem_size += str.size();
    fron_buf.buf.push_back(std::move(str));
  }

  // should be called to get the next buffer for sending
  const Buf& PrepareSendingBuf() {
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

  size_t size() const {
    return std::accumulate(bufs_.begin(), bufs_.end(), 0,
                           [](size_t s, const auto& b) { return s + b.mem_size; });
  }

 private:
  std::deque<Buf> bufs_;
};

}  // namespace dfly
