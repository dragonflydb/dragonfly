// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/inlined_vector.h>

#include <deque>
#include <numeric>
#include <string_view>

namespace dfly {

class PendingBuf {
 public:
  struct Buf {
#ifdef UIO_MAXIOV
    static constexpr size_t kMaxBufSize = UIO_MAXIOV;
#else
    static constexpr size_t kMaxBufSize = 1024;
#endif
    size_t buf_size_ = 0;
    absl::InlinedVector<uint8_t, kMaxBufSize> buf_;
  };

  bool Empty() const {
    return bufs_.empty();
  }

  void Push(std::string str) {
    if (str.size() > Buf::kMaxBufSize) {
      // Doesn't fit into single buf container so we need to split it into mulitple chunkss
      PushHuge(str);
    } else {
      if (bufs_.empty() || (bufs_.back().buf_size_ + str.size() >= Buf::kMaxBufSize)) {
        bufs_.emplace_back();
      }
      auto& back_buf = bufs_.back();
      memcpy(back_buf.buf_.data() + back_buf.buf_size_, str.data(), str.size());
      back_buf.buf_size_ += str.size();
    }
    size_ += str.size();
  }

  // should be called to get the next buffer for sending
  const Buf& PrepareSendingBuf() {
    LOG(INFO) << "bufs_.size() " << bufs_.size();
    DCHECK(bufs_.size() >= 1);
    // Adding to the buffer ensures that future `Push()`es will not modify the in-flight buffer
    return bufs_.front();
  }

  // should be called when the buf from PrepareSendingBuf() method was sent
  void Pop() {
    DCHECK(bufs_.size() >= 1);
    size_ -= bufs_.front().buf_size_;
    bufs_.pop_front();
  }

  size_t Size() const {
    return size_;
  }

 private:
  void PushHuge(std::string_view sv) {
    size_t iterations = (sv.size() / Buf::kMaxBufSize);
    size_t bytes_left = (sv.size() & (Buf::kMaxBufSize - 1));
    size_t i = 0;
    for (; i < iterations; ++i) {
      bufs_.emplace_back();
      auto& back_buf = bufs_.back();
      size_t start = i * Buf::kMaxBufSize;
      size_t end = (i + 1) * Buf::kMaxBufSize;
      memcpy(back_buf.buf_.data(), sv.substr(start, end).data(), Buf::kMaxBufSize);
      back_buf.buf_size_ = Buf::kMaxBufSize;
    }
    if (bytes_left) {
      bufs_.emplace_back();
      auto& back_buf = bufs_.back();
      size_t start = i * Buf::kMaxBufSize;
      memcpy(back_buf.buf_.data(), sv.substr(start).data(), bytes_left);
      back_buf.buf_size_ = bytes_left;
    }
  }

  size_t size_ = 0;
  std::deque<Buf> bufs_;
};

}  // namespace dfly
