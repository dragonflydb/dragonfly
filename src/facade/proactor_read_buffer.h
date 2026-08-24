// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <utility>

#include "io/io_buf.h"

namespace facade {

// Owns the one reusable RESP V2 receive buffer for a proactor thread. The buffer may be borrowed
// by only one connection at a time and must be empty when returned.
class ProactorReadBuffer {
 public:
  // RAII ownership guard: holds exclusive access to the shared buffer. Returning or destroying the
  // borrow verifies that no fiber switch occurred and resets the buffer before another connection
  // can use it.
  class ScopedBorrow {
   public:
    // Non-copyable, move-only. Moving transfers ownership to the destination borrow.
    ScopedBorrow(const ScopedBorrow&) = delete;
    ScopedBorrow& operator=(const ScopedBorrow&) = delete;
    ScopedBorrow(ScopedBorrow&& other) noexcept
        : proactor_read_buffer_(std::exchange(other.proactor_read_buffer_, nullptr)) {
    }
    ScopedBorrow& operator=(ScopedBorrow&& other) noexcept;
    ~ScopedBorrow() {
      Release();
    }

    io::IoBuf& buf() {
      return *proactor_read_buffer_->io_buf_;
    }

   private:
    friend class ProactorReadBuffer;
    explicit ScopedBorrow(ProactorReadBuffer* read_buffer);

    // Validates the active borrow, clears its buffer, and makes it available to another connection.
    // Calling Release() on a moved-from borrow is a no-op (since the ownership has moved to another
    // owner).
    void Release();

    ProactorReadBuffer* proactor_read_buffer_ = nullptr;
  };

  void Init(size_t capacity);
  bool IsInitialized() const {
    return io_buf_.has_value();
  }

  size_t Capacity() const;

  bool in_use() const;

  size_t InputLen() const;

  // Should be used for diagnostics only. For correctness, use in_use().
  uint64_t OwnerConnId() const;

  // Creates an RAII guard for exclusive use of the shared buffer by `conn_id`.
  // Returns nullopt when another connection is already using it. The guard records the current
  // fiber-switch epoch. When the guard is destroyed, it verifies that no fiber switch occurred and
  // makes the buffer available for another connection without deallocating it.
  std::optional<ScopedBorrow> TryBorrow(uint32_t conn_id);

 private:
  std::optional<io::IoBuf> io_buf_;

  // Source of truth for the active-borrow state. false exactly when the buffer is available.
  bool in_use_ = false;

  // Client ID recorded by the active borrow, or kNoOwner while the buffer is available.
  // Can be also 0 too (theoretically) since client IDs start at 1 but held in uint32_t which may
  // wrap around.
  static constexpr uint64_t kNoOwner = std::numeric_limits<uint64_t>::max();
  uint64_t owner_conn_id_ = kNoOwner;

  // The configured size of io_buf_, in bytes. Init() sets this value. The code checks that this
  // size does not change while a connection is using io_buf_.
  size_t capacity_ = 0;

  // Fiber-switch epoch captured when ownership begins. Release verifies it is unchanged, proving
  // the exclusive shared-buffer borrow did not suspend.
  uint64_t switch_epoch_ = 0;
};

}  // namespace facade
