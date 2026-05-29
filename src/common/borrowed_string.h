// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <utility>

namespace cmn {

// Move-only owning handle for a borrowed bulk string. The bytes in `encoded`
// remain valid as long as this object is alive — the destructor releases the
// underlying pin via BorrowedStringOps::Get()->Release().
//
// For NONE_ENC (encoding == 0): `encoded` is the user-visible bytes;
// `decoded_size` is unused (caller treats it as `encoded.size()`).
// For ASCII1/ASCII2_ENC: `encoded` is the packed source; the consumer
// decodes chunk-by-chunk via BorrowedStringOps::Get()->DecodeChunk().

class BorrowedStringOps;

class BorrowedString {
  friend class BorrowedStringOps;

 public:
  // BorrowedString() noexcept = default;
  BorrowedString(std::string_view encoded, size_t decoded_size, uint8_t encoding,
                 void* pin) noexcept
      : encoded_(encoded), pin_(pin), encoding_(encoding) {
  }

  BorrowedString(const BorrowedString&) = delete;
  BorrowedString& operator=(const BorrowedString&) = delete;

  BorrowedString(BorrowedString&& o) noexcept {
    MoveFrom(std::move(o));
  }

  BorrowedString& operator=(BorrowedString&& o) noexcept {
    if (this != &o) {
      Unpin();
      MoveFrom(std::move(o));
    }
    return *this;
  }

  ~BorrowedString() noexcept {
    Unpin();
  }

  void* pin() const noexcept {
    return pin_;
  }

  bool IsEncoded() const noexcept {
    return encoding_ != 0;
  }

  std::string_view view() const noexcept {
    return encoded_;
  }

  uint8_t encoding() const noexcept {
    return encoding_;
  }

 private:
  // Release the pin if held. Delegates to BorrowedStringOps::Get()->Release().
  void Unpin() noexcept;

  void MoveFrom(BorrowedString&& o) noexcept {
    encoded_ = o.encoded_;
    encoding_ = o.encoding_;
    pin_ = std::exchange(o.pin_, nullptr);
  }

  std::string_view encoded_;
  void* pin_ = nullptr;
  uint8_t encoding_ = 0;
};

// Abstract interface registered once (by CompactObj) to provide pin lifecycle
// and ASCII decode operations without coupling common/ or facade/ to core/.
// Designed to be a singleton accessed via BorrowedStringOps::Get().
class BorrowedStringOps {
 public:
  virtual ~BorrowedStringOps() = default;

  // Consume the borrow: extract pin via bs.ReleasePin() and decrement its
  // refcount. Called by BorrowedString::Unpin() / destructor.
  void Release(BorrowedString&& bs) noexcept {
    ReleaseInternal(bs);
    bs.pin_ = nullptr;  // ensure pin is released even if ReleaseInternal throws
  }

  // Result of a single DecodeChunk call. `src_consumed` is the number of
  // source bytes the impl read from `bs.view()` starting at `src_offset`;
  // `dec_written` is the number of decoded bytes the impl wrote into
  // `dest`. Two cursors because source and decoded sizes differ in general
  // (e.g. ASCII packs 7 source bytes → 8 decoded bytes).
  struct DecodeResult {
    size_t src_consumed;
    size_t dec_written;
  };

  // Decode a chunk of source [src_offset, src_offset + src_len) from
  // bs.view() into `dest`. The impl picks how much to actually
  // consume/produce — subject to its own alignment rules — and reports both
  // via the returned struct. Caller advances its source cursor by
  // `src_consumed` and the destination cursor by `dec_written`. `dest` must
  // be large enough for the impl to make progress — a buffer big enough for
  // one alignment unit is always sufficient. Only called for non-raw
  // (packed) encodings.
  virtual DecodeResult DecodeChunk(const BorrowedString& bs, size_t src_len, size_t src_offset,
                                   std::span<char> dest) noexcept = 0;

  virtual size_t DecodedSize(const BorrowedString& bs) noexcept = 0;

  // Register the singleton instance. Must be called before any BorrowedString is used.
  static void Set(BorrowedStringOps* ops) noexcept {
    s_ops_ = ops;
  }

  static BorrowedStringOps* Get() noexcept {
    return s_ops_;
  }

 protected:
  virtual void ReleaseInternal(BorrowedString& bs) noexcept = 0;

 private:
  inline static BorrowedStringOps* s_ops_ = nullptr;
};

inline void BorrowedString::Unpin() noexcept {
  if (pin_)
    BorrowedStringOps::Get()->Release(std::move(*this));
}

}  // namespace cmn
