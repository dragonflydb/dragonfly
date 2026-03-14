// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/inlined_vector.h>

#include <array>
#include <cstdint>
#include <string>

#include "io/io.h"

namespace dfly {

enum class ChunkTag : uint8_t {
  Baseline = 0,
};

struct TaggedChunkHeader {
  static constexpr std::size_t kHeaderSize = 8;
  ChunkTag tag;
  uint32_t payload_size;
  std::array<char, 8> Serialize() const;
  static io::Result<TaggedChunkHeader> Deserialize(const char* buffer);
};

[[nodiscard]] std::error_code PrependChunkHeader(ChunkTag tag, std::string* payload);

// For each message, reads header from upstream and then returns the bytes without header
class TagStrippingSource : public io::Source {
 public:
  explicit TagStrippingSource(Source* upstream) : upstream_{upstream} {
  }

  TagStrippingSource(const TagStrippingSource&) = delete;
  TagStrippingSource& operator=(const TagStrippingSource&) = delete;
  TagStrippingSource(TagStrippingSource&&) = delete;
  TagStrippingSource& operator=(TagStrippingSource&&) = delete;

  io::Result<unsigned long> ReadSome(const iovec* v, uint32_t len) override;

 private:
  // Reads the header (tag, payload size) off the upstream. Sets payload size for later reads which
  // will be sent to downstream readers.
  io::Result<unsigned long> ReadHeader();

  // Copies the given iovec collection, caps to remaining_payload_bytes_, so the next read ends at
  // most at the current chunk boundary.
  absl::InlinedVector<iovec, 4> GetCappedVec(const iovec* v, uint32_t len) const;

  Source* upstream_;
  std::array<char, TaggedChunkHeader::kHeaderSize> header_{};

  uint32_t remaining_payload_bytes_ = 0;
  bool eof_{false};
};

}  // namespace dfly
