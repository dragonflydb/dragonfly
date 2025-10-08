#include "server/tiering/serialized_map.h"

#include <absl/base/internal/endian.h>

#include "base/logging.h"

namespace dfly::tiering {

SerializedMap::Iterator& SerializedMap::Iterator::operator++() {
  slice_.remove_prefix(8 + key_.size() + value_.size());
  Read();
  return *this;
}

SerializedMap::Iterator::Iterator(std::string_view buffer) : slice_{buffer} {
  Read();
}

void SerializedMap::Iterator::Read() {
  if (slice_.empty())
    return;

  uint32_t key_len = absl::little_endian::Load32(slice_.data());
  uint32_t value_len = absl::little_endian::Load32(slice_.data() + 4);
  key_ = {slice_.data() + 8, key_len};
  value_ = {slice_.data() + 8 + key_len, value_len};
}

SerializedMap::SerializedMap(std::string_view slice) {
  size_ = absl::little_endian::Load32(slice.data());
  DCHECK_GT(size_, 0u);
  slice_ = slice;
}

SerializedMap::Iterator SerializedMap::Find(std::string_view key) const {
  return std::find_if(begin(), end(), [key](auto p) { return p.first == key; });
}

SerializedMap::Iterator SerializedMap::begin() const {
  return Iterator{slice_.substr(4)};
}

SerializedMap::Iterator SerializedMap::end() const {
  return Iterator{slice_.substr(slice_.size(), 0)};
}

size_t SerializedMap::size() const {
  return size_;
}

size_t SerializedMap::SerializeSize(SerializedMap::Input input) {
  size_t out = 4;  // 4 byte number of entries
  for (const auto [key, value] : input)
    out += 8 + key.size() + value.size();  // two 4 byte lengths
  return out;
}

size_t SerializedMap::Serialize(Input input, absl::Span<char> buffer) {
  DCHECK_GE(buffer.size(), SerializeSize(input));
  char* ptr = buffer.data();
  absl::little_endian::Store32(ptr, input.size());
  ptr += 4;

  for (const auto [key, value] : input) {
    absl::little_endian::Store32(ptr, key.length());
    ptr += 4;
    absl::little_endian::Store32(ptr, value.length());
    ptr += 4;
    memcpy(ptr, key.data(), key.length());
    ptr += key.length();
    memcpy(ptr, value.data(), value.length());
    ptr += value.length();
  }

  return ptr - buffer.data();
}

}  // namespace dfly::tiering
