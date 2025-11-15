#pragma once

#include <absl/types/span.h>

#include <string_view>

namespace dfly::tiering {

// Map built over single continuous byte slice to allow easy read operations.
struct SerializedMap {
  struct Iterator {
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = std::pair<std::string_view, std::string_view>;
    using reference = value_type;
    using pointer = value_type*;

    Iterator& operator++();

    bool operator==(const Iterator& other) const {
      return slice_.data() == other.slice_.data() && slice_.size() == other.slice_.size();
    }

    bool operator!=(const Iterator& other) const {
      return !operator==(other);
    }

    std::pair<std::string_view, std::string_view> operator*() const {
      return {key_, value_};
    }

   private:
    friend struct SerializedMap;

    explicit Iterator(std::string_view buffer);
    void Read();

    std::string_view slice_;  // the part left
    std::string_view key_, value_;
  };

  explicit SerializedMap(std::string_view slice);

  Iterator Find(std::string_view key) const;  // Linear search
  Iterator begin() const;
  Iterator end() const;
  size_t size() const;

  // Number of bytes of pure keys or values
  size_t DataBytes() const;

  // Input for serialization
  using Input = const absl::Span<const std::pair<std::string_view, std::string_view>>;

  // Buffer size required for serialization
  static size_t SerializeSize(Input);

  // Write a slice that can be used to a SerializedMap on top of it.
  // Returns number of bytes written
  static size_t Serialize(Input, absl::Span<char> buffer);

 private:
  size_t size_;
  std::string_view slice_;
};

}  // namespace dfly::tiering
