#pragma once

#include <absl/types/span.h>

#include <string_view>

namespace dfly::tiering {

struct SerializedMap {
  struct Iterator {
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = std::pair<std::string_view, std::string_view>;

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

    std::string_view slice_;
    std::string_view key_, value_;
  };

  explicit SerializedMap(std::string_view slice);

  Iterator Find(std::string_view key) const;  // Linear search
  Iterator begin() const;
  Iterator end() const;
  size_t size() const;

  // Input for serialization
  using Input = const absl::Span<const std::pair<std::string_view, std::string_view>>;
  static size_t SerializeSize(Input);  // Upper bound of buffer size for serialization
  static size_t Serialize(Input, absl::Span<char> buffer);

 private:
  size_t size_;
  std::string_view slice_;
};

}  // namespace dfly::tiering
