#pragma once

#include <absl/types/span.h>

#include <cstdint>
#include <iterator>
#include <optional>
#include <vector>

namespace dfly::search {

// A list of sorted variable length encoded unsigned integers.
// Because the list is sorted, only the differences between the elements are encoded.
// Doesn't allow random access, but saves on memory.
class CompressedList {
 public:
  // Const copied traversal iterator that decodes the list
  struct Iterator {
    friend class CompressedList;

    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = uint32_t;
    using pointer = uint32_t*;
    using reference = uint32_t&;

    Iterator(const CompressedList& list);
    Iterator();

    uint32_t operator*() const;

    Iterator& operator++();

    friend bool operator==(const Iterator& l, const Iterator& r);
    friend bool operator!=(const Iterator& l, const Iterator& r);

   private:
    void ReadNext();

    std::optional<uint32_t> stash_;
    absl::Span<const uint8_t> diffs_;
  };

  // Output iterator to build compressed list from sorted range
  // TODO: Check if its needed. Not sure if to use compressed lists directly for ops (at least want
  // nocopy passing with)
  struct SortedBackInserter {
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = uint32_t;
    using pointer = uint32_t*;
    using reference = uint32_t&;

    SortedBackInserter(CompressedList* list);

    SortedBackInserter& operator*() {
      return *this;
    }
    SortedBackInserter& operator++() {
      return *this;
    }

    SortedBackInserter& operator=(uint32_t value);

   private:
    uint32_t last_;
    CompressedList* list_;
  };

  friend struct Iterator;
  friend struct BackInserter;

 public:
  Iterator begin() const;
  Iterator end() const;

  void Insert(uint32_t value);  // Insert arbitrary element, linear complexity
  void Remove(uint32_t value);  // Remove arbitrary element, linear complexity

  size_t Size() const;
  size_t ByteSize() const;

 private:
  struct BoundLocation {
    uint32_t bound, prev_bound;
    uint32_t bound_diff;
    absl::Span<const uint8_t> left, bound_span;
  };

 private:
  // Find first element that is not less than value. Return its value, its preceding value,
  // the position of its encoded difference and the tail for further reads.
  BoundLocation LowerBound(uint32_t value) const;

  // Push back difference without any decoding. Used only for efficient contruction from sorted list
  void PushBackDiff(uint32_t diff);

  static absl::Span<uint8_t> WriteVarLen(uint32_t value, absl::Span<uint8_t> buf);

  static std::pair<uint32_t /*value*/, size_t /*read*/> ReadVarLen(
      absl::Span<const uint8_t> source);

 private:
  uint32_t size_{0};  // TODO: Need to check if we need this at all
  std::vector<uint8_t> diffs_{};
};

}  // namespace dfly::search
