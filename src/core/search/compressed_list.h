#pragma once

#include <absl/types/span.h>

#include <cstdint>
#include <iterator>
#include <optional>
#include <vector>

namespace dfly::search {

// A list of sorted unique integers with reduced memory usage.
// Only differences between successive elements are stored
// in a variable length encoding.
class CompressedList {
 public:
  // Const access iterator that decodes the compressed list on traversal
  struct Iterator {
    friend class CompressedList;

    // To make it work with std container contructors
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
    void ReadNext();  // Decode next value to stash

    std::optional<uint32_t> stash_;
    absl::Span<const uint8_t> diffs_;
  };

  // Output iterator to build compressed list from sorted range
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
  friend struct SortedBackInserter;

 public:
  Iterator begin() const;
  Iterator end() const;

  void Insert(uint32_t value);  // Insert arbitrary element, needs to scan while list
  void Remove(uint32_t value);  // Remove arbitrary element, needs to scan while list

  size_t Size() const;
  size_t ByteSize() const;

 private:
  struct EntryLocation {
    uint32_t value;                       // Value or 0
    uint32_t prev_value;                  // Preceding value or 0
    absl::Span<const uint8_t> diff_span;  // Location of value encoded diff, empty if none read
  };

 private:
  // Find EntryLocation of first entry that is not less than value (std::lower_bound)
  EntryLocation LowerBound(uint32_t value) const;

  // Push back difference without any decoding. Used only for efficient contruction from sorted list
  void PushBackDiff(uint32_t diff);

  // Encode interger with variable length encoding into buf and return written subspan
  static absl::Span<uint8_t> WriteVarLen(uint32_t value, absl::Span<uint8_t> buf);

  // Decode integer with variable length encoding from source
  static std::pair<uint32_t /*value*/, size_t /*read*/> ReadVarLen(
      absl::Span<const uint8_t> source);

 private:
  uint32_t size_{0};  // TODO: Need to check if we need this at all (to save space)
  std::vector<uint8_t> diffs_{};
};

}  // namespace dfly::search
