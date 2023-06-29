#pragma once

#include <absl/types/span.h>

#include <cstdint>
#include <iterator>
#include <optional>
#include <vector>

#include "core/search/base.h"

namespace dfly::search {

// A list of sorted unique integers with reduced memory usage.
// Only differences between successive elements are stored
// in a variable length encoding.
class CompressedList {
 public:
  using IntType = DocId;

  // Const access iterator that decodes the compressed list on traversal
  struct Iterator {
    friend class CompressedList;

    // To make it work with std container contructors
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = IntType;
    using pointer = IntType*;
    using reference = IntType&;

    Iterator(const CompressedList& list);
    Iterator() = default;

    IntType operator*() const;
    Iterator& operator++();

    friend class CompressedList;
    friend bool operator==(const Iterator& l, const Iterator& r);
    friend bool operator!=(const Iterator& l, const Iterator& r);

   private:
    void ReadNext();  // Decode next value to stash

    std::optional<IntType> stash_{};
    absl::Span<const uint8_t> last_read_{};
    absl::Span<const uint8_t> diffs_{};
  };

  // Output iterator to build compressed list from sorted range
  struct SortedBackInserter {
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = IntType;
    using pointer = IntType*;
    using reference = IntType&;

    SortedBackInserter(CompressedList* list);

    SortedBackInserter& operator*() {
      return *this;
    }
    SortedBackInserter& operator++() {
      return *this;
    }

    SortedBackInserter& operator=(IntType value);

   private:
    IntType last_;
    CompressedList* list_;
  };

  friend struct Iterator;
  friend struct SortedBackInserter;

 public:
  Iterator begin() const;
  Iterator end() const;

  void Insert(IntType value);  // Insert arbitrary element, needs to scan whole list
  void Remove(IntType value);  // Remove arbitrary element, needs to scan whole list

  size_t Size() const;
  size_t ByteSize() const;

 private:
  struct EntryLocation {
    IntType value;                        // Value or 0
    IntType prev_value;                   // Preceding value or 0
    absl::Span<const uint8_t> diff_span;  // Location of value encoded diff, empty if none read
  };

 private:
  // Find EntryLocation of first entry that is not less than value (std::lower_bound)
  EntryLocation LowerBound(IntType value) const;

  // Push back difference without any decoding. Used only for efficient construction from sorted
  // list
  void PushBackDiff(IntType diff);

  // Encode integer with variable length encoding into buf and return written subspan
  static absl::Span<uint8_t> WriteVarLen(IntType value, absl::Span<uint8_t> buf);

  // Decode integer with variable length encoding from source
  static std::pair<IntType /*value*/, size_t /*read*/> ReadVarLen(absl::Span<const uint8_t> source);

 private:
  IntType size_{0};
  std::vector<uint8_t> diffs_{};
};

}  // namespace dfly::search
