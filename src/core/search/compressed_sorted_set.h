#pragma once

#include <absl/types/span.h>

#include <cstdint>
#include <iterator>
#include <optional>
#include <vector>

#include "base/logging.h"
#include "base/pmr/memory_resource.h"
#include "core/search/base.h"

namespace dfly::search {

// A list of sorted unique integers with reduced memory usage.
// Only differences between successive elements are stored
// in a variable length encoding.
class CompressedSortedSet {
 public:
  using IntType = DocId;
  using ElementType = IntType;

  // Const access iterator that decodes the compressed list on traversal
  struct ConstIterator {
    friend class CompressedSortedSet;

    // To make it work with std container contructors
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = IntType;
    using pointer = IntType*;
    using reference = IntType&;

    IntType operator*() const;
    ConstIterator& operator++();

    friend class CompressedSortedSet;
    friend bool operator==(const ConstIterator& l, const ConstIterator& r);
    friend bool operator!=(const ConstIterator& l, const ConstIterator& r);

    ConstIterator() = default;

   private:
    explicit ConstIterator(const CompressedSortedSet& list);

    void ReadNext();  // Decode next value to stash

    std::optional<IntType> stash_{};
    absl::Span<const uint8_t> last_read_{};
    absl::Span<const uint8_t> diffs_{};
  };

  using iterator = ConstIterator;

 public:
  explicit CompressedSortedSet(PMR_NS::memory_resource* mr);

  ConstIterator begin() const;
  ConstIterator end() const;

  bool Insert(IntType value);  // Insert arbitrary element, needs to scan whole list
  bool Remove(IntType value);  // Remove arbitrary element, needs to scan whole list

  size_t Size() const {
    return size_;
  }

  size_t ByteSize() const {
    return diffs_.size();
  }

  bool Empty() const {
    return size_ == 0;
  }

  void Clear() {
    size_ = 0;
    tail_value_.reset();
    diffs_.clear();
  }

  // Add all values from other
  void Merge(CompressedSortedSet&& other);

  // Split into two equally sized halves
  std::pair<CompressedSortedSet, CompressedSortedSet> Split() &&;

  IntType Back() const {
    DCHECK(!Empty() && tail_value_.has_value());
    return tail_value_.value();
  }

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
  uint32_t size_{0};

  std::optional<IntType> tail_value_{};
  std::vector<uint8_t, PMR_NS::polymorphic_allocator<uint8_t>> diffs_;
};

}  // namespace dfly::search
