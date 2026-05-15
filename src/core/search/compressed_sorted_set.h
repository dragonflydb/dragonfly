#pragma once

#include <absl/container/inlined_vector.h>
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
    uint32_t Freq() const;
    // Sorted ascending. Empty when the owning set is store_positions_=false.
    absl::Span<const uint32_t> Positions() const;
    ConstIterator& operator++();

    friend class CompressedSortedSet;
    friend bool operator==(const ConstIterator& l, const ConstIterator& r);
    friend bool operator!=(const ConstIterator& l, const ConstIterator& r);

    ConstIterator() = default;

   private:
    explicit ConstIterator(const CompressedSortedSet& list);

    void ReadNext();  // Decode next value to stash

    std::optional<IntType> stash_{};
    uint32_t freq_stash_{0};
    absl::InlinedVector<uint32_t, 4> positions_stash_;
    bool store_freq_{false};
    bool store_positions_{false};
    absl::Span<const uint8_t> last_read_{};
    absl::Span<const uint8_t> diffs_{};
  };

  using iterator = ConstIterator;

 public:
  // Entry layout depends on flags:
  //   neither:       [diff_varint]
  //   store_freq:    [diff_varint][freq_varint]
  //   +positions:    [diff_varint][freq_varint][pos0_varint][pos1_delta_varint]...
  //
  // store_positions implies store_freq (freq tells the reader how many positions follow).
  explicit CompressedSortedSet(PMR_NS::memory_resource* mr, bool store_freq = false,
                               bool store_positions = false);

  ConstIterator begin() const;
  ConstIterator end() const;

  // Insert with term frequency. Returns true if new, false if already present.
  // `positions` must be sorted ascending and have size == freq when store_positions_;
  // it is ignored when store_positions_ is false.
  bool Insert(IntType value, uint32_t freq = 1, absl::Span<const uint32_t> positions = {});
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

  bool StoresFreq() const {
    return store_freq_;
  }

  bool StoresPositions() const {
    return store_positions_;
  }

  // Add all values from other
  void Merge(CompressedSortedSet&& other);

  // Split into two equally sized halves
  std::pair<CompressedSortedSet, CompressedSortedSet> Split() &&;

  IntType Back() const {
    DCHECK(!Empty() && tail_value_.has_value());
    return tail_value_.value();
  }

  static DefragmentResult Defragment([[maybe_unused]] PageUsage* page_usage) {
    return {};
  }

 private:
  struct EntryLocation {
    IntType value;                         // Value or 0
    IntType prev_value;                    // Preceding value or 0
    uint32_t freq;                         // Frequency of the value
    absl::Span<const uint8_t> entry_span;  // Location of encoded diff+freq, empty if none read
  };

 private:
  // Find EntryLocation of first entry that is not less than value (std::lower_bound)
  EntryLocation LowerBound(IntType value) const;

  // Push back difference + freq + positions without any decoding. Used for efficient construction.
  // `positions` is consulted only when store_positions_; expected to have size == freq.
  void PushBackDiff(IntType diff, uint32_t freq, absl::Span<const uint32_t> positions = {});

  // Encode integer with variable length encoding into buf and return written subspan
  static absl::Span<uint8_t> WriteVarLen(IntType value, absl::Span<uint8_t> buf);

  // Decode integer with variable length encoding from source
  static std::pair<IntType /*value*/, size_t /*read*/> ReadVarLen(absl::Span<const uint8_t> source);

  // Bytes consumed by an entry's diff+freq prefix (everything before the optional positions block).
  static size_t HeadSize(absl::Span<const uint8_t> entry_span, bool store_freq);

 private:
  uint32_t size_{0};
  bool store_freq_;       // Whether to encode/decode freq alongside diffs
  bool store_positions_;  // Whether to encode/decode positions; implies store_freq_

  std::optional<IntType> tail_value_{};
  std::vector<uint8_t, PMR_NS::polymorphic_allocator<uint8_t>> diffs_;
};

}  // namespace dfly::search
