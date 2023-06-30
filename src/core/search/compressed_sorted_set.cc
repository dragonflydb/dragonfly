#include "core/search/compressed_sorted_set.h"

#include <array>
#include <bitset>

#include "base/logging.h"

namespace dfly::search {

using namespace std;

namespace {

const int kMaxBufferSize = sizeof(CompressedSortedSet::IntType) * 2;

}  // namespace

CompressedSortedSet::ConstIterator::ConstIterator(const CompressedSortedSet& list)
    : stash_{}, diffs_{list.diffs_} {
  ReadNext();
}

CompressedSortedSet::IntType CompressedSortedSet::ConstIterator::operator*() const {
  DCHECK(stash_);
  return *stash_;
}

CompressedSortedSet::ConstIterator& CompressedSortedSet::ConstIterator::operator++() {
  ReadNext();
  return *this;
}

bool operator==(const CompressedSortedSet::ConstIterator& l,
                const CompressedSortedSet::ConstIterator& r) {
  return l.diffs_.data() == r.diffs_.data() && l.diffs_.size() == r.diffs_.size();
}

bool operator!=(const CompressedSortedSet::ConstIterator& l,
                const CompressedSortedSet::ConstIterator& r) {
  return !(l == r);
}

void CompressedSortedSet::ConstIterator::ReadNext() {
  if (diffs_.empty()) {
    stash_ = nullopt;
    last_read_ = {nullptr, 0};
    diffs_ = {nullptr, 0};
    return;
  }

  IntType base = stash_.value_or(0);
  auto [diff, read] = CompressedSortedSet::ReadVarLen(diffs_);

  stash_ = base + diff;
  last_read_ = diffs_.subspan(0, read);
  diffs_.remove_prefix(read);
}

CompressedSortedSet::ConstIterator CompressedSortedSet::begin() const {
  return ConstIterator{*this};
}

CompressedSortedSet::ConstIterator CompressedSortedSet::end() const {
  return ConstIterator{};
}

CompressedSortedSet::SortedBackInserter::SortedBackInserter(CompressedSortedSet* list)
    : last_{0}, list_{list} {
}

CompressedSortedSet::SortedBackInserter& CompressedSortedSet::SortedBackInserter::operator=(
    IntType value) {
  DCHECK_LE(last_, value);
  if (value > last_) {
    list_->PushBackDiff(value - last_);
    last_ = value;
  }
  return *this;
}

// Simply encode difference and add to end of diffs array
void CompressedSortedSet::PushBackDiff(IntType diff) {
  array<uint8_t, kMaxBufferSize> buf;
  auto diff_span = WriteVarLen(diff, absl::MakeSpan(buf));
  diffs_.insert(diffs_.end(), diff_span.begin(), diff_span.end());
}

// Do a linear scan by encoding all diffs to find value
CompressedSortedSet::EntryLocation CompressedSortedSet::LowerBound(IntType value) const {
  auto it = begin(), prev_it = end(), next_it = end();
  while (it != end()) {
    next_it = it;
    if (*it >= value || ++next_it == end())
      break;
    prev_it = it;
    it = next_it;
  }

  return EntryLocation{it.stash_.value_or(0), prev_it.stash_.value_or(0), it.last_read_};
}

// Insert has linear complexity. It tries to find between which two entries A and B the new value V
// needs to be inserted. Then it computes the differences dif1 = V - A and diff2 = B - V that need
// to be stored to encode the triple A V B. Those are stored where diff0 = B - A was previously
// stored, possibly extending the vector
void CompressedSortedSet::Insert(IntType value) {
  auto bound = LowerBound(value);

  // At least one element was read and it's equal to value: return to avoid duplicate
  if (bound.value == value && !bound.diff_span.empty())
    return;

  // We're inserting below unconditionally
  size_++;

  // Value is bigger than any other (or list is empty): append required diff at the end
  if (value > bound.value || bound.diff_span.empty()) {
    PushBackDiff(value - bound.value);
    return;
  }

  // Now the list certainly contains the bound B > V and possibly A < V (or 0 by default),
  // so we need to encode both differences diff1 and diff2
  DCHECK_GT(bound.value, value);
  DCHECK_LE(bound.prev_value, value);

  // Compute and encode new diff1 and diff2 into buf1 and buf2 respectivaly
  array<uint8_t, kMaxBufferSize> buf1, buf2;
  auto diff1_span = WriteVarLen(value - bound.prev_value, absl::MakeSpan(buf1));
  auto diff2_span = WriteVarLen(bound.value - value, absl::MakeSpan(buf2));

  // Extend the location where diff0 is stored with optional zeros before overwriting it
  ptrdiff_t diff_offset = bound.diff_span.data() - diffs_.data();
  size_t required_len = diff1_span.size() + diff2_span.size();
  DCHECK_LE(bound.diff_span.size(), required_len);  // It can't shrink for sure
  diffs_.insert(diffs_.begin() + diff_offset, required_len - bound.diff_span.size(), 0u);

  // Now overwrite diff0 and 0s with the two new differences
  copy(diff1_span.begin(), diff1_span.end(), diffs_.begin() + diff_offset);
  copy(diff2_span.begin(), diff2_span.end(), diffs_.begin() + diff_offset + diff1_span.size());
}

// Remove has linear complexity. It tries to find the element V and its neighbors A and B,
// which are encoded as diff1 = V - A and diff2 = B - V. Adjacently stored diff1 and diff2
// need to be replaced with diff3 = diff1 + diff2s
void CompressedSortedSet::Remove(IntType value) {
  auto bound = LowerBound(value);

  // Nothing was read or the element was not found
  if (bound.diff_span.empty() || bound.value != value)
    return;

  // We're removing below unconditionally
  size_--;

  // Calculate offset where values diff is stored and determine diffs tail
  ptrdiff_t diff_offset = bound.diff_span.data() - diffs_.data();
  auto diffs_tail = absl::MakeSpan(diffs_).subspan(diff_offset + bound.diff_span.size());

  // If it's stored at the end, simply truncate it away
  if (diffs_tail.empty()) {
    diffs_.resize(diffs_.size() - bound.diff_span.size());
    return;
  }

  // Now the list certainly contains a succeeding element B > V and possibly A < V (or 0)
  // Read diff2 and calculate diff3 = diff1 + diff2
  auto [diff2, diff2_read] = ReadVarLen(diffs_tail);
  IntType diff3 = (bound.value - bound.prev_value) + diff2;

  // Encode diff3
  array<uint8_t, kMaxBufferSize> buf;
  auto diff3_buf = WriteVarLen(diff3, absl::MakeSpan(buf));

  // Shrink vector before overwriting
  DCHECK_LE(diff3_buf.size(), diff2_read + bound.diff_span.size());
  size_t to_remove = diff2_read + bound.diff_span.size() - diff3_buf.size();
  diffs_.erase(diffs_.begin() + diff_offset, diffs_.begin() + diff_offset + to_remove);

  // Overwrite diff1/diff2 with new diff3
  copy(diff3_buf.begin(), diff3_buf.end(), diffs_.begin() + diff_offset);
}

size_t CompressedSortedSet::Size() const {
  return size_;
}

size_t CompressedSortedSet::ByteSize() const {
  return diffs_.size();
}

absl::Span<uint8_t> CompressedSortedSet::WriteVarLen(IntType value, absl::Span<uint8_t> buf) {
  buf[0] = (value & 0b11111) << 3;
  value >>= 5;

  uint8_t tail_size = 0;
  while (value > 0) {
    buf[++tail_size] = value & 0xFF;
    value >>= 8;
  }

  buf[0] |= tail_size;
  return buf.first(tail_size + 1);
}

std::pair<CompressedSortedSet::IntType, size_t> CompressedSortedSet::ReadVarLen(
    absl::Span<const uint8_t> source) {
  IntType value = source[0] >> 3;

  uint8_t tail_size = source[0] & 0b111;
  for (uint8_t i = 0; i < tail_size; i++)
    value |= static_cast<IntType>(source[i + 1]) << (5 + i * 8);

  return {value, tail_size + 1};
}

}  // namespace dfly::search
