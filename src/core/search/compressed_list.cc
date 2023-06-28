#include "core/search/compressed_list.h"

#include <array>

#include "base/logging.h"

namespace dfly::search {

using namespace std;

namespace {

const int kMaxBufferSize = sizeof(CompressedList::IntType) * 2;

}  // namespace

CompressedList::Iterator::Iterator(const CompressedList& list) : stash_{}, diffs_{list.diffs_} {
  ReadNext();
}

CompressedList::IntType CompressedList::Iterator::operator*() const {
  DCHECK(stash_);
  return *stash_;
}

CompressedList::Iterator& CompressedList::Iterator::operator++() {
  ReadNext();
  return *this;
}

bool operator==(const CompressedList::Iterator& l, const CompressedList::Iterator& r) {
  return l.diffs_.data() == r.diffs_.data() && l.diffs_.size() == r.diffs_.size();
}

bool operator!=(const CompressedList::Iterator& l, const CompressedList::Iterator& r) {
  return !(l == r);
}

void CompressedList::Iterator::ReadNext() {
  if (diffs_.empty()) {
    stash_ = nullopt;
    last_read_ = {nullptr, 0};
    diffs_ = {nullptr, 0};
    return;
  }

  IntType base = stash_.value_or(0);
  auto [diff, read] = CompressedList::ReadVarLen(diffs_);

  stash_ = base + diff;
  last_read_ = diffs_.subspan(0, read);
  diffs_.remove_prefix(read);
}

CompressedList::Iterator CompressedList::begin() const {
  return Iterator{*this};
}

CompressedList::Iterator CompressedList::end() const {
  return Iterator{};
}

CompressedList::SortedBackInserter::SortedBackInserter(CompressedList* list)
    : last_{0}, list_{list} {
}

CompressedList::SortedBackInserter& CompressedList::SortedBackInserter::operator=(IntType value) {
  DCHECK_LE(last_, value);
  if (value > last_) {
    list_->PushBackDiff(value - last_);
    last_ = value;
  }
  return *this;
}

// Simply encode difference and add to end of diffs array
void CompressedList::PushBackDiff(IntType diff) {
  array<uint8_t, kMaxBufferSize> buf;
  auto diff_span = WriteVarLen(diff, absl::MakeSpan(buf));
  diffs_.insert(diffs_.end(), diff_span.begin(), diff_span.end());
}

// Do a linear scan by encoding all diffs to find value
CompressedList::EntryLocation CompressedList::LowerBound(IntType value) const {
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
void CompressedList::Insert(IntType value) {
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
void CompressedList::Remove(IntType value) {
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

size_t CompressedList::Size() const {
  return size_;
}

size_t CompressedList::ByteSize() const {
  return diffs_.size();
}

// Encode with simple MSB (Most significant bit) encoding: 7 bits of value + 1 to indicate next byte
absl::Span<uint8_t> CompressedList::WriteVarLen(IntType value, absl::Span<uint8_t> buf) {
  size_t i = 0;
  do {
    uint8_t byte = value & 0x7F;
    value >>= 7;
    if (value != 0)  // If its not the last byte, set continuation bit
      byte |= 0x80;
    buf[i++] = byte;
  } while (value != 0);
  return buf.subspan(0, i);
}

std::pair<CompressedList::IntType, size_t> CompressedList::ReadVarLen(
    absl::Span<const uint8_t> source) {
  IntType value = 0;
  size_t read = 0;
  for (uint8_t byte : source) {
    value |= static_cast<IntType>(byte & 0x7F) << (read * 7);
    read++;
    if ((byte & 0x80) == 0)
      break;
  }
  return {value, read};
}

}  // namespace dfly::search
