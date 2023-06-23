#include "core/search/compressed_list.h"

#include <array>

#include "base/logging.h"

namespace dfly::search {

using namespace std;

CompressedList::Iterator::Iterator(const CompressedList& list) : stash_{}, diffs_{list.diffs_} {
  ReadNext();
}

CompressedList::Iterator::Iterator() : stash_{}, diffs_{nullptr, 0} {
}

uint32_t CompressedList::Iterator::operator*() const {
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
    diffs_ = {nullptr, 0};
    return;
  }

  uint32_t base = stash_.value_or(0);
  auto [diff, read] = CompressedList::ReadVarLen(diffs_);

  stash_ = base + diff;
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

CompressedList::SortedBackInserter& CompressedList::SortedBackInserter::operator=(uint32_t value) {
  DCHECK_LE(last_, value);
  if (value > last_) {
    list_->PushBackDiff(value - last_);
    last_ = value;
  }
  return *this;
}

void CompressedList::PushBackDiff(uint32_t diff) {
  array<uint8_t, 16> buf;
  auto diff_span = WriteVarLen(diff, absl::MakeSpan(buf));
  diffs_.insert(diffs_.end(), diff_span.begin(), diff_span.end());
}

void CompressedList::Remove(uint32_t value) {
}

// Insert has linear complexity. It tries to find between which two elements A and B the new value V
// needs to be inserted. Then it computes the differences dif1 = V - A and diff2 = B - V that need
// to be stored to encode the triple A V B. Those are stored where diff0 = B - A was previously
// stored, possibly extending the vector
void CompressedList::Insert(uint32_t value) {
  // Find lower bound: first element that is not less than value
  // Store also previous element to re-compute both differences
  uint32_t bound = 0, prev_bound = 0;
  // Store remaining elements and span of last element read (for overwriting)
  absl::Span<const uint8_t> diffs_left{diffs_}, last_read{};
  while (bound < value && !diffs_left.empty()) {
    auto [diff, read] = ReadVarLen(diffs_left);
    last_read = diffs_left.subspan(0, read);
    prev_bound = bound;
    bound += diff;
    diffs_left.remove_prefix(read);
  }

  // We have read at least one element and value is already present
  if (bound == value && !last_read.empty())
    return;

  // We're inserting below unconditionally
  size_++;

  // It belongs at the very end: all elements are either less or there are none at all
  if (bound < value || last_read.empty()) {
    PushBackDiff(value - bound);
    return;
  }

  // We need to insert value between `prev_bound` and `bound`, so we compute both differences
  // diff1 and diff2 and encode them
  array<uint8_t, 16> buf1, buf2;
  auto diff1_span = WriteVarLen(value - prev_bound, absl::MakeSpan(buf1));
  auto diff2_span = WriteVarLen(bound - value, absl::MakeSpan(buf2));

  // Calculate offset where `bound` is stored
  ptrdiff_t base_offset = last_read.data() - diffs_.data();

  // Compute how much more space we need to insert two differences and fill it with 0s
  DCHECK_LE(last_read.size(), diff1_span.size() + diff2_span.size());  // It can't shrink for sure
  size_t len_diff = diff1_span.size() + diff2_span.size() - last_read.size();
  diffs_.insert(diffs_.begin() + base_offset, len_diff, 0u);

  // Now overwrite previous diff + padded 0s with the two new differences
  copy(diff1_span.begin(), diff1_span.end(), diffs_.begin() + base_offset);
  copy(diff2_span.begin(), diff2_span.end(), diffs_.begin() + base_offset + diff1_span.size());
}

size_t CompressedList::Size() const {
  return size_;
}

size_t CompressedList::ByteSize() const {
  return diffs_.size();
}

absl::Span<uint8_t> CompressedList::WriteVarLen(uint32_t value, absl::Span<uint8_t> buf) {
  size_t i = 0;
  do {
    uint8_t byte = value & 0x7F;  // 0x7F = 0111 1111b
    value >>= 7;
    if (value != 0) {  // not the last byte?
      byte |= 0x80;    // 0x80 = 1000 0000b
    }
    buf[i++] = byte;
  } while (value != 0);
  return buf.subspan(0, i);
}

std::pair<uint32_t /*value*/, size_t /*read*/> CompressedList::ReadVarLen(
    absl::Span<const uint8_t> source) {
  uint32_t value = 0;
  size_t shift = 0, read = 0;
  for (uint8_t byte : source) {
    read++;
    value |= static_cast<uint32_t>(byte & 0x7F) << shift;
    if ((byte & 0x80) == 0) {  // last byte?
      break;
    }
    shift += 7;
  }
  return {value, read};
}

}  // namespace dfly::search
