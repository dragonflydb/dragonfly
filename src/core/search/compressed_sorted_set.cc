#include "core/search/compressed_sorted_set.h"

#include <array>
#include <bitset>

#include "absl/types/span.h"
#include "base/flit.h"
#include "base/logging.h"

namespace dfly::search {

using namespace std;

namespace {

using VarintBuffer = array<uint8_t, sizeof(CompressedSortedSet::IntType) * 3>;

}  // namespace

CompressedSortedSet::CompressedSortedSet(PMR_NS::memory_resource* mr, bool store_freq)
    : store_freq_{store_freq}, diffs_{mr} {
}

CompressedSortedSet::ConstIterator::ConstIterator(const CompressedSortedSet& list)
    : stash_{}, store_freq_{list.store_freq_}, diffs_{list.diffs_} {
  ReadNext();
}

CompressedSortedSet::IntType CompressedSortedSet::ConstIterator::operator*() const {
  DCHECK(stash_);
  return *stash_;
}

uint32_t CompressedSortedSet::ConstIterator::Freq() const {
  DCHECK(stash_);
  return freq_stash_;
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

// Each entry is encoded as: [diff_varint] or [diff_varint][freq_varint] when store_freq_ is set.
void CompressedSortedSet::ConstIterator::ReadNext() {
  if (diffs_.empty()) {
    stash_ = nullopt;
    freq_stash_ = 0;
    last_read_ = {nullptr, 0};
    diffs_ = {nullptr, 0};
    return;
  }

  IntType base = stash_.value_or(0);
  auto [diff, diff_read] = CompressedSortedSet::ReadVarLen(diffs_);
  size_t total_read = diff_read;

  stash_ = base + diff;

  if (store_freq_) {
    auto [freq, freq_read] = CompressedSortedSet::ReadVarLen(diffs_.subspan(diff_read));
    freq_stash_ = freq;
    total_read += freq_read;
  } else {
    freq_stash_ = 1;
  }

  last_read_ = diffs_.subspan(0, total_read);
  diffs_.remove_prefix(total_read);
}

CompressedSortedSet::ConstIterator CompressedSortedSet::begin() const {
  return ConstIterator{*this};
}

CompressedSortedSet::ConstIterator CompressedSortedSet::end() const {
  return ConstIterator{};
}

// Encode difference (and optionally freq) and add to end of diffs array
void CompressedSortedSet::PushBackDiff(IntType diff, uint32_t freq) {
  size_++;

  VarintBuffer buf;
  auto diff_span = WriteVarLen(diff, absl::MakeSpan(buf));
  diffs_.insert(diffs_.end(), diff_span.begin(), diff_span.end());

  if (store_freq_) {
    auto freq_span = WriteVarLen(freq, absl::MakeSpan(buf));
    diffs_.insert(diffs_.end(), freq_span.begin(), freq_span.end());
  }
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

  return EntryLocation{.value = it.stash_.value_or(0),
                       .prev_value = prev_it.stash_.value_or(0),
                       .freq = it.freq_stash_,
                       .entry_span = it.last_read_};
}

// Insert has linear complexity. It tries to find between which two entries A and B the new value V
// needs to be inserted. Then it computes the differences dif1 = V - A and diff2 = B - V that need
// to be stored where diff0 = B - A was previously stored, possibly extending the vector.
// Each entry is [diff_varint] or [diff_varint][freq_varint] when store_freq_ is set.
bool CompressedSortedSet::Insert(IntType value, uint32_t freq) {
  if (tail_value_ && *tail_value_ == value)
    return false;

  if (tail_value_ && value > *tail_value_) {
    PushBackDiff(value - *tail_value_, freq);
    tail_value_ = value;
    return true;
  }

  auto bound = LowerBound(value);

  // At least one element was read and it's equal to value: return to avoid duplicate
  if (bound.value == value && !bound.entry_span.empty())
    return false;

  // Value is bigger than any other (or list is empty): append required diff at the end
  if (value > bound.value || bound.entry_span.empty()) {
    PushBackDiff(value - bound.value, freq);
    tail_value_ = value;
    return true;
  }

  size_++;

  // Now the list certainly contains the bound B > V and possibly A < V (or 0 by default),
  // so we need to encode both new entries replacing the old one
  DCHECK_GT(bound.value, value);
  DCHECK_LE(bound.prev_value, value);

  VarintBuffer buf1, buf2, buf3, buf4;
  auto diff1_span = WriteVarLen(value - bound.prev_value, absl::MakeSpan(buf1));
  auto diff2_span = WriteVarLen(bound.value - value, absl::MakeSpan(buf3));

  absl::Span<uint8_t> freq1_span, freq2_span;
  if (store_freq_) {
    freq1_span = WriteVarLen(freq, absl::MakeSpan(buf2));
    freq2_span = WriteVarLen(bound.freq, absl::MakeSpan(buf4));
  }

  // Extend the location where the old entry is stored with optional zeros before overwriting it
  ptrdiff_t entry_offset = bound.entry_span.data() - diffs_.data();
  size_t required_len =
      diff1_span.size() + freq1_span.size() + diff2_span.size() + freq2_span.size();
  DCHECK_LE(bound.entry_span.size(), required_len);  // It can't shrink for sure
  diffs_.insert(diffs_.begin() + entry_offset, required_len - bound.entry_span.size(), 0u);

  // Now overwrite old entry with the two new entries
  auto out = diffs_.begin() + entry_offset;
  out = copy(diff1_span.begin(), diff1_span.end(), out);
  out = copy(freq1_span.begin(), freq1_span.end(), out);
  out = copy(diff2_span.begin(), diff2_span.end(), out);
  copy(freq2_span.begin(), freq2_span.end(), out);

  return true;
}

// Remove has linear complexity. It tries to find the element V and its neighbors A and B.
// Adjacent diffs need to be replaced with diff3 = diff1 + diff2, preserving B's freq.
bool CompressedSortedSet::Remove(IntType value) {
  auto bound = LowerBound(value);

  // Nothing was read or the element was not found
  if (bound.entry_span.empty() || bound.value != value)
    return false;

  // We're removing below unconditionally
  size_--;

  // Calculate offset where entry is stored and determine tail
  ptrdiff_t entry_offset = bound.entry_span.data() - diffs_.data();
  auto diffs_tail = absl::MakeSpan(diffs_).subspan(entry_offset + bound.entry_span.size());

  // If it's stored at the end, simply truncate it away
  if (diffs_tail.empty()) {
    diffs_.resize(diffs_.size() - bound.entry_span.size());
    tail_value_ = bound.prev_value;
    if (diffs_.empty())
      tail_value_ = nullopt;
    return true;
  }

  // Now the list certainly contains a succeeding element B > V and possibly A < V (or 0)
  // Read diff2 (and freq2 if stored) from the next entry
  auto [diff2, diff2_read] = ReadVarLen(diffs_tail);
  size_t next_entry_size = diff2_read;
  uint32_t freq2 = 1;
  if (store_freq_) {
    auto [f, freq2_read] = ReadVarLen(diffs_tail.subspan(diff2_read));
    freq2 = f;
    next_entry_size += freq2_read;
  }

  IntType diff3 = (bound.value - bound.prev_value) + diff2;

  // Encode diff3 (and optionally freq2, preserving the next entry's freq)
  VarintBuffer buf_diff, buf_freq;
  auto diff3_buf = WriteVarLen(diff3, absl::MakeSpan(buf_diff));
  absl::Span<uint8_t> freq2_buf;
  if (store_freq_)
    freq2_buf = WriteVarLen(freq2, absl::MakeSpan(buf_freq));

  // Shrink vector before overwriting
  size_t old_size = next_entry_size + bound.entry_span.size();
  size_t new_size = diff3_buf.size() + freq2_buf.size();
  DCHECK_LE(new_size, old_size);
  size_t to_remove = old_size - new_size;
  diffs_.erase(diffs_.begin() + entry_offset, diffs_.begin() + entry_offset + to_remove);

  // Overwrite with new diff3 (+freq2 if stored)
  auto out = diffs_.begin() + entry_offset;
  out = copy(diff3_buf.begin(), diff3_buf.end(), out);
  if (store_freq_)
    copy(freq2_buf.begin(), freq2_buf.end(), out);

  return true;
}

void CompressedSortedSet::Merge(CompressedSortedSet&& other) {
  DCHECK_EQ(store_freq_, other.store_freq_);
  // Quadratic compexity in theory, but in practice used only to merge with larger values.
  // Tail insert optimization makes it linear
  for (auto it = other.begin(); it != other.end(); ++it)
    Insert(*it, it.Freq());
}

std::pair<CompressedSortedSet, CompressedSortedSet> CompressedSortedSet::Split() && {
  DCHECK_GT(Size(), 5u);

  CompressedSortedSet second(diffs_.get_allocator().resource(), store_freq_);

  // Move iterator to middle position and save size of diffs tail
  auto it = begin();
  std::advance(it, (size_ - 1) / 2);

  // Save last value in the first set
  tail_value_ = *it;
  ++it;

  size_t keep_bytes = it.last_read_.data() - diffs_.data();

  // Copy second half into second set
  for (; it != end(); ++it)
    second.Insert(*it, it.Freq());

  // Erase diffs tail
  diffs_.resize(keep_bytes);
  size_ -= second.Size();

  return std::make_pair(std::move(*this), std::move(second));
}

// The leftmost three bits of the first byte store the number of additional bytes. All following
// bits store the number itself.
absl::Span<uint8_t> CompressedSortedSet::WriteVarLen(IntType value, absl::Span<uint8_t> buf) {
  // TODO: fix flit encoding of large numbers
  size_t written = base::flit::EncodeT(static_cast<uint64_t>(value), buf.data());
  return buf.first(written);
}

std::pair<CompressedSortedSet::IntType, size_t> CompressedSortedSet::ReadVarLen(
    absl::Span<const uint8_t> source) {
  uint64_t out = 0;
  size_t read = 0;

  // We need this because ParseT may read 8 bytes even if source can be less than that
  // due to the encoding and we end up accessing an invalid memory location.
  // (not really a bug because ParseT ignores the extra bytes it reads).
  if (source.size() < 8) {
    VarintBuffer ranged_source{0};
    memcpy(&ranged_source, source.data(), source.size());
    read = base::flit::ParseT(ranged_source.data(), &out);
  } else {
    read = base::flit::ParseT(source.data(), &out);
  }

  CHECK_LE(out, numeric_limits<IntType>::max());
  return {out, read};
}

}  // namespace dfly::search
