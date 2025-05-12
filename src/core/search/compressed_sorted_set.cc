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

CompressedSortedSet::CompressedSortedSet(PMR_NS::memory_resource* mr) : diffs_{mr} {
}

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

// Simply encode difference and add to end of diffs array
void CompressedSortedSet::PushBackDiff(IntType diff) {
  size_++;

  VarintBuffer buf;
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

  return EntryLocation{.value = it.stash_.value_or(0),
                       .prev_value = prev_it.stash_.value_or(0),
                       .diff_span = it.last_read_};
}

// Insert has linear complexity. It tries to find between which two entries A and B the new value V
// needs to be inserted. Then it computes the differences dif1 = V - A and diff2 = B - V that need
// to be stored to encode the triple A V B. Those are stored where diff0 = B - A was previously
// stored, possibly extending the vector
bool CompressedSortedSet::Insert(IntType value) {
  if (tail_value_ && *tail_value_ == value)
    return false;

  if (tail_value_ && value > *tail_value_) {
    PushBackDiff(value - *tail_value_);
    tail_value_ = value;
    return true;
  }

  auto bound = LowerBound(value);

  // At least one element was read and it's equal to value: return to avoid duplicate
  if (bound.value == value && !bound.diff_span.empty())
    return false;

  // Value is bigger than any other (or list is empty): append required diff at the end
  if (value > bound.value || bound.diff_span.empty()) {
    PushBackDiff(value - bound.value);
    tail_value_ = value;
    return true;
  }

  size_++;

  // Now the list certainly contains the bound B > V and possibly A < V (or 0 by default),
  // so we need to encode both differences diff1 and diff2
  DCHECK_GT(bound.value, value);
  DCHECK_LE(bound.prev_value, value);

  // Compute and encode new diff1 and diff2 into buf1 and buf2 respectivaly
  VarintBuffer buf1, buf2;
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

  return true;
}

// Remove has linear complexity. It tries to find the element V and its neighbors A and B,
// which are encoded as diff1 = V - A and diff2 = B - V. Adjacently stored diff1 and diff2
// need to be replaced with diff3 = diff1 + diff2s
bool CompressedSortedSet::Remove(IntType value) {
  auto bound = LowerBound(value);

  // Nothing was read or the element was not found
  if (bound.diff_span.empty() || bound.value != value)
    return false;

  // We're removing below unconditionally
  size_--;

  // Calculate offset where values diff is stored and determine diffs tail
  ptrdiff_t diff_offset = bound.diff_span.data() - diffs_.data();
  auto diffs_tail = absl::MakeSpan(diffs_).subspan(diff_offset + bound.diff_span.size());

  // If it's stored at the end, simply truncate it away
  if (diffs_tail.empty()) {
    diffs_.resize(diffs_.size() - bound.diff_span.size());
    tail_value_ = bound.prev_value;
    if (diffs_.empty())
      tail_value_ = nullopt;
    return true;
  }

  // Now the list certainly contains a succeeding element B > V and possibly A < V (or 0)
  // Read diff2 and calculate diff3 = diff1 + diff2
  auto [diff2, diff2_read] = ReadVarLen(diffs_tail);
  IntType diff3 = (bound.value - bound.prev_value) + diff2;

  // Encode diff3
  VarintBuffer buf;
  auto diff3_buf = WriteVarLen(diff3, absl::MakeSpan(buf));

  // Shrink vector before overwriting
  DCHECK_LE(diff3_buf.size(), diff2_read + bound.diff_span.size());
  size_t to_remove = diff2_read + bound.diff_span.size() - diff3_buf.size();
  diffs_.erase(diffs_.begin() + diff_offset, diffs_.begin() + diff_offset + to_remove);

  // Overwrite diff1/diff2 with new diff3
  copy(diff3_buf.begin(), diff3_buf.end(), diffs_.begin() + diff_offset);

  return true;
}

size_t CompressedSortedSet::Size() const {
  return size_;
}

size_t CompressedSortedSet::ByteSize() const {
  return diffs_.size();
}

void CompressedSortedSet::Merge(CompressedSortedSet&& other) {
  // Quadratic compexity in theory, but in practice used only to merge with larger values.
  // Tail insert optimization makes it linear
  for (int v : other)
    Insert(v);
}

std::pair<CompressedSortedSet, CompressedSortedSet> CompressedSortedSet::Split() && {
  DCHECK_GT(Size(), 5u);

  CompressedSortedSet second(diffs_.get_allocator().resource());

  // Move iterator to middle position and save size of diffs tail
  auto it = begin();
  std::advance(it, size_ / 2);
  size_t keep_bytes = it.last_read_.data() - diffs_.data();

  // Copy second half into second set
  for (; it != end(); ++it)
    second.Insert(*it);

  // Erase diffs tail
  diffs_.resize(keep_bytes);
  tail_value_ = std::nullopt;
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
