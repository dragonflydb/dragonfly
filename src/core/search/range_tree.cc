#include "core/search/range_tree.h"

namespace dfly::search {

namespace {

// TODO: remove this
long long SafeCastFromDoubleToLongLong(double value) {
  if (std::isnan(value)) {
    return 0;
  }
  if (value >= static_cast<double>(std::numeric_limits<long long>::max())) {
    return std::numeric_limits<long long>::max();
  }
  if (value <= static_cast<double>(std::numeric_limits<long long>::min())) {
    return std::numeric_limits<long long>::min();
  }
  return static_cast<long long>(value);
}

}  // namespace

RangeTree::RangeTree(PMR_NS::memory_resource* mr) : entries_{mr} {
  // TODO: at the beggining create more blocks
  entries_.insert(
      {{std::numeric_limits<RangeNumber>::min(), std::numeric_limits<RangeNumber>::max()},
       RangeBlock{entries_.get_allocator().resource(), kBlockSize}});
}

void RangeTree::Add(DocId id, double value) {
  DCHECK(std::isfinite(value)) << "Value must be finite, got: " << value;

  auto it = FindRangeBlock(value);
  RangeBlock& block = it->second;

  [[maybe_unused]] auto insert_result = block.Insert({id, value});
  DCHECK(insert_result);

  if (block.Size() <= kMaxRangeBlockSize) {
    return;
  }

  SplitBlock(std::move(it));
}

void RangeTree::Remove(DocId id, double value) {
  DCHECK(std::isfinite(value)) << "Value must be finite, got: " << value;

  auto it = FindRangeBlock(value);
  RangeBlock& block = it->second;

  [[maybe_unused]] auto remove_result = block.Remove({id, value});
  DCHECK(remove_result);

  // TODO: maybe merging blocks if they are too small
  // The problem that for each mutable operation we do Remove and then Add,
  // So we can do merge and split for one operation.
  // Or in common cases users do not remove a lot of documents?
}

RangeResult RangeTree::Range(double l, double r) const {
  DCHECK(l <= r);

  auto it_l = FindRangeBlock(l);
  auto it_r = FindRangeBlock(r);

  absl::InlinedVector<const RangeBlock*, 10> blocks;
  for (auto it = it_l;; ++it) {
    blocks.push_back(&it->second);
    if (it == it_r) {
      break;
    }
  }

  DCHECK(!blocks.empty());

  if (blocks.size() == 1) {
    return {*blocks[0], {l, r}};
  }

  const std::optional<double> leftmost_block_range =
      static_cast<double>(it_l->first.first) == l ? std::nullopt : std::make_optional(l);
  const std::optional<double> rightmost_block_range =
      static_cast<double>(it_r->first.second) == r ? std::nullopt : std::make_optional(r);
  return {std::move(blocks), leftmost_block_range, rightmost_block_range};
}

RangeResult RangeTree::GetAllDocIds() const {
  absl::InlinedVector<const RangeBlock*, 10> blocks;
  blocks.reserve(entries_.size());

  for (const auto& entry : entries_) {
    blocks.push_back(&entry.second);
  }

  return RangeResult{std::move(blocks)};
}

RangeTree::Map::iterator RangeTree::FindRangeBlock(double value) {
  RangeNumber normalized_value = SafeCastFromDoubleToLongLong(value);
  DCHECK(!entries_.empty());

  auto it = entries_.lower_bound({normalized_value, std::numeric_limits<RangeNumber>::min()});
  if (it != entries_.begin() && (it == entries_.end() || it->first.first > normalized_value)) {
    // TODO: remove this, we do log N here
    // we can use negative left bouding to find the block
    --it;  // Move to the block that contains the value
  }

  DCHECK(it != entries_.end() || (it->first.first <= value && it->first.second > value));
  return it;
}

RangeTree::Map::const_iterator RangeTree::FindRangeBlock(double value) const {
  RangeNumber normalized_value = SafeCastFromDoubleToLongLong(value);
  DCHECK(!entries_.empty());

  auto it = entries_.lower_bound({normalized_value, std::numeric_limits<RangeNumber>::min()});
  if (it != entries_.begin() && (it == entries_.end() || it->first.first > normalized_value)) {
    // TODO: remove this, we do log N here
    // we can use negative left bouding to find the block
    --it;  // Move to the block that contains the value
  }

  DCHECK(it != entries_.end() || (it->first.first <= value && it->first.second > value));
  return it;
}

void RangeTree::SplitBlock(Map::iterator it) {
  auto split_result = std::move(it->second).Split();

  const RangeNumber l = it->first.first;
  const RangeNumber m = split_result.median;
  const RangeNumber r = it->first.second;

  entries_.erase(it);
  entries_.emplace(std::piecewise_construct, std::forward_as_tuple(l, m),
                   std::forward_as_tuple(std::move(split_result.left)));

  entries_.emplace(std::piecewise_construct, std::forward_as_tuple(m, r),
                   std::forward_as_tuple(std::move(split_result.right)));
}

/*
void RangeTree::PrintTheWholeTree() const {
  if (entries_.empty()) {
    std::cerr << "RangeTree is empty\n";
    return;
  }

  std::cerr << "RangeTree contains " << entries_.size() << " blocks:\n";
  for (const auto& [key, block] : entries_) {
    std::cerr << "Range: [" << key.first << ", " << key.second << "], Size: " << block.Size()
              << "\n";
    for (const auto& entry : block) {
      std::cerr << "{DocId: " << entry.first << ", Value: " << entry.second << "}, ";
    }
    std::cerr << "\n";
  }
} */

RangeResult::ResultsMerger::ResultsMerger(const RangeResult& result)
    : range_result_(result),
      leftmost_it_(result.leftmost_block_.begin()),
      rightmost_it_(result.rightmost_block_.begin()) {
  for (size_t i = 0; i < result.middle_blocks_.size(); ++i) {
    auto it = result.middle_blocks_[i]->begin();
    DCHECK(it != result.middle_blocks_[i]->end()) << "Middle block iterator should not be empty";
    middle_blocks_iters_.push_back(it);
    heap_.push(i);
  }
}

DocId RangeResult::ResultsMerger::GetMin(DocId min_value) {
  DCHECK(HasNext());

  auto skip_until = [&](const BorderBlock& view, BorderBlockIterator* it) -> DocId {
    DocId last_value = kInlavidDocId;
    while ((*it) != view.end()) {
      const DocId value = **it;
      if (value >= min_value) {
        last_value = value;
        break;
      }
      ++(*it);
    }
    return last_value;
  };

  const DocId l_value = skip_until(range_result_.leftmost_block_, &leftmost_it_);
  const DocId r_value = skip_until(range_result_.rightmost_block_, &rightmost_it_);

  DocId m_value = kInlavidDocId;
  size_t m_index = std::numeric_limits<size_t>::max();
  while (!heap_.empty()) {
    const size_t index = heap_.top();
    DCHECK(middle_blocks_iters_[index] != range_result_.middle_blocks_[index]->end())
        << "Middle block iterator should not be empty";

    DocId value = (*middle_blocks_iters_[index]).first;
    if (value >= min_value) {
      m_value = value;
      m_index = index;
      break;
    }

    heap_.pop();
    ++middle_blocks_iters_[index];

    // Skip until we find a value >= min_value
    while (middle_blocks_iters_[index] != range_result_.middle_blocks_[index]->end()) {
      if ((*middle_blocks_iters_[index]).first >= min_value) {
        heap_.push(index);
        break;
      }

      ++middle_blocks_iters_[index];
    }
  }

  if (l_value < m_value && l_value < r_value) {
    ++leftmost_it_;
    return l_value;
  } else if (m_value < l_value && m_value < r_value) {
    DCHECK(m_index != std::numeric_limits<size_t>::max());
    DCHECK(!heap_.empty());
    heap_.pop();
    ++middle_blocks_iters_[m_index];
    if (middle_blocks_iters_[m_index] != range_result_.middle_blocks_[m_index]->end()) {
      heap_.push(m_index);
    }
    return m_value;
  } else {
    if (r_value != kInlavidDocId) {
      ++rightmost_it_;
    }
    return r_value;
  }
}

RangeResult::RangeResult(absl::InlinedVector<RangeBlockPointer, 10> blocks)
    : leftmost_block_(GetAllDocIds(*blocks.front())) {
  const size_t size = blocks.size();
  DCHECK(size > 0);

  if (size > 1) {
    rightmost_block_ = GetAllDocIds(*blocks.back());
    if (size > 2) {
      middle_blocks_.reserve(size - 2);
      for (size_t i = 1; i < size - 1; ++i) {
        middle_blocks_.push_back(blocks[i]);
      }
    }
  }
}

RangeResult::RangeResult(const RangeTree::RangeBlock& block, std::pair<double, double> range)
    : leftmost_block_(Range(block, range.first, range.second)) {
}

RangeResult::RangeResult(absl::InlinedVector<RangeBlockPointer, 10> blocks,
                         std::optional<double> leftmost_block_range,
                         std::optional<double> rightmost_block_range) {
  DCHECK(blocks.size() > 1);

  if (leftmost_block_range) {
    leftmost_block_ = GreaterOrEqual(*blocks.front(), leftmost_block_range.value());
  } else {
    leftmost_block_ = GetAllDocIds(*blocks.front());
  }

  if (rightmost_block_range) {
    rightmost_block_ = LessOrEqual(*blocks.back(), rightmost_block_range.value());
  } else {
    rightmost_block_ = GetAllDocIds(*blocks.back());
  }

  middle_blocks_.reserve(blocks.size() - 2);
  for (size_t i = 1; i < blocks.size() - 1; ++i) {
    middle_blocks_.push_back(blocks[i]);
  }
}

std::vector<DocId> RangeResult::MergeAllResults() {
  std::vector<DocId> result;
  result.reserve(leftmost_block_.size() + rightmost_block_.size() + middle_blocks_.size() * 20);

  ResultsMerger merger(*this);
  while (merger.HasNext()) {
    const DocId next_min_doc_id =
        result.empty() ? std::numeric_limits<DocId>::min() : (result.back() + 1u);
    const DocId doc_id = merger.GetMin(next_min_doc_id);
    if (doc_id != ResultsMerger::kInlavidDocId) {
      result.push_back(doc_id);
    } else {
      break;
    }
  }

  return result;
}

std::vector<DocId> RangeResult::LessOrEqual(const RangeTree::RangeBlock& range_block, double r) {
  std::vector<DocId> result;
  result.reserve(range_block.Size() / 8);
  for (const auto& entry : range_block) {
    if (entry.second <= r) {
      result.push_back(entry.first);
    }
  }
  return result;
}

std::vector<DocId> RangeResult::GreaterOrEqual(const RangeTree::RangeBlock& range_block, double l) {
  std::vector<DocId> result;
  result.reserve(range_block.Size() / 8);
  for (const auto& entry : range_block) {
    if (entry.second >= l) {
      result.push_back(entry.first);
    }
  }
  return result;
}

std::vector<DocId> RangeResult::Range(const RangeTree::RangeBlock& range_block, double l,
                                      double r) {
  std::vector<DocId> result;
  result.reserve(range_block.Size() / 16);
  for (const auto& entry : range_block) {
    if (entry.second >= l && entry.second <= r) {
      result.push_back(entry.first);
    }
  }
  return result;
}

std::vector<DocId> RangeResult::GetAllDocIds(const RangeTree::RangeBlock& range_block) {
  std::vector<DocId> result;
  result.reserve(range_block.Size());
  for (const auto& entry : range_block) {
    result.push_back(entry.first);
  }
  return result;
}

}  // namespace dfly::search
