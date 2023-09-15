// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/indices.h"

#include <absl/container/flat_hash_set.h>
#include <absl/strings/ascii.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_split.h>

#define UNI_ALGO_DISABLE_NFKC_NFKD

#include <hnswlib/hnswalg.h>
#include <hnswlib/hnswlib.h>
#include <hnswlib/space_ip.h>
#include <hnswlib/space_l2.h>
#include <uni_algo/case.h>
#include <uni_algo/ranges_word.h>

#include <algorithm>
#include <cctype>

#include "base/logging.h"

namespace dfly::search {

using namespace std;

namespace {

bool IsAllAscii(string_view sv) {
  return all_of(sv.begin(), sv.end(), [](unsigned char c) { return isascii(c); });
}

// Get all words from text as matched by the ICU library
absl::flat_hash_set<std::string> TokenizeWords(std::string_view text) {
  absl::flat_hash_set<std::string> words;
  for (std::string_view word : una::views::word_only::utf8(text))
    words.insert(una::cases::to_lowercase_utf8(word));
  return words;
}

// Split taglist, remove duplicates and convert all to lowercase
absl::flat_hash_set<string> NormalizeTags(string_view taglist) {
  string tmp;
  absl::flat_hash_set<string> tags;
  for (string_view tag : absl::StrSplit(taglist, ',')) {
    tmp = absl::StripAsciiWhitespace(tag);
    absl::AsciiStrToLower(&tmp);
    tags.insert(move(tmp));
  }
  return tags;
}

};  // namespace

NumericIndex::NumericIndex(pmr::memory_resource* mr) : entries_{mr} {
}

void NumericIndex::Add(DocId id, DocumentAccessor* doc, string_view field) {
  int64_t num;
  if (absl::SimpleAtoi(doc->GetString(field), &num))
    entries_.emplace(num, id);
}

void NumericIndex::Remove(DocId id, DocumentAccessor* doc, string_view field) {
  int64_t num;
  if (absl::SimpleAtoi(doc->GetString(field), &num))
    entries_.erase({num, id});
}

vector<DocId> NumericIndex::Range(int64_t l, int64_t r) const {
  auto it_l = entries_.lower_bound({l, 0});
  auto it_r = entries_.lower_bound({r + 1, 0});

  vector<DocId> out;
  for (auto it = it_l; it != it_r; ++it)
    out.push_back(it->second);

  sort(out.begin(), out.end());
  return out;
}

BaseStringIndex::BaseStringIndex(pmr::memory_resource* mr) : entries_{mr} {
}

const CompressedSortedSet* BaseStringIndex::Matching(string_view str) const {
  str = absl::StripAsciiWhitespace(str);

  string word;
  if (IsAllAscii(str))
    word = absl::AsciiStrToLower(str);
  else
    word = una::cases::to_lowercase_utf8(str);

  auto* mr = entries_.get_allocator().resource();
  auto it = entries_.find(pmr::string{word, mr});
  return (it != entries_.end()) ? &it->second : nullptr;
}

CompressedSortedSet* BaseStringIndex::GetOrCreate(string_view word) {
  auto* mr = entries_.get_allocator().resource();
  return &entries_.try_emplace(pmr::string{word, mr}, mr).first->second;
}

void BaseStringIndex::Add(DocId id, DocumentAccessor* doc, string_view field) {
  for (const auto& word : Tokenize(doc->GetString(field)))
    GetOrCreate(word)->Insert(id);
}

void BaseStringIndex::Remove(DocId id, DocumentAccessor* doc, string_view field) {
  for (const auto& word : Tokenize(doc->GetString(field)))
    GetOrCreate(word)->Remove(id);
}

absl::flat_hash_set<std::string> TextIndex::Tokenize(std::string_view value) const {
  return TokenizeWords(value);
}

absl::flat_hash_set<std::string> TagIndex::Tokenize(std::string_view value) const {
  return NormalizeTags(value);
}

BaseVectorIndex::BaseVectorIndex(size_t dim, VectorSimilarity sim) : dim_{dim}, sim_{sim} {
}

std::pair<size_t /*dim*/, VectorSimilarity> BaseVectorIndex::Info() const {
  return {dim_, sim_};
}

FlatVectorIndex::FlatVectorIndex(size_t dim, VectorSimilarity sim, pmr::memory_resource* mr)
    : BaseVectorIndex{dim, sim}, entries_{mr} {
}

void FlatVectorIndex::Add(DocId id, DocumentAccessor* doc, string_view field) {
  DCHECK_LE(id * dim_, entries_.size());
  if (id * dim_ == entries_.size())
    entries_.resize((id + 1) * dim_);

  // TODO: Let get vector write to buf itself
  auto [ptr, size] = doc->GetVector(field);

  if (size == dim_)
    memcpy(&entries_[id * dim_], ptr.get(), dim_ * sizeof(float));
}

void FlatVectorIndex::Remove(DocId id, DocumentAccessor* doc, string_view field) {
  // noop
}

const float* FlatVectorIndex::Get(DocId doc) const {
  return &entries_[doc * dim_];
}

struct HnswlibAdapter {
  HnswlibAdapter(size_t dim, VectorSimilarity sim, size_t cap)
      : space_{MakeSpace(dim, sim)}, world_{GetSpacePtr(), cap} {
  }

  void Add(float* data, DocId id) {
    world_.addPoint(data, id);
  }

  void Remove(DocId id) {
    world_.markDelete(id);
  }

  vector<pair<float, DocId>> Knn(float* target, size_t k) {
    return QueueToVec(world_.searchKnn(target, k));
  }

  vector<pair<float, DocId>> Knn(float* target, size_t k, const vector<DocId>& allowed) {
    struct BinsearchFilter : hnswlib::BaseFilterFunctor {
      virtual bool operator()(hnswlib::labeltype id) {
        return binary_search(allowed->begin(), allowed->end(), id);
      }

      BinsearchFilter(const vector<DocId>* allowed) : allowed{allowed} {
      }
      const vector<DocId>* allowed;
    };

    BinsearchFilter filter{&allowed};
    return QueueToVec(world_.searchKnn(target, k, &filter));
  }

 private:
  using SpaceUnion = std::variant<hnswlib::L2Space, hnswlib::InnerProductSpace>;

  static SpaceUnion MakeSpace(size_t dim, VectorSimilarity sim) {
    if (sim == VectorSimilarity::L2)
      return hnswlib::L2Space{dim};
    else
      return hnswlib::InnerProductSpace{dim};
  }

  hnswlib::SpaceInterface<float>* GetSpacePtr() {
    return visit([](auto& space) -> hnswlib::SpaceInterface<float>* { return &space; }, space_);
  }

  template <typename Q> static vector<pair<float, DocId>> QueueToVec(Q queue) {
    vector<pair<float, DocId>> out(queue.size());
    size_t idx = out.size();
    while (!queue.empty()) {
      out[--idx] = queue.top();
      queue.pop();
    }
    return out;
  }

  SpaceUnion space_;
  hnswlib::HierarchicalNSW<float> world_;
};

HnswVectorIndex::HnswVectorIndex(size_t dim, VectorSimilarity sim, size_t capacity,
                                 pmr::memory_resource* mr)
    : BaseVectorIndex{dim, sim}, adapter_{make_unique<HnswlibAdapter>(dim, sim, capacity)} {
  (void)(mr);  // TODO: Patch hnsw to use MR
}

HnswVectorIndex::~HnswVectorIndex() {
}

void HnswVectorIndex::Add(DocId id, DocumentAccessor* doc, string_view field) {
  auto [ptr, size] = doc->GetVector(field);
  if (size == dim_)
    adapter_->Add(ptr.get(), id);
}

std::vector<std::pair<float, DocId>> HnswVectorIndex::Knn(float* target, size_t k) const {
  return adapter_->Knn(target, k);
}
std::vector<std::pair<float, DocId>> HnswVectorIndex::Knn(float* target, size_t k,
                                                          const std::vector<DocId>& allowed) const {
  return adapter_->Knn(target, k, allowed);
}

void HnswVectorIndex::Remove(DocId id, DocumentAccessor* doc, string_view field) {
  adapter_->Remove(id);
}

}  // namespace dfly::search
