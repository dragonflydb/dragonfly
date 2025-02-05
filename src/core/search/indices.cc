// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/indices.h"

#include <absl/container/flat_hash_set.h>
#include <absl/strings/ascii.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_join.h>
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

string ToLower(string_view word) {
  return IsAllAscii(word) ? absl::AsciiStrToLower(word) : una::cases::to_lowercase_utf8(word);
}

// Get all words from text as matched by the ICU library
absl::flat_hash_set<std::string> TokenizeWords(std::string_view text,
                                               const TextIndex::StopWords& stopwords) {
  absl::flat_hash_set<std::string> words;
  for (std::string_view word : una::views::word_only::utf8(text)) {
    if (std::string word_lc = una::cases::to_lowercase_utf8(word); !stopwords.contains(word_lc))
      words.insert(std::move(word_lc));
  }
  return words;
}

// Split taglist, remove duplicates and convert all to lowercase
// TODO: introduce unicode support if needed
absl::flat_hash_set<string> NormalizeTags(string_view taglist, bool case_sensitive,
                                          char separator) {
  LOG_IF(WARNING, !IsAllAscii(taglist)) << "Non ascii tag usage";

  string tmp;
  absl::flat_hash_set<string> tags;
  for (string_view tag : absl::StrSplit(taglist, separator, absl::SkipEmpty())) {
    tmp = absl::StripAsciiWhitespace(tag);
    if (!case_sensitive)
      absl::AsciiStrToLower(&tmp);
    tags.insert(std::move(tmp));
  }
  return tags;
}

};  // namespace

NumericIndex::NumericIndex(PMR_NS::memory_resource* mr) : entries_{mr} {
}

bool NumericIndex::Add(DocId id, const DocumentAccessor& doc, string_view field) {
  auto numbers = doc.GetNumbers(field);
  if (!numbers) {
    return false;
  }

  for (auto num : numbers.value()) {
    entries_.emplace(num, id);
  }
  return true;
}

void NumericIndex::Remove(DocId id, const DocumentAccessor& doc, string_view field) {
  auto numbers = doc.GetNumbers(field).value();
  for (auto num : numbers) {
    entries_.erase({num, id});
  }
}

vector<DocId> NumericIndex::Range(double l, double r) const {
  if (r < l)
    return {};

  auto it_l = entries_.lower_bound({l, 0});
  auto it_r = entries_.lower_bound({r, numeric_limits<DocId>::max()});
  DCHECK_GE(it_r - it_l, 0);

  vector<DocId> out;
  for (auto it = it_l; it != it_r; ++it)
    out.push_back(it->second);

  sort(out.begin(), out.end());
  out.erase(unique(out.begin(), out.end()), out.end());
  return out;
}

template <typename C>
BaseStringIndex<C>::BaseStringIndex(PMR_NS::memory_resource* mr, bool case_sensitive)
    : case_sensitive_{case_sensitive}, entries_{mr} {
}

template <typename C>
const typename BaseStringIndex<C>::Container* BaseStringIndex<C>::Matching(string_view str) const {
  str = absl::StripAsciiWhitespace(str);

  string tmp;
  if (!case_sensitive_) {
    tmp = ToLower(str);
    str = tmp;
  }

  auto it = entries_.find(str);
  return (it != entries_.end()) ? &it->second : nullptr;
}

template <typename C>
void BaseStringIndex<C>::MatchingPrefix(std::string_view prefix,
                                        absl::FunctionRef<void(const Container*)> cb) const {
  for (auto it = entries_.lower_bound(prefix);
       it != entries_.end() && (*it).first.rfind(prefix, 0) == 0; ++it) {
    cb(&(*it).second);
  }
}

template <typename C>
typename BaseStringIndex<C>::Container* BaseStringIndex<C>::GetOrCreate(string_view word) {
  auto* mr = entries_.get_allocator().resource();
  return &entries_.try_emplace(PMR_NS::string{word, mr}, mr, 1000 /* block size */).first->second;
}

template <typename C>
bool BaseStringIndex<C>::Add(DocId id, const DocumentAccessor& doc, string_view field) {
  auto strings_list = GetStrings(doc, field);
  if (!strings_list) {
    return false;
  }

  absl::flat_hash_set<std::string> tokens;
  for (string_view str : strings_list.value())
    tokens.merge(Tokenize(str));

  for (string_view token : tokens)
    GetOrCreate(token)->Insert(id);
  return true;
}

template <typename C>
void BaseStringIndex<C>::Remove(DocId id, const DocumentAccessor& doc, string_view field) {
  auto strings_list = GetStrings(doc, field).value();

  absl::flat_hash_set<std::string> tokens;
  for (string_view str : strings_list)
    tokens.merge(Tokenize(str));

  for (const auto& token : tokens) {
    auto it = entries_.find(token);
    if (it == entries_.end())
      continue;

    it->second.Remove(id);
    if (it->second.Size() == 0)
      entries_.erase(it);
  }
}

template <typename C> vector<string> BaseStringIndex<C>::GetTerms() const {
  vector<string> res;
  res.reserve(entries_.size());
  for (const auto& [term, _] : entries_) {
    res.push_back(string{term});
  }
  return res;
}

template struct BaseStringIndex<CompressedSortedSet>;
template struct BaseStringIndex<SortedVector>;

std::optional<DocumentAccessor::StringList> TextIndex::GetStrings(const DocumentAccessor& doc,
                                                                  std::string_view field) const {
  return doc.GetStrings(field);
}

absl::flat_hash_set<std::string> TextIndex::Tokenize(std::string_view value) const {
  return TokenizeWords(value, *stopwords_);
}

std::optional<DocumentAccessor::StringList> TagIndex::GetStrings(const DocumentAccessor& doc,
                                                                 std::string_view field) const {
  return doc.GetTags(field);
}

absl::flat_hash_set<std::string> TagIndex::Tokenize(std::string_view value) const {
  return NormalizeTags(value, case_sensitive_, separator_);
}

BaseVectorIndex::BaseVectorIndex(size_t dim, VectorSimilarity sim) : dim_{dim}, sim_{sim} {
}

std::pair<size_t /*dim*/, VectorSimilarity> BaseVectorIndex::Info() const {
  return {dim_, sim_};
}

bool BaseVectorIndex::Add(DocId id, const DocumentAccessor& doc, std::string_view field) {
  auto vector = doc.GetVector(field);
  if (!vector)
    return false;

  auto& [ptr, size] = vector.value();
  if (ptr && size != dim_) {
    return false;
  }

  AddVector(id, ptr);
  return true;
}

FlatVectorIndex::FlatVectorIndex(const SchemaField::VectorParams& params,
                                 PMR_NS::memory_resource* mr)
    : BaseVectorIndex{params.dim, params.sim}, entries_{mr} {
  DCHECK(!params.use_hnsw);
  entries_.reserve(params.capacity * params.dim);
}

void FlatVectorIndex::AddVector(DocId id, const VectorPtr& vector) {
  DCHECK_LE(id * dim_, entries_.size());
  if (id * dim_ == entries_.size())
    entries_.resize((id + 1) * dim_);

  // TODO: Let get vector write to buf itself
  if (vector) {
    memcpy(&entries_[id * dim_], vector.get(), dim_ * sizeof(float));
  }
}

void FlatVectorIndex::Remove(DocId id, const DocumentAccessor& doc, string_view field) {
  // noop
}

const float* FlatVectorIndex::Get(DocId doc) const {
  return &entries_[doc * dim_];
}

struct HnswlibAdapter {
  // Default setting of hnswlib/hnswalg
  constexpr static size_t kDefaultEfRuntime = 10;

  HnswlibAdapter(const SchemaField::VectorParams& params)
      : space_{MakeSpace(params.dim, params.sim)},
        world_{GetSpacePtr(), params.capacity, params.hnsw_m, params.hnsw_ef_construction,
               100 /* seed*/} {
  }

  void Add(const float* data, DocId id) {
    if (world_.cur_element_count + 1 >= world_.max_elements_)
      world_.resizeIndex(world_.cur_element_count * 2);
    world_.addPoint(data, id);
  }

  void Remove(DocId id) {
    world_.markDelete(id);
  }

  vector<pair<float, DocId>> Knn(float* target, size_t k, std::optional<size_t> ef) {
    world_.setEf(ef.value_or(kDefaultEfRuntime));
    return QueueToVec(world_.searchKnn(target, k));
  }

  vector<pair<float, DocId>> Knn(float* target, size_t k, std::optional<size_t> ef,
                                 const vector<DocId>& allowed) {
    struct BinsearchFilter : hnswlib::BaseFilterFunctor {
      virtual bool operator()(hnswlib::labeltype id) {
        return binary_search(allowed->begin(), allowed->end(), id);
      }

      BinsearchFilter(const vector<DocId>* allowed) : allowed{allowed} {
      }
      const vector<DocId>* allowed;
    };

    world_.setEf(ef.value_or(kDefaultEfRuntime));
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

HnswVectorIndex::HnswVectorIndex(const SchemaField::VectorParams& params, PMR_NS::memory_resource*)
    : BaseVectorIndex{params.dim, params.sim}, adapter_{make_unique<HnswlibAdapter>(params)} {
  DCHECK(params.use_hnsw);
  // TODO: Patch hnsw to use MR
}

HnswVectorIndex::~HnswVectorIndex() {
}

void HnswVectorIndex::AddVector(DocId id, const VectorPtr& vector) {
  if (vector) {
    adapter_->Add(vector.get(), id);
  }
}

std::vector<std::pair<float, DocId>> HnswVectorIndex::Knn(float* target, size_t k,
                                                          std::optional<size_t> ef) const {
  return adapter_->Knn(target, k, ef);
}
std::vector<std::pair<float, DocId>> HnswVectorIndex::Knn(float* target, size_t k,
                                                          std::optional<size_t> ef,
                                                          const std::vector<DocId>& allowed) const {
  return adapter_->Knn(target, k, ef, allowed);
}

void HnswVectorIndex::Remove(DocId id, const DocumentAccessor& doc, string_view field) {
  adapter_->Remove(id);
}

}  // namespace dfly::search
