// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/indices.h"

#include <absl/container/flat_hash_set.h>
#include <absl/strings/ascii.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>

#include <boost/iterator/function_output_iterator.hpp>
#include <shared_mutex>

#include "core/search/base.h"
#include "core/search/vector_utils.h"
#include "util/fibers/synchronization.h"

#define UNI_ALGO_DISABLE_NFKC_NFKD

#include <hnswlib/hnswalg.h>
#include <hnswlib/hnswlib.h>
#include <hnswlib/space_ip.h>
#include <hnswlib/space_l2.h>
#include <uni_algo/case.h>
#include <uni_algo/ranges_word.h>

#include <algorithm>
#include <cctype>

#include "base/flags.h"

ABSL_FLAG(bool, use_numeric_range_tree, true,
          "Use range tree for numeric index. "
          "If false, use a simple implementation with btree_set. "
          "Range tree is more memory efficient and faster for range queries, "
          "but slower for single value queries.");

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
                                               const TextIndex::StopWords& stopwords,
                                               const Synonyms* synonyms) {
  absl::flat_hash_set<std::string> words;
  for (std::string_view word : una::views::word_only::utf8(text)) {
    if (std::string word_lc = una::cases::to_lowercase_utf8(word); !stopwords.contains(word_lc)) {
      if (synonyms) {
        if (auto group_id = synonyms->GetGroupToken(word_lc); group_id) {
          words.insert(*group_id);
        }
      }

      words.insert(std::move(word_lc));
    }
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

// Iterate over all suffixes of all words
void IterateAllSuffixes(const absl::flat_hash_set<string>& words,
                        absl::FunctionRef<void(std::string_view)> cb) {
  for (string_view word : words) {
    for (size_t offs = 0; offs < word.length(); offs++) {
      cb(word.substr(offs));
    }
  }
}

// Haversine with earth radius in meters. Used to calculate distance.
boost::geometry::strategy::distance::haversine haversine_(6372797.560856);

double ConvertToRadiusInMeters(size_t radius, std::string_view arg) {
  const std::string unit = absl::AsciiStrToUpper(arg);
  if (unit == "M") {
    return radius * 1;
  } else if (unit == "KM") {
    return radius * 1000;
  } else if (unit == "FT") {
    return radius * 0.3048;
  } else if (unit == "MI") {
    return radius * 1609.34;
  } else {
    return -1;
  }
}

std::optional<GeoIndex::point> GetGeoPoint(const DocumentAccessor& doc, string_view field) {
  auto element = doc.GetStrings(field);

  if (!element)
    return std::nullopt;

  absl::InlinedVector<string_view, 2> coordinates = absl::StrSplit(element.value()[0], ",");

  if (coordinates.size() != 2)
    return std::nullopt;

  double lon, lat;
  if (!absl::SimpleAtod(coordinates[0], &lon) || !absl::SimpleAtod(coordinates[1], &lat))
    return nullopt;

  return GeoIndex::point{lon, lat};
}

template <typename Q, typename T = GlobalDocId> vector<pair<float, T>> QueueToVec(Q queue) {
  vector<pair<float, T>> out(queue.size());
  size_t idx = out.size();
  while (!queue.empty()) {
    out[--idx] = queue.top();
    queue.pop();
  }
  return out;
}

};  // namespace

class RangeTreeAdapter : public NumericIndex::RangeTreeBase {
 public:
  explicit RangeTreeAdapter(size_t max_range_block_size, PMR_NS::memory_resource* mr)
      : range_tree_(mr, max_range_block_size, false) {
  }

  void Add(DocId id, absl::Span<double> values) override {
    for (double value : values) {
      range_tree_.Add(id, value);
    }
  }

  void Remove(DocId id, absl::Span<double> values) override {
    for (double value : values) {
      range_tree_.Remove(id, value);
    }
  }

  RangeResult Range(double l, double r) const override {
    return range_tree_.Range(l, r);
  }

  vector<DocId> GetAllDocIds() const override {
    // TODO: remove take
    return range_tree_.GetAllDocIds().Take();
  }

  void FinalizeInitialization() override {
    range_tree_.FinalizeInitialization();
  }

 private:
  RangeTree range_tree_;
};

class BtreeSetImpl : public NumericIndex::RangeTreeBase {
 public:
  explicit BtreeSetImpl(PMR_NS::memory_resource* mr) : entries_(mr) {
  }

  void Add(DocId id, absl::Span<double> values) override {
    if (values.size() > 1) {
      unique_ids_ = false;
    }
    for (double value : values) {
      entries_.insert({value, id});
    }
  }

  void Remove(DocId id, absl::Span<double> values) override {
    for (double value : values) {
      entries_.erase({value, id});
    }
  }

  RangeResult Range(double l, double r) const override {
    DCHECK(l <= r);

    auto it_l = entries_.lower_bound({l, 0});
    auto it_r = entries_.lower_bound({r, numeric_limits<DocId>::max()});
    DCHECK_GE(it_r - it_l, 0);

    vector<DocId> out;
    for (auto it = it_l; it != it_r; ++it)
      out.push_back(it->second);

    sort(out.begin(), out.end());

    if (!unique_ids_) {
      out.erase(unique(out.begin(), out.end()), out.end());
    }
    return RangeResult(std::move(out));
  }

  vector<DocId> GetAllDocIds() const override {
    std::vector<DocId> result;

    result.reserve(entries_.size());

    if (unique_ids_) {
      // If unique_ids_ is true, we can just take the second element of each entry
      for (const auto& [_, doc_id] : entries_) {
        result.push_back(doc_id);
      }
    } else {
      absl::flat_hash_set<DocId> unique_docs;
      unique_docs.reserve(entries_.size());
      for (const auto& [_, doc_id] : entries_) {
        const auto [__, is_new] = unique_docs.insert(doc_id);
        if (is_new) {
          result.push_back(doc_id);
        }
      }
    }

    std::sort(result.begin(), result.end());
    return result;
  }

 private:
  bool unique_ids_ = true;  // If true, docs ids are unique in the index, otherwise they can repeat.
  using Entry = std::pair<double, DocId>;
  absl::btree_set<Entry, std::less<Entry>, PMR_NS::polymorphic_allocator<Entry>> entries_;
};

NumericIndex::NumericIndex(size_t max_range_block_size, PMR_NS::memory_resource* mr) {
  if (absl::GetFlag(FLAGS_use_numeric_range_tree)) {
    range_tree_ = make_unique<RangeTreeAdapter>(max_range_block_size, mr);
  } else {
    range_tree_ = make_unique<BtreeSetImpl>(mr);
  }
}

bool NumericIndex::Add(DocId id, const DocumentAccessor& doc, string_view field) {
  auto numbers = doc.GetNumbers(field);
  if (!numbers) {
    return false;
  }

  range_tree_->Add(id, absl::MakeSpan(numbers.value()));
  return true;
}

void NumericIndex::Remove(DocId id, const DocumentAccessor& doc, string_view field) {
  auto numbers = doc.GetNumbers(field).value();
  range_tree_->Remove(id, absl::MakeSpan(numbers));
}

void NumericIndex::FinalizeInitialization() {
  range_tree_->FinalizeInitialization();
}

RangeResult NumericIndex::Range(double l, double r) const {
  if (r < l)
    return {};
  return range_tree_->Range(l, r);
}

vector<DocId> NumericIndex::GetAllDocsWithNonNullValues() const {
  return range_tree_->GetAllDocIds();
}

template <typename C>
BaseStringIndex<C>::BaseStringIndex(PMR_NS::memory_resource* mr, bool case_sensitive,
                                    bool with_suffix)
    : case_sensitive_{case_sensitive}, entries_{mr} {
  if (with_suffix)
    suffix_trie_.emplace(mr);
}

template <typename C>
const typename BaseStringIndex<C>::Container* BaseStringIndex<C>::Matching(
    string_view word, bool strip_whitespace) const {
  if (strip_whitespace)
    word = absl::StripAsciiWhitespace(word);

  auto it = entries_.find(NormalizeQueryWord(word).view());
  return (it != entries_.end()) ? &it->second : nullptr;
}

template <typename C>
void BaseStringIndex<C>::MatchPrefix(std::string_view prefix,
                                     absl::FunctionRef<void(const Container*)> cb) const {
  StringOrView prefix_norm{NormalizeQueryWord(prefix)};
  prefix = prefix_norm.view();

  // TODO(vlad): Use right iterator to avoid string comparison?
  for (auto it = entries_.lower_bound(prefix);
       it != entries_.end() && (*it).first.rfind(prefix, 0) == 0; ++it) {
    cb(&(*it).second);
  }
}

template <typename C>
void BaseStringIndex<C>::MatchSuffix(std::string_view suffix,
                                     absl::FunctionRef<void(const Container*)> cb) const {
  StringOrView suffix_norm{NormalizeQueryWord(suffix)};
  suffix = suffix_norm.view();

  // If we have a suffix trie built, we just need to fetch the relevant suffix
  if (suffix_trie_) {
    auto it = suffix_trie_->find(suffix);
    cb((it != suffix_trie_->end()) ? &it->second : nullptr);
    return;
  }

  // Otherwise, iterate over all entries and look for the suffix
  for (const auto& entry : entries_) {
    int32_t start = entry.first.size() - suffix.size();
    if (start >= 0 && entry.first.substr(start) == suffix)
      cb(&entry.second);
  }
}

template <typename C>
void BaseStringIndex<C>::MatchInfix(std::string_view infix,
                                    absl::FunctionRef<void(const Container*)> cb) const {
  StringOrView infix_norm{NormalizeQueryWord(infix)};
  infix = infix_norm.view();

  // If we have a suffix trie built, we just need to match the prefix
  if (suffix_trie_) {
    for (auto it = suffix_trie_->lower_bound(infix);
         it != suffix_trie_->end() && (*it).first.rfind(infix, 0) == 0; ++it)
      cb(&(*it).second);
    return;
  }

  // Otherwise, iterate over all entries and check if it contains the entry
  for (const auto& entry : entries_) {
    if (entry.first.find(infix) != string::npos)
      cb(&entry.second);
  }
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

  if (tokens.size() > 1)
    unique_ids_ = false;
  for (string_view token : tokens)
    GetOrCreate(&entries_, token)->Insert(id);

  if (suffix_trie_)
    IterateAllSuffixes(tokens,
                       [&](string_view str) { GetOrCreate(&*suffix_trie_, str)->Insert(id); });

  return true;
}

template <typename C>
void BaseStringIndex<C>::Remove(DocId id, const DocumentAccessor& doc, string_view field) {
  auto strings_list = GetStrings(doc, field).value();

  absl::flat_hash_set<std::string> tokens;
  for (string_view str : strings_list)
    tokens.merge(Tokenize(str));

  for (string_view token : tokens)
    Remove(&entries_, id, token);

  if (suffix_trie_)
    IterateAllSuffixes(tokens, [&](string_view str) { Remove(&*suffix_trie_, id, str); });
}

template <typename C> vector<string> BaseStringIndex<C>::GetTerms() const {
  vector<string> res;
  res.reserve(entries_.size());
  for (const auto& [term, _] : entries_) {
    res.push_back(string{term});
  }
  return res;
}

template <typename C> vector<DocId> BaseStringIndex<C>::GetAllDocsWithNonNullValues() const {
  std::vector<DocId> result;

  result.reserve(entries_.size());

  if (unique_ids_) {
    // If unique_ids_ is true, we can just take the second element of each entry
    for (const auto& [_, container] : entries_) {
      for (const auto& doc_id : container) {
        result.push_back(doc_id);
      }
    }
  } else {
    absl::flat_hash_set<DocId> unique_docs;
    unique_docs.reserve(entries_.size());

    for (const auto& [_, container] : entries_) {
      for (const auto& doc_id : container) {
        auto [_, is_new] = unique_docs.insert(doc_id);
        if (is_new) {
          result.push_back(doc_id);
        }
      }
    }
  }
  std::sort(result.begin(), result.end());
  return result;
}

template <typename C>
StringOrView BaseStringIndex<C>::NormalizeQueryWord(std::string_view query) const {
  if (case_sensitive_)
    return StringOrView::FromView(query);

  return StringOrView::FromString(ToLower(query));
}

template <typename C>
typename BaseStringIndex<C>::Container* BaseStringIndex<C>::GetOrCreate(
    search::RaxTreeMap<Container>* map, string_view word) {
  auto* mr = map->get_allocator().resource();
  return &map->try_emplace(PMR_NS::string{word, mr}, mr, 1000 /* block size */).first->second;
}

template <typename C>
void BaseStringIndex<C>::Remove(search::RaxTreeMap<Container>* map, DocId id, string_view word) {
  auto it = map->find(word);
  if (it == map->end())
    return;

  it->second.Remove(id);
  if (it->second.Size() == 0)
    map->erase(it);
}

template struct BaseStringIndex<CompressedSortedSet>;
template struct BaseStringIndex<SortedVector<DocId>>;

TextIndex::TextIndex(PMR_NS::memory_resource* mr, const StopWords* stopwords,
                     const Synonyms* synonyms, bool with_suffixtrie)
    : BaseStringIndex(mr, false, with_suffixtrie), stopwords_{stopwords}, synonyms_{synonyms} {
}

std::optional<DocumentAccessor::StringList> TextIndex::GetStrings(const DocumentAccessor& doc,
                                                                  std::string_view field) const {
  return doc.GetStrings(field);
}

absl::flat_hash_set<std::string> TextIndex::Tokenize(std::string_view value) const {
  return TokenizeWords(value, *stopwords_, synonyms_);
}

std::optional<DocumentAccessor::StringList> TagIndex::GetStrings(const DocumentAccessor& doc,
                                                                 std::string_view field) const {
  return doc.GetTags(field);
}

absl::flat_hash_set<std::string> TagIndex::Tokenize(std::string_view value) const {
  return NormalizeTags(value, case_sensitive_, separator_);
}

template <typename T>
BaseVectorIndex<T>::BaseVectorIndex(size_t dim, VectorSimilarity sim) : dim_{dim}, sim_{sim} {
}

template <typename T>
bool BaseVectorIndex<T>::Add(T id, const DocumentAccessor& doc, std::string_view field) {
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

ShardNoOpVectorIndex::ShardNoOpVectorIndex(const SchemaField::VectorParams& params)
    : BaseVectorIndex<DocId>{params.dim, params.sim} {
}

FlatVectorIndex::FlatVectorIndex(const SchemaField::VectorParams& params, ShardId shard_set_size,
                                 PMR_NS::memory_resource* mr)
    : BaseVectorIndex<GlobalDocId>{params.dim, params.sim},
      entries_{mr},
      shard_vector_locks_(shard_set_size) {
  DCHECK(!params.use_hnsw);
  entries_.resize(shard_set_size);
  for (size_t i = 0; i < shard_set_size; i++) {
    entries_[i].reserve(params.capacity * params.dim);
  }
}

void FlatVectorIndex::AddVector(GlobalDocId id,
                                const typename BaseVectorIndex<GlobalDocId>::VectorPtr& vector) {
  auto shard_id = search::GlobalDocIdShardId(id);
  auto shard_doc_id = search::GlobalDocIdLocalId(id);
  DCHECK_LE(shard_doc_id * BaseVectorIndex<GlobalDocId>::dim_, entries_[shard_id].size());
  if (shard_doc_id * BaseVectorIndex<GlobalDocId>::dim_ == entries_[shard_id].size()) {
    unique_lock<util::fb2::SharedMutex> lock{shard_vector_locks_[shard_id]};
    entries_[shard_id].resize((shard_doc_id + 1) * BaseVectorIndex<GlobalDocId>::dim_);
  }
  if (vector) {
    memcpy(&entries_[shard_id][shard_doc_id * BaseVectorIndex<GlobalDocId>::dim_], vector.get(),
           BaseVectorIndex<GlobalDocId>::dim_ * sizeof(float));
  }
}

void FlatVectorIndex::Remove(GlobalDocId id, const DocumentAccessor& doc, string_view field) {
  // noop
}

std::vector<std::pair<float, GlobalDocId>> FlatVectorIndex::Knn(float* target, size_t k) const {
  std::priority_queue<std::pair<float, search::GlobalDocId>> queue;

  for (size_t shard_id = 0; shard_id < entries_.size(); shard_id++) {
    shared_lock<util::fb2::SharedMutex> lock{shard_vector_locks_[shard_id]};
    size_t num_vectors = entries_[shard_id].size() / BaseVectorIndex<GlobalDocId>::dim_;
    for (GlobalDocId id = 0; id < num_vectors; ++id) {
      const float* vec = &entries_[shard_id][id * dim_];
      float dist = VectorDistance(target, vec, dim_, sim_);
      queue.emplace(dist, CreateGlobalDocId(shard_id, id));
    }
  }

  return QueueToVec(queue);
}

std::vector<std::pair<float, GlobalDocId>> FlatVectorIndex::Knn(
    float* target, size_t k, const std::vector<FilterShardDocs>& allowed_docs) const {
  std::priority_queue<std::pair<float, search::GlobalDocId>> queue;

  for (size_t shard_id = 0; shard_id < allowed_docs.size(); shard_id++) {
    shared_lock<util::fb2::SharedMutex> lock{shard_vector_locks_[shard_id]};
    for (auto& shard_doc_id : allowed_docs[shard_id]) {
      const float* vec = &entries_[shard_id][shard_doc_id * dim_];
      float dist = VectorDistance(target, vec, dim_, sim_);
      queue.emplace(dist, CreateGlobalDocId(shard_id, shard_doc_id));
    }
  }
  return QueueToVec(queue);
}

template <typename T> struct HnswlibAdapter {
  // Default setting of hnswlib/hnswalg
  constexpr static size_t kDefaultEfRuntime = 10;

  HnswlibAdapter(const SchemaField::VectorParams& params)
      : space_{MakeSpace(params.dim, params.sim)},
        world_{GetSpacePtr(), params.capacity, params.hnsw_m, params.hnsw_ef_construction,
               100 /* seed*/} {
  }

  void Add(const float* data, T id) {
    while (true) {
      try {
        absl::ReaderMutexLock lock(&resize_mutex_);
        world_.addPoint(data, id);
        return;
      } catch (const std::exception& e) {
        std::string error_msg = e.what();
        if (absl::StrContains(error_msg, "The number of elements exceeds the specified limit")) {
          ResizeIfFull();
          continue;
        }
        throw e;
      }
    }
  }

  void Remove(T id) {
    try {
      world_.markDelete(id);
    } catch (const std::exception& e) {
    }
  }

  vector<pair<float, T>> Knn(float* target, size_t k, std::optional<size_t> ef) {
    world_.setEf(ef.value_or(kDefaultEfRuntime));
    return QueueToVec(world_.searchKnn(target, k));
  }

  vector<pair<float, T>> Knn(float* target, size_t k, std::optional<size_t> ef,
                             const vector<T>& allowed) {
    struct BinsearchFilter : hnswlib::BaseFilterFunctor {
      virtual bool operator()(hnswlib::labeltype id) {
        return binary_search(allowed->begin(), allowed->end(), id);
      }

      BinsearchFilter(const vector<T>* allowed) : allowed{allowed} {
      }
      const vector<T>* allowed;
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

  void ResizeIfFull() {
    {
      absl::ReaderMutexLock lock(&resize_mutex_);
      if (world_.getCurrentElementCount() < world_.getMaxElements() ||
          (world_.allow_replace_deleted_ && world_.getDeletedCount() > 0)) {
        return;
      }
    }
    try {
      absl::WriterMutexLock lock(&resize_mutex_);
      if (world_.getCurrentElementCount() == world_.getMaxElements() &&
          (!world_.allow_replace_deleted_ || world_.getDeletedCount() == 0)) {
        auto max_elements = world_.getMaxElements();
        world_.resizeIndex(max_elements * 2);
        LOG(INFO) << "Resizing HNSW Index, current size: " << max_elements
                  << ", expand by: " << max_elements * 2;
      }
    } catch (const std::exception& e) {
      throw e;
    }
  }

  SpaceUnion space_;
  hnswlib::HierarchicalNSW<float> world_;
  absl::Mutex resize_mutex_;
};

HnswVectorIndex::HnswVectorIndex(const SchemaField::VectorParams& params, PMR_NS::memory_resource*)
    : BaseVectorIndex<GlobalDocId>{params.dim, params.sim},
      adapter_{make_unique<HnswlibAdapter<GlobalDocId>>(params)} {
  DCHECK(params.use_hnsw);
  // TODO: Patch hnsw to use MR
}
HnswVectorIndex::~HnswVectorIndex() {
}

void HnswVectorIndex::AddVector(GlobalDocId id,
                                const typename BaseVectorIndex<GlobalDocId>::VectorPtr& vector) {
  if (vector) {
    adapter_->Add(vector.get(), id);
  }
}

std::vector<std::pair<float, GlobalDocId>> HnswVectorIndex::Knn(float* target, size_t k,
                                                                std::optional<size_t> ef) const {
  return adapter_->Knn(target, k, ef);
}

std::vector<std::pair<float, GlobalDocId>> HnswVectorIndex::Knn(
    float* target, size_t k, std::optional<size_t> ef,
    const std::vector<GlobalDocId>& allowed) const {
  return adapter_->Knn(target, k, ef, allowed);
}

void HnswVectorIndex::Remove(GlobalDocId id, const DocumentAccessor& doc, string_view field) {
  adapter_->Remove(id);
}

GeoIndex::GeoIndex(PMR_NS::memory_resource* mr) : rtree_(make_unique<rtree>()) {
}

GeoIndex::~GeoIndex() {
}

bool GeoIndex::Add(DocId id, const DocumentAccessor& doc, std::string_view field) {
  auto doc_point = GetGeoPoint(doc, field);
  if (!doc_point) {
    return false;
  }
  rtree_->insert({doc_point.value(), id});
  return true;
}

void GeoIndex::Remove(DocId id, const DocumentAccessor& doc, string_view field) {
  auto doc_point = GetGeoPoint(doc, field);
  rtree_->remove({doc_point.value(), id});
}

std::vector<DocId> GeoIndex::RadiusSearch(double lon, double lat, double radius,
                                          std::string_view unit) {
  std::vector<DocId> results;

  // Get radius in meters
  double converted_radius = ConvertToRadiusInMeters(radius, unit);

  // Declare the geographic_point_circle strategy with 4 points
  boost::geometry::strategy::buffer::geographic_point_circle<> point_strategy(4);

  // Declare the distance strategy in meters around the point
  boost::geometry::strategy::buffer::distance_symmetric<double> distance_strategy(converted_radius);

  // Declare other necessary strategies, unused for point
  boost::geometry::strategy::buffer::join_round join_strategy;
  boost::geometry::strategy::buffer::end_round end_strategy;
  boost::geometry::strategy::buffer::side_straight side_strategy;

  point p{lon, lat};

  // Create polygon with 4 point around point
  boost::geometry::model::multi_polygon<boost::geometry::model::polygon<point>> buffer_polygon;

  boost::geometry::buffer(p, buffer_polygon, distance_strategy, side_strategy, join_strategy,
                          end_strategy, point_strategy);

  // Create bouding box around polygon to include all possible points
  boost::geometry::model::box<point> box;
  boost::geometry::envelope(buffer_polygon, box);

  rtree_->query(
      boost::geometry::index::within(box),
      boost::make_function_output_iterator([&results, &p, &converted_radius](auto const& val) {
        if (haversine_.apply(val.first, p) <= converted_radius) {
          results.push_back(val.second);
        }
      }));

  // TODO: we should return sorted results by radius distance
  return results;
}

std::vector<DocId> GeoIndex::GetAllDocsWithNonNullValues() const {
  std::vector<DocId> results;
  std::for_each(boost::geometry::index::begin(*rtree_), boost::geometry::index::end(*rtree_),
                [&results](auto const& val) { results.push_back(val.second); });
  return results;
}

}  // namespace dfly::search
