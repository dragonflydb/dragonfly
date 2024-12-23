// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/aggregator.h"

#include "base/logging.h"

namespace dfly::aggregate {

namespace {

using ValuesList = absl::FixedArray<Value>;

ValuesList ExtractFieldsValues(const DocValues& dv, absl::Span<const std::string> fields) {
  ValuesList out(fields.size());
  for (size_t i = 0; i < fields.size(); i++) {
    auto it = dv.find(fields[i]);
    out[i] = (it != dv.end()) ? it->second : Value{};
  }
  return out;
}

DocValues PackFields(ValuesList values, absl::Span<const std::string> fields) {
  DCHECK_EQ(values.size(), fields.size());
  DocValues out;
  for (size_t i = 0; i < fields.size(); i++)
    out[fields[i]] = std::move(values[i]);
  return out;
}

const Value kEmptyValue = Value{};

}  // namespace

void Aggregator::DoGroup(absl::Span<const std::string> fields, absl::Span<const Reducer> reducers) {
  // Separate items into groups
  absl::flat_hash_map<ValuesList, std::vector<DocValues>> groups;
  for (auto& value : result.values) {
    groups[ExtractFieldsValues(value, fields)].push_back(std::move(value));
  }

  // Restore DocValues and apply reducers
  auto& values = result.values;
  values.clear();
  values.reserve(groups.size());
  while (!groups.empty()) {
    auto node = groups.extract(groups.begin());
    DocValues doc = PackFields(std::move(node.key()), fields);
    for (auto& reducer : reducers) {
      doc[reducer.result_field] = reducer.func({reducer.source_field, node.mapped()});
    }
    values.push_back(std::move(doc));
  }

  auto& fields_to_print = result.fields_to_print;
  fields_to_print.clear();
  fields_to_print.reserve(fields.size() + reducers.size());

  for (auto& field : fields) {
    fields_to_print.insert(field);
  }
  for (auto& reducer : reducers) {
    fields_to_print.insert(reducer.result_field);
  }
}

void Aggregator::DoSort(const SortParams& sort_params) {
  /*
    Comparator for sorting DocValues by fields.
    If some of the fields is not present in the DocValues, comparator returns:
    1. l_it == l.end() && r_it != r.end()
      asc -> false
      desc -> false
    2. l_it != l.end() && r_it == r.end()
      asc -> true
      desc -> true
    3. l_it == l.end() && r_it == r.end()
      asc -> false
      desc -> false
  */
  auto comparator = [&](const DocValues& l, const DocValues& r) {
    for (const auto& [field, order] : sort_params.fields) {
      auto l_it = l.find(field);
      auto r_it = r.find(field);

      // If some of the values is not present
      if (l_it == l.end() || r_it == r.end()) {
        if (l_it == l.end() && r_it == r.end()) {
          continue;
        }
        return l_it != l.end();
      }

      const auto& lv = l_it->second;
      const auto& rv = r_it->second;
      if (lv == rv) {
        continue;
      }
      return order == SortParams::SortOrder::ASC ? lv < rv : lv > rv;
    }
    return false;
  };

  auto& values = result.values;
  if (sort_params.SortAll()) {
    std::sort(values.begin(), values.end(), comparator);
  } else {
    DCHECK_GE(sort_params.max, 0);
    const size_t limit = std::min(values.size(), size_t(sort_params.max));
    std::partial_sort(values.begin(), values.begin() + limit, values.end(), comparator);
    values.resize(limit);
  }

  for (auto& field : sort_params.fields) {
    result.fields_to_print.insert(field.first);
  }
}

void Aggregator::DoLimit(size_t offset, size_t num) {
  auto& values = result.values;
  values.erase(values.begin(), values.begin() + std::min(offset, values.size()));
  values.resize(std::min(num, values.size()));
}

const Value& ValueIterator::operator*() const {
  auto it = values_.front().find(field_);
  return it == values_.front().end() ? kEmptyValue : it->second;
}

ValueIterator& ValueIterator::operator++() {
  values_.remove_prefix(1);
  return *this;
}

Reducer::Func FindReducerFunc(ReducerFunc name) {
  const static auto kCountReducer = [](ValueIterator it) -> double {
    return std::distance(it, it.end());
  };

  const static auto kSumReducer = [](ValueIterator it) -> double {
    double sum = 0;
    for (; it != it.end(); ++it)
      sum += std::holds_alternative<double>(*it) ? std::get<double>(*it) : 0.0;
    return sum;
  };

  switch (name) {
    case ReducerFunc::COUNT:
      return [](ValueIterator it) -> Value { return kCountReducer(it); };
    case ReducerFunc::COUNT_DISTINCT:
      return [](ValueIterator it) -> Value {
        return double(std::unordered_set<Value>(it, it.end()).size());
      };
    case ReducerFunc::SUM:
      return [](ValueIterator it) -> Value { return kSumReducer(it); };
    case ReducerFunc::AVG:
      return [](ValueIterator it) -> Value { return kSumReducer(it) / kCountReducer(it); };
    case ReducerFunc::MAX:
      return [](ValueIterator it) -> Value { return *std::max_element(it, it.end()); };
    case ReducerFunc::MIN:
      return [](ValueIterator it) -> Value { return *std::min_element(it, it.end()); };
  }

  return nullptr;
}

AggregationStep MakeGroupStep(std::vector<std::string> fields, std::vector<Reducer> reducers) {
  return [fields = std::move(fields), reducers = std::move(reducers)](Aggregator* aggregator) {
    aggregator->DoGroup(fields, reducers);
  };
}

AggregationStep MakeSortStep(SortParams sort_params) {
  return [params = std::move(sort_params)](Aggregator* aggregator) { aggregator->DoSort(params); };
}

AggregationStep MakeLimitStep(size_t offset, size_t num) {
  return [=](Aggregator* aggregator) { aggregator->DoLimit(offset, num); };
}

AggregationResult Process(std::vector<DocValues> values,
                          absl::Span<const std::string_view> fields_to_print,
                          absl::Span<const AggregationStep> steps) {
  Aggregator aggregator{std::move(values), {fields_to_print.begin(), fields_to_print.end()}};
  for (auto& step : steps) {
    step(&aggregator);
  }
  return aggregator.result;
}

}  // namespace dfly::aggregate
