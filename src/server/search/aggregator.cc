#include "server/search/aggregator.h"

#include "base/logging.h"

namespace dfly::aggregate {

namespace {

struct GroupStep {
  PipelineResult operator()(std::vector<DocValues> values) {
    // Separate items into groups
    absl::flat_hash_map<absl::FixedArray<Value>, std::vector<DocValues>> groups;
    for (auto& value : values) {
      groups[Extract(value)].push_back(std::move(value));
    }

    // Restore DocValues and appy reducers
    std::vector<DocValues> out;
    while (!groups.empty()) {
      auto node = groups.extract(groups.begin());
      DocValues doc = Unpack(std::move(node.key()));
      for (auto& reducer : reducers_) {
        doc[reducer.result_field] = reducer.func({reducer.source_field, node.mapped()});
      }
      out.push_back(std::move(doc));
    }
    return out;
  }

  absl::FixedArray<Value> Extract(const DocValues& dv) {
    absl::FixedArray<Value> out(fields_.size());
    for (size_t i = 0; i < fields_.size(); i++) {
      auto it = dv.find(fields_[i]);
      out[i] = (it != dv.end()) ? it->second : Value{};
    }
    return out;
  }

  DocValues Unpack(absl::FixedArray<Value>&& values) {
    DCHECK_EQ(values.size(), fields_.size());
    DocValues out;
    for (size_t i = 0; i < fields_.size(); i++)
      out[fields_[i]] = std::move(values[i]);
    return out;
  }

  std::vector<std::string> fields_;
  std::vector<Reducer> reducers_;
};

}  // namespace

const Value ValueIterator::kEmpty = Value{};

Reducer::Func FindReducerFunc(std::string_view name) {
  const static auto kCountReducer = [](ValueIterator it) -> double {
    return std::distance(it, it.end());
  };

  const static auto kSumReducer = [](ValueIterator it) -> double {
    double sum = 0;
    for (; it != it.end(); ++it)
      sum += std::holds_alternative<double>(*it) ? std::get<double>(*it) : 0.0;
    return sum;
  };

  static const std::unordered_map<std::string_view, std::function<Value(ValueIterator)>> kReducers =
      {{"COUNT", [](auto it) { return kCountReducer(it); }},
       {"COUNT_DISTINCT",
        [](auto it) { return double(std::unordered_set<Value>(it, it.end()).size()); }},
       {"SUM", [](auto it) { return kSumReducer(it); }},
       {"AVG", [](auto it) { return kSumReducer(it) / kCountReducer(it); }},
       {"MAX", [](auto it) { return *std::max_element(it, it.end()); }},
       {"MIN", [](auto it) { return *std::min_element(it, it.end()); }}};

  auto it = kReducers.find(name);
  return it != kReducers.end() ? it->second : Reducer::Func{};
}

PipelineStep MakeGroupStep(absl::Span<const std::string_view> fields,
                           std::vector<Reducer> reducers) {
  return GroupStep{std::vector<std::string>(fields.begin(), fields.end()), std::move(reducers)};
}

PipelineStep MakeSortStep(std::string field, bool descending) {
  return [field, descending](std::vector<DocValues> values) -> PipelineResult {
    std::sort(values.begin(), values.end(), [field](const DocValues& l, const DocValues& r) {
      auto it1 = l.find(field);
      auto it2 = r.find(field);
      return it1 == l.end() || (it2 != r.end() && it1->second < it2->second);
    });
    if (descending)
      std::reverse(values.begin(), values.end());
    return values;
  };
}

PipelineStep MakeLimitStep(size_t offset, size_t num) {
  return [offset, num](std::vector<DocValues> values) -> PipelineResult {
    values.erase(values.begin(), values.begin() + std::min(offset, values.size()));
    values.resize(std::min(num, values.size()));
    return values;
  };
}

PipelineResult Process(std::vector<DocValues> values, absl::Span<PipelineStep> steps) {
  for (auto& step : steps) {
    auto result = step(std::move(values));
    if (!result.has_value())
      return result;
    values = std::move(result.value());
  }
  return values;
}

}  // namespace dfly::aggregate
