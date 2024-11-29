// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/aggregator.h"

#include "base/logging.h"

namespace dfly::aggregate {

namespace {

struct GroupStep {
  PipelineResult operator()(PipelineResult result) {
    // Separate items into groups
    absl::flat_hash_map<absl::FixedArray<Value>, std::vector<DocValues>> groups;
    for (auto& value : result.values) {
      groups[Extract(value)].push_back(std::move(value));
    }

    // Restore DocValues and apply reducers
    std::vector<DocValues> out;
    while (!groups.empty()) {
      auto node = groups.extract(groups.begin());
      DocValues doc = Unpack(std::move(node.key()));
      for (auto& reducer : reducers_) {
        doc[reducer.result_field] = reducer.func({reducer.source_field, node.mapped()});
      }
      out.push_back(std::move(doc));
    }

    absl::flat_hash_set<std::string> fields_to_print;
    fields_to_print.reserve(fields_.size() + reducers_.size());

    for (auto& field : fields_) {
      fields_to_print.insert(std::move(field));
    }
    for (auto& reducer : reducers_) {
      fields_to_print.insert(std::move(reducer.result_field));
    }

    return {std::move(out), std::move(fields_to_print)};
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

const Value kEmptyValue = Value{};

}  // namespace

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

PipelineStep MakeGroupStep(absl::Span<const std::string_view> fields,
                           std::vector<Reducer> reducers) {
  return GroupStep{std::vector<std::string>(fields.begin(), fields.end()), std::move(reducers)};
}

PipelineStep MakeSortStep(std::string_view field, bool descending) {
  return [field = std::string(field), descending](PipelineResult result) -> PipelineResult {
    auto& values = result.values;

    std::sort(values.begin(), values.end(), [field](const DocValues& l, const DocValues& r) {
      auto it1 = l.find(field);
      auto it2 = r.find(field);
      return it1 == l.end() || (it2 != r.end() && it1->second < it2->second);
    });

    if (descending) {
      std::reverse(values.begin(), values.end());
    }

    result.fields_to_print.insert(field);
    return result;
  };
}

PipelineStep MakeLimitStep(size_t offset, size_t num) {
  return [offset, num](PipelineResult result) {
    auto& values = result.values;
    values.erase(values.begin(), values.begin() + std::min(offset, values.size()));
    values.resize(std::min(num, values.size()));
    return result;
  };
}

PipelineResult Process(std::vector<DocValues> values,
                       absl::Span<const std::string_view> fields_to_print,
                       absl::Span<const PipelineStep> steps) {
  PipelineResult result{std::move(values), {fields_to_print.begin(), fields_to_print.end()}};
  for (auto& step : steps) {
    PipelineResult step_result = step(std::move(result));
    result = std::move(step_result);
  }
  return result;
}

}  // namespace dfly::aggregate
