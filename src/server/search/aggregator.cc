#include "server/search/aggregator.h"

#include "base/logging.h"

namespace dfly::aggregate {

namespace {

struct Reducer {
  std::string field;
  std::function<Value(absl::Span<const DocValues> values)> func;
};

struct GroupStep {
  GroupStep(std::vector<std::string> fields, std::vector<Reducer> reducers)
      : fields_{std::move(fields)}, reducers_{std::move(reducers)} {
  }

  PipelineResult operator()(std::vector<DocValues> values) {
    // Separate items into groups
    absl::flat_hash_map<absl::FixedArray<Value>, std::vector<DocValues>> groups;
    for (auto& value : values)
      groups[Extract(value)].push_back(std::move(value));

    // Restore DocValues and appy reducers
    std::vector<DocValues> out;
    while (!groups.empty()) {
      auto node = groups.extract(groups.begin());
      DocValues doc = Unpack(std::move(node.key()));
      for (auto& reducer : reducers_) {
        doc[reducer.field] = reducer.func(node.mapped());
      }
      out.push_back(std::move(doc));
    }
    return out;
  }

 private:
  absl::FixedArray<Value> Extract(const DocValues& dv) {
    absl::FixedArray<Value> out(fields_.size());
    for (size_t i = 0; i < fields_.size(); i++) {
      auto it = dv.find(fields_[i]);
      out[i] = (it != dv.end()) ? it->second : Value{0.0};
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

PipelineStep MakeGroupStep(absl::Span<const std::string_view> fields) {
  return GroupStep{std::vector<std::string>(fields.begin(), fields.end()), {}};
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

PipelineResult Execute(std::vector<DocValues> values, absl::Span<PipelineStep> steps) {
  for (auto& step : steps) {
    auto result = step(std::move(values));
    if (!result.has_value())
      return result;
    values = std::move(result.value());
  }
  return values;
}

}  // namespace dfly::aggregate
