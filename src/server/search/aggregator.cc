// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/aggregator.h"

#include <algorithm>
#include <cmath>

#include "base/logging.h"
#include "server/search/doc_index.h"
#include "server/search/filter_driver.h"
#include "server/search/filter_eval.h"

namespace rng = std::ranges;

namespace dfly::aggregate {

namespace {

using ValuesList = absl::FixedArray<Value>;

ValuesList ExtractFieldsValues(const DocValues& dv, absl::Span<const std::string> fields) {
  ValuesList out(fields.size());
  for (size_t i = 0; i < fields.size(); i++) {
    auto it = dv.find(fields[i]);
    if (it != dv.end()) {
      const Value& v = it->second;
      // Normalize NaN to monostate so that NaN values group together.
      if (std::holds_alternative<double>(v) && std::isnan(std::get<double>(v)))
        out[i] = Value{};
      else
        out[i] = v;
    }
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
  // A value is "sortable" if the field is present, not monostate (null), and not NaN.
  // Unsortable values sort after all sortable ones (regardless of ASC/DESC).
  // Different types (double vs string) are ordered by variant index
  // (ASC: doubles before strings, DESC: strings before doubles).
  auto sortable = [](auto it, auto end) -> const Value* {
    if (it == end)
      return nullptr;
    const Value& v = it->second;
    if (std::holds_alternative<std::monostate>(v))
      return nullptr;
    if (std::holds_alternative<double>(v) && std::isnan(std::get<double>(v)))
      return nullptr;
    return &v;
  };

  auto comparator = [&](const DocValues& l, const DocValues& r) {
    for (const auto& [field, order] : sort_params.fields) {
      const Value* lv = sortable(l.find(field), l.end());
      const Value* rv = sortable(r.find(field), r.end());

      // Both absent -- equal for this field; one absent -- present sorts first.
      if (!lv || !rv) {
        if (!lv && !rv)
          continue;
        return lv != nullptr;
      }

      // Different types -- order by variant index for strict weak ordering.
      if (lv->index() != rv->index())
        return order == SortOrder::ASC ? lv->index() < rv->index() : lv->index() > rv->index();

      if (*lv == *rv)
        continue;
      return order == SortOrder::ASC ? *lv < *rv : *lv > *rv;
    }
    return false;
  };

  auto& values = result.values;
  if (sort_params.SortAll()) {
    rng::sort(values, comparator);
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

  // Sum only finite numeric (double) values; non-finite (NaN, +/-Inf) and non-numeric are skipped.
  const static auto kSumReducer = [](ValueIterator it) -> std::pair<double, double> {
    double sum = 0;
    double count = 0;
    for (; it != it.end(); ++it) {
      if (std::holds_alternative<double>(*it)) {
        double d = std::get<double>(*it);
        if (std::isfinite(d)) {
          sum += d;
          count += 1;
        }
      }
    }
    return {sum, count};
  };

  switch (name) {
    case ReducerFunc::COUNT:
      return [](ValueIterator it) -> Value { return kCountReducer(it); };
    case ReducerFunc::COUNT_DISTINCT:
      return [](ValueIterator it) -> Value {
        absl::flat_hash_set<Value> seen;
        for (; it != it.end(); ++it) {
          // Normalize NaN to monostate to avoid NaN != NaN duplication in the set.
          if (std::holds_alternative<double>(*it) && std::isnan(std::get<double>(*it)))
            seen.insert(Value{});
          else
            seen.insert(*it);
        }
        return static_cast<double>(seen.size());
      };
    case ReducerFunc::SUM:
      return [](ValueIterator it) -> Value { return kSumReducer(it).first; };
    case ReducerFunc::AVG:
      return [](ValueIterator it) -> Value {
        auto [sum, count] = kSumReducer(it);
        return count > 0 ? Value{sum / count} : Value{};
      };
    case ReducerFunc::MAX:
      return [](ValueIterator it) -> Value {
        Value result{};
        for (; it != it.end(); ++it) {
          const Value& v = *it;
          if (std::holds_alternative<std::monostate>(v))
            continue;
          if (std::holds_alternative<double>(v) && std::isnan(std::get<double>(v)))
            continue;
          if (std::holds_alternative<std::monostate>(result) || result < v)
            result = v;
        }
        return result;
      };
    case ReducerFunc::MIN:
      return [](ValueIterator it) -> Value {
        Value result{};
        for (; it != it.end(); ++it) {
          const Value& v = *it;
          if (std::holds_alternative<std::monostate>(v))
            continue;
          if (std::holds_alternative<double>(v) && std::isnan(std::get<double>(v)))
            continue;
          if (std::holds_alternative<std::monostate>(result) || v < result)
            result = v;
        }
        return result;
      };
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

void Aggregator::DoFilter(const FilterExprNode& expr) {
  auto& values = result.values;
  std::erase_if(values, [&](const DocValues& doc) { return !IsTruthy(EvalFilterExpr(expr, doc)); });
}

std::variant<AggregationStep, std::string> MakeFilterStep(std::string_view raw_expr) {
  FilterParseResult parsed = ParseFilterExpr(raw_expr);
  if (!std::holds_alternative<FilterExpr>(parsed))
    return std::get<std::string>(std::move(parsed));

  // Wrap in shared_ptr so the AST can be captured by copy into std::function.
  auto shared = std::shared_ptr<FilterExprNode>(std::get<FilterExpr>(std::move(parsed)).release());
  return AggregationStep{[shared](Aggregator* agg) { agg->DoFilter(*shared); }};
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
