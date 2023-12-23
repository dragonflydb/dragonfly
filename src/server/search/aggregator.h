#pragma once

#include <absl/container/flat_hash_map.h>

#include <string>
#include <variant>

#include "facade/reply_builder.h"
#include "io/io.h"

namespace dfly::aggregate {

using Value = std::variant<double, std::string>;
using DocValues = absl::flat_hash_map<std::string, Value>;

using PipelineResult = io::Result<std::vector<DocValues>, facade::ErrorReply>;
using PipelineStep = std::function<PipelineResult(std::vector<DocValues>)>;

PipelineStep MakeGroupStep(absl::Span<const std::string_view> fields);
PipelineStep MakeSortStep(std::string field, bool descending = false);
PipelineStep MakeLimitStep(size_t offset, size_t num);

PipelineResult Execute(std::vector<DocValues> values, absl::Span<PipelineStep> steps);

}  // namespace dfly::aggregate
