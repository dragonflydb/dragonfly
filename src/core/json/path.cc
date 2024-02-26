// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "src/core/json/path.h"

#include <absl/strings/str_cat.h>
#include <absl/types/span.h>

#include "base/logging.h"
#include "core/json/detail/jsoncons_dfs.h"
#include "core/json/jsonpath_grammar.hh"
#include "src/core/json/driver.h"
#include "src/core/overloaded.h"

using namespace std;
using nonstd::make_unexpected;

namespace dfly::json {

namespace {
class JsonPathDriver : public json::Driver {
 public:
  string msg;
  void Error(const json::location& l, const std::string& msg) final {
    this->msg = absl::StrCat("Error: ", msg);
  }
};

}  // namespace

const char* SegmentName(SegmentType type) {
  switch (type) {
    case SegmentType::IDENTIFIER:
      return "IDENTIFIER";
    case SegmentType::INDEX:
      return "INDEX";
    case SegmentType::WILDCARD:
      return "WILDCARD";
    case SegmentType::DESCENT:
      return "DESCENT";
    case SegmentType::FUNCTION:
      return "FUNCTION";
  }
  return nullptr;
}

void PathSegment::Evaluate(const JsonType& json) const {
  CHECK(type() == SegmentType::FUNCTION);
  AggFunction* func = std::get<shared_ptr<AggFunction>>(value_).get();
  CHECK(func);
  func->Apply(json);
}

JsonType PathSegment::GetResult() const {
  CHECK(type() == SegmentType::FUNCTION);
  const auto& func = std::get<shared_ptr<AggFunction>>(value_).get();
  CHECK(func);
  return func->GetResult();
}

void EvaluatePath(const Path& path, const JsonType& json, PathCallback callback) {
  if (path.empty()) {  // root node
    callback(nullopt, json);
    return;
  }

  if (path.front().type() != SegmentType::FUNCTION) {
    detail::Dfs().Traverse(path, json, std::move(callback));
    return;
  }

  // Handling the case of `func($.somepath)`
  // We pass our own callback to gather all the results and then call the function.
  JsonType result(JsonType::null());
  absl::Span<const PathSegment> path_tail(path.data() + 1, path.size() - 1);

  const PathSegment& func_segment = path.front();

  if (path_tail.empty()) {
    LOG(DFATAL) << "Invalid path";  // parser should not allow this.
  } else {
    detail::Dfs().Traverse(path_tail, json,
                           [&](auto, const JsonType& val) { func_segment.Evaluate(val); });
  }
  callback(nullopt, func_segment.GetResult());
}

nonstd::expected<json::Path, string> ParsePath(string_view path) {
  if (path.size() > 8192)
    return nonstd::make_unexpected("Path too long");

  VLOG(2) << "Parsing path: " << path;

  JsonPathDriver driver;
  Parser parser(&driver);

  driver.SetInput(string(path));
  int res = parser();
  if (res != 0) {
    return nonstd::make_unexpected(driver.msg);
  }

  return driver.TakePath();
}

unsigned MutatePath(const Path& path, MutateCallback callback, JsonType* json) {
  if (path.empty()) {
    callback(nullopt, json);
    return 1;
  }

  detail::Dfs dfs;
  dfs.Mutate(path, callback, json);
  return dfs.matches();
}

}  // namespace dfly::json
