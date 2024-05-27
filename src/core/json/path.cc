// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "src/core/json/path.h"

#include <absl/strings/str_cat.h>
#include <absl/types/span.h>

#include "base/logging.h"
#include "core/json/detail/flat_dfs.h"
#include "core/json/detail/jsoncons_dfs.h"
#include "core/json/jsonpath_grammar.hh"
#include "src/core/json/driver.h"
#include "src/core/overloaded.h"

using namespace std;
using nonstd::make_unexpected;

namespace dfly::json {

using detail::Dfs;
using detail::FlatDfs;

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

IndexExpr IndexExpr::Normalize(size_t array_len) const {
  if (array_len == 0)
    return IndexExpr(1, 0);  // empty range.

  IndexExpr res = *this;
  auto wrap = [array_len](int negative) {
    unsigned positive = -negative;
    return positive > array_len ? 0 : array_len - positive;
  };

  if (res.second >= int(array_len)) {
    res.second = array_len - 1;
  } else if (res.second < 0) {
    res.second = wrap(res.second);
    DCHECK_GE(res.second, 0);
  }
  if (res.first < 0) {
    res.first = wrap(res.first);
    DCHECK_GE(res.first, 0);
  }
  return res;
}

void PathSegment::Evaluate(const JsonType& json) const {
  CHECK(type() == SegmentType::FUNCTION);
  AggFunction* func = std::get<shared_ptr<AggFunction>>(value_).get();
  CHECK(func);
  func->Apply(json);
}

void PathSegment::Evaluate(FlatJson json) const {
  CHECK(type() == SegmentType::FUNCTION);
  AggFunction* func = std::get<shared_ptr<AggFunction>>(value_).get();
  CHECK(func);
  func->Apply(json);
}

AggFunction::Result PathSegment::GetResult() const {
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
    Dfs::Traverse(path, json, std::move(callback));
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
    Dfs::Traverse(path_tail, json, [&](auto, const JsonType& val) { func_segment.Evaluate(val); });
  }

  AggFunction::Result res = func_segment.GetResult();
  JsonType val = visit(  // Transform the result to JsonType.
      Overloaded{
          [](monostate) { return JsonType::null(); },
          [&](double d) { return JsonType(d); },

          [&](int64_t i) { return JsonType(i); },
      },
      res);
  callback(nullopt, val);
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

  Dfs dfs = Dfs::Mutate(path, callback, json);
  return dfs.matches();
}

// Flat json path evaluation
void EvaluatePath(const Path& path, FlatJson json, PathFlatCallback callback) {
  if (path.empty()) {  // root node
    callback(nullopt, json);
    return;
  }

  if (path.front().type() != SegmentType::FUNCTION) {
    FlatDfs::Traverse(path, json, std::move(callback));
    return;
  }

  // Handling the case of `func($.somepath)`
  // We pass our own callback to gather all the results and then call the function.
  FlatJson result;
  absl::Span<const PathSegment> path_tail(path.data() + 1, path.size() - 1);

  const PathSegment& func_segment = path.front();

  if (path_tail.empty()) {
    LOG(DFATAL) << "Invalid path";  // parser should not allow this.
  } else {
    FlatDfs::Traverse(path_tail, json, [&](auto, FlatJson val) { func_segment.Evaluate(val); });
  }
  AggFunction::Result res = func_segment.GetResult();
  flexbuffers::Builder fbb;
  FlatJson val = visit(  // Transform the result to a flexbuffer reference.
      Overloaded{
          [](monostate) { return FlatJson{}; },
          [&](double d) {
            fbb.Double(d);
            fbb.Finish();
            return flexbuffers::GetRoot(fbb.GetBuffer());
          },

          [&](int64_t i) {
            fbb.Int(i);
            fbb.Finish();
            return flexbuffers::GetRoot(fbb.GetBuffer());
          },
      },
      res);

  callback(nullopt, val);
}

JsonType FromFlat(FlatJson src) {
  if (src.IsNull()) {
    return JsonType::null();
  }

  if (src.IsBool()) {
    return JsonType(src.AsBool());
  }

  if (src.IsInt()) {
    return JsonType(src.AsInt64());
  }

  if (src.IsFloat()) {
    return JsonType(src.AsDouble());
  }
  if (src.IsString()) {
    flexbuffers::String str = src.AsString();
    return JsonType(string_view{str.c_str(), str.size()});
  }

  CHECK(src.IsVector());
  auto vec = src.AsVector();
  JsonType js =
      src.IsMap() ? JsonType{jsoncons::json_object_arg} : JsonType{jsoncons::json_array_arg};
  auto keys = src.AsMap().Keys();
  for (unsigned i = 0; i < vec.size(); ++i) {
    JsonType value = FromFlat(vec[i]);
    if (src.IsMap()) {
      js[keys[i].AsKey()] = std::move(value);
    } else {
      js.push_back(std::move(value));
    }
  }
  return js;
}

void FromJsonType(const JsonType& src, flexbuffers::Builder* fbb) {
  if (src.is_null()) {
    return fbb->Null();
  }

  if (src.is_bool()) {
    return fbb->Bool(src.as_bool());
  }

  if (src.is_int64()) {
    return fbb->Int(src.as<int64_t>());
  }

  if (src.is_double()) {
    return fbb->Double(src.as_double());
  }

  if (src.is_string()) {
    string_view sv = src.as_string_view();
    fbb->String(sv.data(), sv.size());
    return;
  }

  if (src.is_object()) {
    auto range = src.object_range();
    size_t start = fbb->StartMap();
    for (auto it = range.cbegin(); it != range.cend(); ++it) {
      fbb->Key(it->key().c_str(), it->key().size());
      FromJsonType(it->value(), fbb);
    }
    fbb->EndMap(start);
    return;
  }

  CHECK(src.is_array());
  auto range = src.array_range();
  size_t start = fbb->StartVector();
  for (auto it = range.cbegin(); it != range.cend(); ++it) {
    FromJsonType(*it, fbb);
  }
  fbb->EndVector(start, false, false);
}

unsigned MutatePath(const Path& path, MutateCallback callback, FlatJson json,
                    flexbuffers::Builder* fbb) {
  JsonType mut_json = FromFlat(json);
  unsigned res = MutatePath(path, std::move(callback), &mut_json);
  if (res) {
    FromJsonType(mut_json, fbb);
    fbb->Finish();
  }

  return res;
}

}  // namespace dfly::json
