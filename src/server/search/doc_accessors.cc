// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

// GCC yields a spurious warning about uninitialized data in DocumentAccessor::StringList.
#ifndef __clang__
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

#include "server/search/doc_accessors.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include "base/flags.h"
#include "core/json/path.h"
#include "core/overloaded.h"
#include "core/search/search.h"
#include "core/search/vector_utils.h"
#include "core/string_map.h"
#include "server/container_utils.h"

extern "C" {
#include "redis/listpack.h"
};

ABSL_DECLARE_FLAG(bool, jsonpathv2);

namespace dfly {

using namespace std;

namespace {

string_view SdsToSafeSv(sds str) {
  return str != nullptr ? string_view{str, sdslen(str)} : ""sv;
}

using FieldValue = std::optional<search::SortableValue>;

FieldValue ToSortableValue(search::SchemaField::FieldType type, string_view value) {
  if (value.empty()) {
    return std::nullopt;
  }

  if (type == search::SchemaField::NUMERIC) {
    auto value_as_double = search::ParseNumericField(value);
    if (!value_as_double) {  // temporary convert to double
      LOG(DFATAL) << "Failed to convert " << value << " to double";
      return std::nullopt;
    }
    return value_as_double.value();
  }
  if (type == search::SchemaField::VECTOR) {
    auto opt_vector = search::BytesToFtVectorSafe(value);
    if (!opt_vector) {
      LOG(DFATAL) << "Failed to convert " << value << " to vector";
      return std::nullopt;
    }
    auto& [ptr, size] = opt_vector.value();
    return absl::StrCat("[", absl::StrJoin(absl::Span<const float>{ptr.get(), size}, ","), "]");
  }
  return string{value};
}

FieldValue ExtractSortableValue(const search::Schema& schema, string_view key, string_view value) {
  auto it = schema.fields.find(key);
  if (it == schema.fields.end())
    return ToSortableValue(search::SchemaField::TEXT, value);
  return ToSortableValue(it->second.type, value);
}

FieldValue ExtractSortableValueFromJson(const search::Schema& schema, string_view key,
                                        const JsonType& json) {
  auto json_as_string = json.to_string();
  return ExtractSortableValue(schema, key, json_as_string);
}

}  // namespace

SearchDocData BaseAccessor::Serialize(const search::Schema& schema,
                                      absl::Span<const SearchField> fields) const {
  SearchDocData out{};
  for (const auto& field : fields) {
    const auto& fident = field.GetIdentifier(schema, false);
    const auto& fname = field.GetShortName(schema);

    auto field_value =
        ExtractSortableValue(schema, fident, absl::StrJoin(GetStrings(fident).value(), ","));
    if (field_value) {
      out[fname] = std::move(field_value).value();
    }
  }
  return out;
}

SearchDocData BaseAccessor::SerializeDocument(const search::Schema& schema) const {
  return Serialize(schema);
}

std::optional<BaseAccessor::VectorInfo> BaseAccessor::GetVector(
    std::string_view active_field) const {
  auto strings_list = GetStrings(active_field);
  if (strings_list) {
    return !strings_list->empty() ? search::BytesToFtVectorSafe(strings_list->front())
                                  : VectorInfo{};
  }
  return std::nullopt;
}

std::optional<BaseAccessor::NumsList> BaseAccessor::GetNumbers(
    std::string_view active_field) const {
  auto strings_list = GetStrings(active_field);
  if (!strings_list) {
    return std::nullopt;
  }

  NumsList nums_list;
  nums_list.reserve(strings_list->size());
  for (auto str : strings_list.value()) {
    auto num = search::ParseNumericField(str);
    if (!num) {
      return std::nullopt;
    }
    nums_list.push_back(num.value());
  }
  return nums_list;
}

std::optional<BaseAccessor::StringList> ListPackAccessor::GetStrings(
    string_view active_field) const {
  auto strsv = container_utils::LpFind(lp_, active_field, intbuf_[0].data());
  return strsv.has_value() ? StringList{*strsv} : StringList{};
}

SearchDocData ListPackAccessor::Serialize(const search::Schema& schema) const {
  SearchDocData out{};

  uint8_t* fptr = lpFirst(lp_);
  DCHECK_NE(fptr, nullptr);

  while (fptr) {
    string_view k = container_utils::LpGetView(fptr, intbuf_[0].data());
    fptr = lpNext(lp_, fptr);
    string_view v = container_utils::LpGetView(fptr, intbuf_[1].data());
    fptr = lpNext(lp_, fptr);

    auto field_value = ExtractSortableValue(schema, k, v);
    if (field_value) {
      out[k] = std::move(field_value).value();
    }
  }

  return out;
}

std::optional<BaseAccessor::StringList> StringMapAccessor::GetStrings(
    string_view active_field) const {
  auto it = hset_->Find(active_field);
  return it != hset_->end() ? StringList{SdsToSafeSv(it->second)} : StringList{};
}

SearchDocData StringMapAccessor::Serialize(const search::Schema& schema) const {
  SearchDocData out{};
  for (const auto& [kptr, vptr] : *hset_) {
    auto field_value = ExtractSortableValue(schema, SdsToSafeSv(kptr), SdsToSafeSv(vptr));
    if (field_value) {
      out[SdsToSafeSv(kptr)] = std::move(field_value).value();
    }
  }
  return out;
}

struct JsonAccessor::JsonPathContainer {
  vector<JsonType> Evaluate(const JsonType& json) const {
    vector<JsonType> res;

    visit(Overloaded{[&](const json::Path& path) {
                       json::EvaluatePath(path, json,
                                          [&](auto, const JsonType& v) { res.push_back(v); });
                     },
                     [&](const jsoncons::jsonpath::jsonpath_expression<JsonType>& path) {
                       auto json_arr = path.evaluate(json);
                       for (const auto& v : json_arr.array_range())
                         res.push_back(v);
                     }},
          val);

    return res;
  }

  variant<json::Path, jsoncons::jsonpath::jsonpath_expression<JsonType>> val;
};

std::optional<BaseAccessor::StringList> JsonAccessor::GetStrings(string_view active_field) const {
  auto* path = GetPath(active_field);
  if (!path)
    return search::EmptyAccessResult<StringList>();

  auto path_res = path->Evaluate(json_);
  if (path_res.empty())
    return search::EmptyAccessResult<StringList>();

  if (path_res.size() == 1 && !path_res[0].is_array()) {
    if (!path_res[0].is_string())
      return std::nullopt;

    buf_ = path_res[0].as_string();
    return StringList{buf_};
  }

  buf_.clear();

  // First, grow buffer and compute string sizes
  vector<size_t> sizes;

  auto add_json_to_buf = [&](const JsonType& json) {
    size_t start = buf_.size();
    buf_ += json.as_string();
    sizes.push_back(buf_.size() - start);
  };

  if (!path_res[0].is_array()) {
    sizes.reserve(path_res.size());
    for (const auto& element : path_res) {
      if (!element.is_string())
        return std::nullopt;

      add_json_to_buf(element);
    }
  } else {
    if (path_res.size() > 1) {
      return std::nullopt;
    }

    sizes.reserve(path_res[0].size());
    for (const auto& element : path_res[0].array_range()) {
      if (!element.is_string())
        return std::nullopt;

      add_json_to_buf(element);
    }
  }

  // Reposition start pointers to the most recent allocation of buf
  StringList out(sizes.size());

  size_t start = 0;
  for (size_t i = 0; i < out.size(); i++) {
    out[i] = string_view{buf_}.substr(start, sizes[i]);
    start += sizes[i];
  }

  return out;
}

std::optional<BaseAccessor::VectorInfo> JsonAccessor::GetVector(string_view active_field) const {
  auto* path = GetPath(active_field);
  if (!path)
    return VectorInfo{};

  auto res = path->Evaluate(json_);
  if (res.empty())
    return VectorInfo{};

  if (!res[0].is_array())
    return std::nullopt;

  size_t size = res[0].size();
  auto ptr = make_unique<float[]>(size);

  size_t i = 0;
  for (const auto& v : res[0].array_range()) {
    if (!v.is_number()) {
      return std::nullopt;
    }
    ptr[i++] = v.as<float>();
  }

  return BaseAccessor::VectorInfo{std::move(ptr), size};
}

std::optional<BaseAccessor::NumsList> JsonAccessor::GetNumbers(string_view active_field) const {
  auto* path = GetPath(active_field);
  if (!path)
    return search::EmptyAccessResult<NumsList>();

  auto path_res = path->Evaluate(json_);
  if (path_res.empty())
    return search::EmptyAccessResult<NumsList>();

  NumsList nums_list;
  if (!path_res[0].is_array()) {
    nums_list.reserve(path_res.size());
    for (const auto& element : path_res) {
      if (!element.is_number())
        return std::nullopt;
      nums_list.push_back(element.as<double>());
    }
  } else {
    if (path_res.size() > 1) {
      return std::nullopt;
    }

    nums_list.reserve(path_res[0].size());
    for (const auto& element : path_res[0].array_range()) {
      if (!element.is_number())
        return std::nullopt;
      nums_list.push_back(element.as<double>());
    }
  }
  return nums_list;
}

JsonAccessor::JsonPathContainer* JsonAccessor::GetPath(std::string_view field) const {
  if (auto it = path_cache_.find(field); it != path_cache_.end()) {
    return it->second.get();
  }

  string ec_msg;
  unique_ptr<JsonPathContainer> ptr;
  if (absl::GetFlag(FLAGS_jsonpathv2)) {
    auto path_expr = json::ParsePath(field);
    if (path_expr) {
      ptr.reset(new JsonPathContainer{std::move(path_expr.value())});
    } else {
      ec_msg = path_expr.error();
    }
  } else {
    error_code ec;
    auto path_expr = MakeJsonPathExpr(field, ec);
    if (ec) {
      ec_msg = ec.message();
    } else {
      ptr.reset(new JsonPathContainer{std::move(path_expr)});
    }
  }

  if (!ptr) {
    LOG(WARNING) << "Invalid Json path: " << field << ' ' << ec_msg;
    return nullptr;
  }

  JsonPathContainer* path = ptr.get();
  path_cache_[field] = std::move(ptr);
  return path;
}

SearchDocData JsonAccessor::Serialize(const search::Schema& schema) const {
  SearchFieldsList fields{};
  for (const auto& [fname, fident] : schema.field_names)
    fields.emplace_back(StringOrView::FromView(fident), false, StringOrView::FromView(fname));
  return Serialize(schema, fields);
}

SearchDocData JsonAccessor::Serialize(const search::Schema& schema,
                                      absl::Span<const SearchField> fields) const {
  SearchDocData out{};
  for (const auto& field : fields) {
    const auto& ident = field.GetIdentifier(schema, true);
    const auto& name = field.GetShortName(schema);
    if (auto* path = GetPath(ident); path) {
      if (auto res = path->Evaluate(json_); !res.empty()) {
        auto field_value = ExtractSortableValueFromJson(schema, ident, res[0]);
        if (field_value) {
          out[name] = std::move(field_value).value();
        }
      }
    }
  }
  return out;
}

SearchDocData JsonAccessor::SerializeDocument(const search::Schema& schema) const {
  return {{"$", json_.to_string()}};
}

void JsonAccessor::RemoveFieldFromCache(string_view field) {
  path_cache_.erase(field);
}

thread_local absl::flat_hash_map<std::string, std::unique_ptr<JsonAccessor::JsonPathContainer>>
    JsonAccessor::path_cache_;

unique_ptr<BaseAccessor> GetAccessor(const DbContext& db_cntx, const PrimeValue& pv) {
  DCHECK(pv.ObjType() == OBJ_HASH || pv.ObjType() == OBJ_JSON);

  if (pv.ObjType() == OBJ_JSON) {
    DCHECK(pv.GetJson());
    return make_unique<JsonAccessor>(pv.GetJson());
  }

  if (pv.Encoding() == kEncodingListPack) {
    auto ptr = reinterpret_cast<ListPackAccessor::LpPtr>(pv.RObjPtr());
    return make_unique<ListPackAccessor>(ptr);
  } else {
    auto* sm = container_utils::GetStringMap(pv, db_cntx);
    return make_unique<StringMapAccessor>(sm);
  }
}

}  // namespace dfly
