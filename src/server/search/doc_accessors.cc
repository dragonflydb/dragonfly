// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/doc_accessors.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

#include "core/json_object.h"
#include "core/search/search.h"
#include "core/search/vector_utils.h"
#include "core/string_map.h"
#include "server/container_utils.h"

extern "C" {
#include "redis/listpack.h"
#include "redis/object.h"
};

namespace dfly {

using namespace std;

namespace {

string_view SdsToSafeSv(sds str) {
  return str != nullptr ? string_view{str, sdslen(str)} : ""sv;
}

string PrintField(search::SchemaField::FieldType type, string_view value) {
  if (type == search::SchemaField::VECTOR) {
    auto [ptr, size] = search::BytesToFtVector(value);
    return absl::StrCat("[", absl::StrJoin(absl::Span<const float>{ptr.get(), size}, ","), "]");
  }
  return string{value};
}

string ExtractValue(const search::Schema& schema, string_view key, string_view value) {
  auto it = schema.fields.find(key);
  if (it == schema.fields.end())
    return string{value};

  return PrintField(it->second.type, value);
}

}  // namespace

SearchDocData BaseAccessor::Serialize(const search::Schema& schema,
                                      const SearchParams::FieldReturnList& fields) const {
  SearchDocData out{};
  for (const auto& [fident, fname] : fields) {
    auto it = schema.fields.find(fident);
    auto type = it != schema.fields.end() ? it->second.type : search::SchemaField::TEXT;
    out[fname] = PrintField(type, absl::StrJoin(GetStrings(fident), ","));
  }
  return out;
}

BaseAccessor::StringList ListPackAccessor::GetStrings(string_view active_field) const {
  auto strsv = container_utils::LpFind(lp_, active_field, intbuf_[0].data());
  return strsv.has_value() ? StringList{*strsv} : StringList{};
}

BaseAccessor::VectorInfo ListPackAccessor::GetVector(string_view active_field) const {
  auto strlist = GetStrings(active_field);
  return strlist.empty() ? VectorInfo{} : search::BytesToFtVector(strlist.front());
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

    out[k] = ExtractValue(schema, k, v);
  }

  return out;
}

BaseAccessor::StringList StringMapAccessor::GetStrings(string_view active_field) const {
  auto it = hset_->Find(active_field);
  return it != hset_->end() ? StringList{it->second} : StringList{};
}

BaseAccessor::VectorInfo StringMapAccessor::GetVector(string_view active_field) const {
  auto strlist = GetStrings(active_field);
  return strlist.empty() ? VectorInfo{} : search::BytesToFtVector(strlist.front());
}

SearchDocData StringMapAccessor::Serialize(const search::Schema& schema) const {
  SearchDocData out{};
  for (const auto& [kptr, vptr] : *hset_)
    out[SdsToSafeSv(kptr)] = ExtractValue(schema, SdsToSafeSv(kptr), SdsToSafeSv(vptr));

  return out;
}

struct JsonAccessor::JsonPathContainer : public jsoncons::jsonpath::jsonpath_expression<JsonType> {
};

BaseAccessor::StringList JsonAccessor::GetStrings(string_view active_field) const {
  auto path_res = GetPath(active_field)->evaluate(json_);
  DCHECK(path_res.is_array());  // json path always returns arrays

  if (path_res.empty())
    return {};

  if (path_res.size() == 1) {
    buf_ = path_res[0].as_string();
    return {buf_};
  }

  StringList out;

  // First, grow buffer and compute string sizes
  for (auto element : path_res.array_range()) {
    size_t start = buf_.size();
    buf_ += element.as_string();
    out.push_back(string_view{nullptr, buf_.size() - start});
  }

  // Reposition start pointers to the most recent allocation of buf
  size_t start = 0;
  for (auto& sv : out) {
    sv = string_view{buf_.data() + start, sv.size()};
    start += sv.size();
  }

  return out;
}

BaseAccessor::VectorInfo JsonAccessor::GetVector(string_view active_field) const {
  auto res = GetPath(active_field)->evaluate(json_);
  DCHECK(res.is_array());
  if (res.empty())
    return {nullptr, 0};

  size_t size = res[0].size();
  auto ptr = make_unique<float[]>(size);

  size_t i = 0;
  for (auto v : res[0].array_range())
    ptr[i++] = v.as<float>();

  return {std::move(ptr), size};
}

JsonAccessor::JsonPathContainer* JsonAccessor::GetPath(std::string_view field) const {
  if (auto it = path_cache_.find(field); it != path_cache_.end()) {
    return it->second.get();
  }

  error_code ec;
  auto path_expr = jsoncons::jsonpath::make_expression<JsonType>(field, ec);
  DCHECK(!ec) << "missing validation on ft.create step";

  JsonPathContainer path_container{move(path_expr)};
  auto ptr = make_unique<JsonPathContainer>(move(path_container));

  JsonPathContainer* path = ptr.get();
  path_cache_[field] = move(ptr);
  return path;
}

SearchDocData JsonAccessor::Serialize(const search::Schema& schema) const {
  return {{"$", json_.to_string()}};
}

SearchDocData JsonAccessor::Serialize(const search::Schema& schema,
                                      const SearchParams::FieldReturnList& fields) const {
  SearchDocData out{};
  for (const auto& [ident, name] : fields)
    out[name] = GetPath(ident)->evaluate(json_).to_string();
  return out;
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
