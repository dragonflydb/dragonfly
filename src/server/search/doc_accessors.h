// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/types/span.h>

#include <functional>
#include <string>

#include "core/detail/listpack_wrap.h"
#include "core/json/json_object.h"
#include "core/search/search.h"
#include "core/search/vector_utils.h"
#include "server/search/doc_index.h"
#include "server/table.h"

namespace dfly {

class StringMap;

// Document accessors allow different types (json/hset) to be hidden
// behind a document interface for quering fields and serializing.
// Field string_view's are only valid until the next is requested.
struct BaseAccessor : public search::DocumentAccessor {
  // Serialize all fields
  virtual SearchDocData Serialize(const search::Schema& schema) const = 0;

  // Serialize selected fields
  virtual SearchDocData Serialize(const search::Schema& schema,
                                  absl::Span<const FieldReference> fields) const;

  // Default implementation uses GetStrings
  virtual std::optional<VectorInfo> GetVector(std::string_view active_field,
                                              size_t dim) const override;
  virtual std::optional<NumsList> GetNumbers(std::string_view active_field) const override;
  virtual std::optional<StringList> GetTags(std::string_view active_field) const override;
};

// Accessor for hashes stored with listpack
struct ListPackAccessor : public BaseAccessor {
  explicit ListPackAccessor(uint8_t* ptr /* listpack ptr */) : lw_{ptr} {
  }

  std::optional<StringList> GetStrings(std::string_view field) const override;
  SearchDocData Serialize(const search::Schema& schema) const override;

 private:
  detail::ListpackWrap lw_;
};

// Accessor for hashes stored with StringMap.
// If cleanup context is supplied, the destructor deletes the underlying key
// when the StringMap becomes empty (e.g. from lazy field expiry during
// Serialize).  This keeps post-access cleanup out of search logic.
struct StringMapAccessor : public BaseAccessor {
  explicit StringMapAccessor(StringMap* hset) : hset_{hset} {
  }

  StringMapAccessor(StringMap* hset, std::string cleanup_key, DbContext db_cntx,
                    const PrimeValue* pv)
      : hset_{hset}, cleanup_key_{std::move(cleanup_key)}, db_cntx_{db_cntx}, cleanup_pv_{pv} {
  }

  ~StringMapAccessor() override;

  std::optional<StringList> GetStrings(std::string_view field) const override;
  SearchDocData Serialize(const search::Schema& schema) const override;

 private:
  StringMap* hset_;
  // Cleanup state (empty key means no cleanup requested).
  std::string cleanup_key_;
  DbContext db_cntx_{};
  const PrimeValue* cleanup_pv_ = nullptr;
};

// Accessor for json values
struct JsonAccessor : public BaseAccessor {
  struct JsonPathContainer;  // contains jsoncons::jsonpath::jsonpath_expression

  explicit JsonAccessor(const JsonType* json) : json_{*json} {
  }

  std::optional<StringList> GetStrings(std::string_view field) const override;
  std::optional<VectorInfo> GetVector(std::string_view field, size_t dim) const override;
  std::optional<NumsList> GetNumbers(std::string_view active_field) const override;
  std::optional<StringList> GetTags(std::string_view active_field) const override;

  // The JsonAccessor works with structured types and not plain strings, so an overload is needed
  SearchDocData Serialize(const search::Schema& schema,
                          absl::Span<const FieldReference> fields) const override;
  SearchDocData Serialize(const search::Schema& schema) const override;

  static void RemoveFieldFromCache(std::string_view field);

 private:
  /* If accept_boolean_values is true, then json boolean values are converted to strings */
  std::optional<StringList> GetStrings(std::string_view field, bool accept_boolean_values) const;

  /// Parses `field` into a JSON path. Caches the results internally.
  JsonPathContainer* GetPath(std::string_view field) const;

  const JsonType& json_;
  mutable std::string buf_;

  // Contains built json paths to avoid parsing them repeatedly
  static thread_local absl::flat_hash_map<std::string, std::unique_ptr<JsonPathContainer>>
      path_cache_;
};

// Get accessor for value
// If cleanup_key is non-empty, the returned StringMapAccessor (if any) will
// delete the DB key in its destructor when the hash has become empty.
std::unique_ptr<BaseAccessor> GetAccessor(const DbContext& db_cntx, const PrimeValue& pv,
                                          std::string_view cleanup_key = {});

}  // namespace dfly
