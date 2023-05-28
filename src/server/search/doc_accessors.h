// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include "core/json_object.h"
#include "core/search/search.h"
#include "server/common.h"
#include "server/search/doc_index.h"
#include "server/table.h"

namespace dfly {

class StringMap;

// Document accessors allow different types (json/hset) to be hidden
// behind a document interface for quering fields and serializing.
struct BaseAccessor : public search::DocumentAccessor {
  using FieldConsumer = search::DocumentAccessor::FieldConsumer;

  // Convert underlying type to a map<string, string> to be sent as a reply
  virtual SearchDocData Serialize() const = 0;
};

// Accessor for hashes stored with listpack
struct ListPackAccessor : public BaseAccessor {
  using LpPtr = uint8_t*;

  explicit ListPackAccessor(LpPtr ptr) : lp_{ptr} {
  }

  bool Check(FieldConsumer f, std::string_view active_field) const override;
  SearchDocData Serialize() const override;

 private:
  LpPtr lp_;
};

// Accessor for hashes stored with StringMap
struct StringMapAccessor : public BaseAccessor {
  explicit StringMapAccessor(StringMap* hset) : hset_{hset} {
  }

  bool Check(FieldConsumer f, std::string_view active_field) const override;
  SearchDocData Serialize() const override;

 private:
  StringMap* hset_;
};

// Accessor for json values
struct JsonAccessor : public BaseAccessor {
  explicit JsonAccessor(JsonType* json) : json_{json} {
  }

  bool Check(FieldConsumer f, std::string_view active_field) const override;
  SearchDocData Serialize() const override;

 private:
  JsonType* json_;
};

// Get accessor for value
std::unique_ptr<BaseAccessor> GetAccessor(const OpArgs& op_args, const PrimeValue& pv);

}  // namespace dfly
