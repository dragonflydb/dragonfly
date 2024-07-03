// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "core/wrapped_json_path.h"
#include "facade/reply_builder.h"

namespace facade {

struct NullType {
  explicit NullType(){};
};

struct NullArrayType {
  explicit NullArrayType(){};
};

struct EmptyArrayType {
  explicit EmptyArrayType(){};
};

struct StringArr {
  using StrSpan = RedisReplyBuilder::StrSpan;
  using CollectionType = RedisReplyBuilder::CollectionType;

  StrSpan arr;
  CollectionType type = CollectionType::ARRAY;
};

struct SimpleString {
  std::string_view str;
};

struct BulkString {
  std::string_view str;
};

struct VerbatimString {
  using VerbatimFormat = RedisReplyBuilder::VerbatimFormat;

  std::string_view str;
  VerbatimFormat format = VerbatimFormat::TXT;
};

struct ScoredArray {
  const std::vector<std::pair<std::string, double>>& arr;
  bool with_scores;
};

class GenericReplyBuilder;

namespace reply_builder_generic {

template <typename T> void Send(GenericReplyBuilder& rb, T& value) {
}

template <typename T> void Send(GenericReplyBuilder& rb, std::optional<T>& opt);

template <typename T> void Send(GenericReplyBuilder& rb, std::vector<T>& vec);

template <> void Send(GenericReplyBuilder& rb, std::vector<bool>& vec);

template <typename T> void Send(GenericReplyBuilder& rb, dfly::JsonCallbackResult<T>& value);

template <typename T> void Send(GenericReplyBuilder& rb, dfly::JsonV1CallbackResult<T>& value);

template <typename T> void Send(GenericReplyBuilder& rb, dfly::JsonV2CallbackResult<T>& value);

template <> void Send(GenericReplyBuilder& rb, long& value);

template <> void Send(GenericReplyBuilder& rb, unsigned long& value);

template <> void Send(GenericReplyBuilder& rb, double& value);

template <> void Send(GenericReplyBuilder& rb, bool& value);

template <> void Send(GenericReplyBuilder& rb, RedisReplyBuilder::StrSpan& value);

template <> void Send(GenericReplyBuilder& rb, StringArr& value);

template <> void Send(GenericReplyBuilder& rb, SimpleString& value);

template <> void Send(GenericReplyBuilder& rb, BulkString& value);

template <> void Send(GenericReplyBuilder& rb, VerbatimString& value);

template <> void Send(GenericReplyBuilder& rb, ScoredArray& value);

}  // namespace reply_builder_generic

class GenericReplyBuilder : public RedisReplyBuilder {
 public:
  explicit GenericReplyBuilder(RedisReplyBuilder* reply_builder)
      : RedisReplyBuilder(nullptr), reply_builder_(reply_builder) {
  }

  void SendNullArray() override {
    reply_builder_->SendNullArray();
  }

  void SendEmptyArray() override {
    reply_builder_->SendEmptyArray();
  }

  void SendSimpleStrArr(RedisReplyBuilder::StrSpan arr) override {
    reply_builder_->SendSimpleStrArr(std::move(arr));
  }

  void SendStringArr(RedisReplyBuilder::StrSpan arr,
                     RedisReplyBuilder::CollectionType type) override {
    reply_builder_->SendStringArr(std::move(arr), type);
  }

  void SendNull() override {
    reply_builder_->SendNull();
  }

  void SendLong(long val) override {
    reply_builder_->SendLong(val);
  }

  void SendDouble(double val) override {
    reply_builder_->SendDouble(val);
  }

  void SendSimpleString(std::string_view str) override {
    reply_builder_->SendSimpleString(str);
  }

  void SendBulkString(std::string_view str) override {
    reply_builder_->SendBulkString(str);
  }

  void SendVerbatimString(std::string_view str, RedisReplyBuilder::VerbatimFormat format) override {
    reply_builder_->SendVerbatimString(str, format);
  }

  void SendScoredArray(const std::vector<std::pair<std::string, double>>& arr,
                       bool with_scores) override {
    reply_builder_->SendScoredArray(arr, with_scores);
  }

  void StartCollection(unsigned len, RedisReplyBuilder::CollectionType type) override {
    reply_builder_->StartCollection(len, type);
  }

  template <typename T> void Send(T& value) {
    reply_builder_generic::Send(*this, value);
  }

  template <typename T> void Send(std::vector<T>& value) {
    reply_builder_generic::Send(*this, value);
  }

  void Send(std::vector<bool>& value) {
    reply_builder_generic::Send<>(*this, value);
  }

  template <typename T> void Send(std::optional<T>& value) {
    reply_builder_generic::Send(*this, value);
  }

  template <typename T> void Send(dfly::JsonCallbackResultPtr<T>& value) {
    reply_builder_generic::Send(*this, *value);
  }

 private:
  RedisReplyBuilder* reply_builder_;
};

GenericReplyBuilder AsGenericReplyBuilder(SinkReplyBuilder* reply_builder) {
  return GenericReplyBuilder{static_cast<RedisReplyBuilder*>(reply_builder)};
}

namespace reply_builder_generic {

template <typename T> void Send(GenericReplyBuilder& rb, std::optional<T>& opt) {
  if (opt.has_value()) {
    Send(rb, opt.value());
  } else {
    rb.SendNull();
  }
}

template <typename T> void Send(GenericReplyBuilder& rb, std::vector<T>& vec) {
  if (vec.empty()) {
    rb.SendNullArray();
  } else {
    rb.StartArray(vec.size());
    for (T& x : vec) {
      Send(rb, x);
    }
  }
}

template <> void Send(GenericReplyBuilder& rb, std::vector<bool>& vec) {
  if (vec.empty()) {
    rb.SendNullArray();
  } else {
    rb.StartArray(vec.size());
    for (bool x : vec) {
      Send(rb, x);
    }
  }
}

template <> void Send(GenericReplyBuilder& rb, long& value) {
  rb.SendLong(value);
}

template <> void Send(GenericReplyBuilder& rb, unsigned long& value) {
  rb.SendLong(value);
}

template <> void Send(GenericReplyBuilder& rb, double& value) {
  rb.SendDouble(value);
}

template <> void Send(GenericReplyBuilder& rb, bool& value) {
  if (value) {
    rb.SendOk();
  } else {
    rb.SendNull();
  }
}

template <> void Send(GenericReplyBuilder& rb, RedisReplyBuilder::StrSpan& value) {
  rb.SendSimpleStrArr(std::move(value));
}

template <> void Send(GenericReplyBuilder& rb, StringArr& value) {
  rb.SendStringArr(std::move(value.arr), value.type);
}

template <> void Send(GenericReplyBuilder& rb, SimpleString& value) {
  rb.SendSimpleString(value.str);
}

template <> void Send(GenericReplyBuilder& rb, BulkString& value) {
  rb.SendBulkString(value.str);
}

template <> void Send(GenericReplyBuilder& rb, VerbatimString& value) {
  rb.SendVerbatimString(value.str, value.format);
}

template <> void Send(GenericReplyBuilder& rb, ScoredArray& value) {
  rb.SendScoredArray(value.arr, value.with_scores);
}

template <typename T> void Send(GenericReplyBuilder& rb, dfly::JsonCallbackResult<T>& value) {
  if (value.IsV1()) {
    Send(rb, AsV1(value));
  } else {
    Send(rb, AsV2(value));
  }
}

template <typename T> void Send(GenericReplyBuilder& rb, dfly::JsonV1CallbackResult<T>& value) {
  Send(rb, value.result);
}

template <typename T> void Send(GenericReplyBuilder& rb, dfly::JsonV2CallbackResult<T>& value) {
  Send(rb, value.result);
}
}  // namespace reply_builder_generic

}  // namespace facade
