// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string>
#include <string_view>
#include <utility>
#include <vector>

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

class GenericReplyBuilder final {
 public:
  explicit GenericReplyBuilder(RedisReplyBuilder* reply_builder);

  void SendNullArray();
  void SendEmptyArray();
  void SendSimpleStrArr(RedisReplyBuilder::StrSpan arr);
  void SendStringArr(RedisReplyBuilder::StrSpan arr, RedisReplyBuilder::CollectionType type);

  void SendNull();
  void SendLong(long val);
  void SendDouble(double val);
  void SendSimpleString(std::string_view str);

  void SendBulkString(std::string_view str);
  void SendVerbatimString(std::string_view str, RedisReplyBuilder::VerbatimFormat format);
  void SendScoredArray(const std::vector<std::pair<std::string, double>>& arr, bool with_scores);

  void StartCollection(unsigned len, RedisReplyBuilder::CollectionType type);

  template <typename T> void Send(T& value);

 private:
  RedisReplyBuilder* reply_builder_;
};

GenericReplyBuilder AsGenericReplyBuilder(SinkReplyBuilder* reply_builder);

/* namespace details {

template <typename T> void Send(GenericReplyBuilder& rb, T& value) = delete;

template <typename T> void Send(GenericReplyBuilder& rb, std::optional<T>& opt);

template <typename T> void Send(GenericReplyBuilder& rb, std::vector<T>& vec);

template <> void Send(GenericReplyBuilder& rb, long& value);

template <> void Send(GenericReplyBuilder& rb, double& value);

template <> void Send(GenericReplyBuilder& rb, RedisReplyBuilder::StrSpan& value);

template <> void Send(GenericReplyBuilder& rb, StringArr& value);

template <> void Send(GenericReplyBuilder& rb, SimpleString& value);

template <> void Send(GenericReplyBuilder& rb, BulkString& value);

template <> void Send(GenericReplyBuilder& rb, VerbatimString& value);

template <> void Send(GenericReplyBuilder& rb, ScoredArray& value);

template <typename T> void Send(GenericReplyBuilder& rb, dfly::JsonCallbackResult<T>& value);

template <typename T> void Send(GenericReplyBuilder& rb, dfly::JsonV1CallbackResult<T>& value);

template <typename T> void Send(GenericReplyBuilder& rb, dfly::JsonV2CallbackResult<T>& value);

}  // namespace details */

}  // namespace facade
