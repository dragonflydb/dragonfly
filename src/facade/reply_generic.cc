// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "facade/reply_generic.h"

namespace facade {

GenericReplyBuilder::GenericReplyBuilder(RedisReplyBuilder* reply_builder)
    : reply_builder_(reply_builder) {
}

void GenericReplyBuilder::SendNullArray() {
  reply_builder_->SendNullArray();
}

void GenericReplyBuilder::SendEmptyArray() {
  reply_builder_->SendEmptyArray();
}

void GenericReplyBuilder::SendSimpleStrArr(RedisReplyBuilder::StrSpan arr) {
  reply_builder_->SendSimpleStrArr(std::move(arr));
}

void GenericReplyBuilder::SendStringArr(RedisReplyBuilder::StrSpan arr,
                                        RedisReplyBuilder::CollectionType type) {
  reply_builder_->SendStringArr(std::move(arr), type);
}

void GenericReplyBuilder::SendNull() {
  reply_builder_->SendNull();
}

void GenericReplyBuilder::SendLong(long val) {
  reply_builder_->SendLong(val);
}

void GenericReplyBuilder::SendDouble(double val) {
  reply_builder_->SendDouble(val);
}

void GenericReplyBuilder::SendSimpleString(std::string_view str) {
  reply_builder_->SendSimpleString(str);
}

void GenericReplyBuilder::SendBulkString(std::string_view str) {
  reply_builder_->SendBulkString(str);
}

void GenericReplyBuilder::SendVerbatimString(std::string_view str,
                                             RedisReplyBuilder::VerbatimFormat format) {
  reply_builder_->SendVerbatimString(str, format);
}

void GenericReplyBuilder::SendScoredArray(const std::vector<std::pair<std::string, double>>& arr,
                                          bool with_scores) {
  reply_builder_->SendScoredArray(arr, with_scores);
}

void GenericReplyBuilder::StartCollection(unsigned len, RedisReplyBuilder::CollectionType type) {
  reply_builder_->StartCollection(len, type);
}

template <typename T> void GenericReplyBuilder::Send(T& value) {
  // details::Send<T>(*this, value);
}

GenericReplyBuilder AsGenericReplyBuilder(SinkReplyBuilder* reply_builder) {
  return GenericReplyBuilder{static_cast<RedisReplyBuilder*>(reply_builder)};
}

/* namespace details {

template <typename T> void Send(GenericReplyBuilder& rb, std::optional<T>& opt) {
  if (opt.has_value()) {
    rb.Send<T>(opt.value());
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
      rb.Send<T>(x);
    }
  }
}

template <> void Send(GenericReplyBuilder& rb, long& value) {
  rb.SendLong(value);
}

template <> void Send(GenericReplyBuilder& rb, double& value) {
  rb.SendDouble(value);
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
    rb.Send<T>(AsV1(value));
  } else {
    rb.Send<T>(AsV2(value));
  }
}

template <typename T> void Send(GenericReplyBuilder& rb, dfly::JsonV1CallbackResult<T>& value) {
  rb.Send<std::optional<T>>(value.result);
}

template <typename T> void Send(GenericReplyBuilder& rb, dfly::JsonV2CallbackResult<T>& value) {
  rb.Send<std::vector<T>>(value.result);
}
}  // namespace details */

}  // namespace facade
