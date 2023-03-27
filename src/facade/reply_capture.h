// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <stack>
#include <variant>

#include "base/logging.h"
#include "facade/reply_builder.h"

namespace facade {

struct CaptureVisitor;

class CapturingReplyBuilder : public RedisReplyBuilder {
  friend struct CaptureVisitor;

 public:
  void SendError(std::string_view str, std::string_view type = {}) override;
  void SendMGetResponse(const OptResp* resp, uint32_t count) override;

  void SendStored() override;
  void SendSetSkipped() override;
  void SendError(OpStatus status) override;

  void SendNullArray() override;
  void SendEmptyArray() override;
  void SendSimpleStrArr(StrSpan arr) override;
  void SendStringArr(StrSpan arr, CollectionType type = ARRAY) override;

  void SendNull() override;
  void SendLong(long val) override;
  void SendDouble(double val) override;
  void SendSimpleString(std::string_view str) override;

  void SendBulkString(std::string_view str) override;
  void SendScoredArray(const std::vector<std::pair<std::string, double>>& arr,
                       bool with_scores) override;

  void StartCollection(unsigned len, CollectionType type) override;

 private:
  using Error = std::pair<std::string, std::string>;  // SendError
  using Null = std::nullptr_t;                        // SendNull or SendNullArray
  struct SimpleString : public std::string {};        // SendSimpleString
  struct BulkString : public std::string {};          // SendBulkString

  struct StrArrPayload {
    bool simple;
    CollectionType type;
    std::vector<std::string> arr;
  };

  struct CollectionPayload;

 public:
  using Payload = std::variant<std::monostate, Null, Error, OpStatus, long, double, SimpleString,
                               BulkString, StrArrPayload, std::unique_ptr<CollectionPayload>>;

  Payload Get();

  static void Apply(Payload&& pl, RedisReplyBuilder* builder);

 private:
  struct CollectionPayload {
    unsigned len;
    CollectionType type;
    std::vector<Payload> arr;
  };

 private:
  template <typename T> void Capture(T&& val, bool skip_collection = false) {
    // Try adding collection to stack if not skipping it.
    bool added = false;
    if constexpr (std::is_same_v<std::remove_reference_t<T>, std::unique_ptr<CollectionPayload>>) {
      if (!skip_collection) {
        int size = val ? (val->type == MAP ? val->len * 2 : val->len) : 0;
        stack_.emplace(std::move(val), size);
        added = true;
      }
    }

    // Add simple element to topmost collection or as standalone.
    if (!added) {
      if (!stack_.empty()) {
        stack_.top().first->arr.push_back(Payload{});
        stack_.top().second--;
      } else {
        DCHECK_EQ(current_.index(), 0u);
        current_ = std::move(val);
      }
    }

    while (!stack_.empty() && stack_.top().second == 0) {
      auto pl = std::move(stack_.top());
      stack_.pop();
      Capture(std::move(pl.first), true);
    }
  }

  std::stack<std::pair<std::unique_ptr<CollectionPayload>, int>> stack_;
  Payload current_;
};

}  // namespace facade
