// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <stack>
#include <string_view>
#include <variant>
#include <vector>

#include "facade/reply_builder.h"

namespace facade {

struct CaptureVisitor;

// CapturingReplyBuilder allows capturing replies and retrieveing them with Take().
// Those replies can be stored standalone and sent with
// CapturingReplyBuilder::Apply() to another reply builder.
class CapturingReplyBuilder : public RedisReplyBuilder {
  friend struct CaptureVisitor;

 public:
  using RedisReplyBuilder::SendError;
  void SendError(std::string_view str, std::string_view type) override;

  void SendLong(long val) override;
  void SendDouble(double val) override;
  void SendSimpleString(std::string_view str) override;
  void SendBulkString(std::string_view str) override;

  void StartCollection(unsigned len, CollectionType type) override;
  void SendNullArray() override;
  void SendNull() override;

 public:
  using Error = std::pair<std::string, std::string>;  // SendError (msg, type)
  using Null = std::nullptr_t;                        // SendNull or SendNullArray

  struct CollectionPayload;
  struct SimpleString : public std::string {};  // SendSimpleString
  struct BulkString : public std::string {};    // SendBulkString

  CapturingReplyBuilder(ReplyMode mode = ReplyMode::FULL, RespVersion resp_v = RespVersion::kResp2)
      : RedisReplyBuilder{nullptr}, reply_mode_{mode}, stack_{}, current_{} {
    SetRespVersion(resp_v);
  }

  using Payload = std::variant<std::monostate, Null, Error, long, double, SimpleString, BulkString,
                               std::unique_ptr<CollectionPayload>>;

  // Non owned Error based on SendError arguments (msg, type)
  using ErrorRef = std::pair<std::string_view, std::string_view>;

  void SetReplyMode(ReplyMode mode);

  // Take payload and clear state.
  Payload Take();

  // Send payload to builder.
  static void Apply(Payload&& pl, RedisReplyBuilder* builder);

  // If an error is stored inside payload, get a reference to it.
  static std::optional<ErrorRef> TryExtractError(const Payload& pl);

  struct CollectionPayload {
    CollectionPayload(unsigned len, CollectionType type);

    unsigned len;
    CollectionType type;
    std::vector<Payload> arr;
  };

 private:
  // Send payload directly, bypassing external interface. For efficient passing between two
  // captures.
  void SendDirect(Payload&& val);

  // Capture value and store eiter in current topmost collection or as a standalone value.
  void Capture(Payload val);

  // While topmost collection in stack is full, finalize it and add it as a regular value.
  void CollapseFilledCollections();

  ReplyMode reply_mode_;

  // List of nested active collections that are being built.
  std::stack<std::pair<std::unique_ptr<CollectionPayload>, int>> stack_;

  // Root payload.
  Payload current_;
};

}  // namespace facade
