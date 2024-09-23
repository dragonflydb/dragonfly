// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "facade/reply_capture.h"

#include "absl/types/span.h"
#include "base/logging.h"
#include "reply_capture.h"

#define SKIP_LESS(needed)     \
  has_replied_ = true;        \
  if (reply_mode_ < needed) { \
    current_ = monostate{};   \
    return;                   \
  }
namespace facade {

using namespace std;

void CapturingReplyBuilder::SendError(std::string_view str, std::string_view type) {
  last_error_ = str;
  SKIP_LESS(ReplyMode::ONLY_ERR);
  Capture(Error{str, type});
}

void CapturingReplyBuilder::SendMGetResponse(MGetResponse resp) {
  SKIP_LESS(ReplyMode::FULL);
  Capture(std::move(resp));
}

void CapturingReplyBuilder::SendError(OpStatus status) {
  if (status != OpStatus::OK) {
    last_error_ = StatusToMsg(status);
  }
  SKIP_LESS(ReplyMode::ONLY_ERR);
  Capture(status);
}

void CapturingReplyBuilder::SendNullArray() {
  SKIP_LESS(ReplyMode::FULL);
  Capture(unique_ptr<CollectionPayload>{nullptr});
}

void CapturingReplyBuilder::SendEmptyArray() {
  SKIP_LESS(ReplyMode::FULL);
  Capture(make_unique<CollectionPayload>(0, ARRAY));
}

void CapturingReplyBuilder::SendSimpleStrArr(StrSpan arr) {
  SKIP_LESS(ReplyMode::FULL);
  DCHECK_EQ(current_.index(), 0u);

  Capture(StrArrPayload{true, ARRAY, {arr.begin(), arr.end()}});
}

void CapturingReplyBuilder::SendStringArr(StrSpan arr, CollectionType type) {
  SKIP_LESS(ReplyMode::FULL);
  DCHECK_EQ(current_.index(), 0u);

  // TODO: 1. Allocate all strings at once 2. Allow movable types
  Capture(StrArrPayload{false, type, {arr.begin(), arr.end()}});
}

void CapturingReplyBuilder::SendNull() {
  SKIP_LESS(ReplyMode::FULL);
  Capture(nullptr_t{});
}

void CapturingReplyBuilder::SendLong(long val) {
  SKIP_LESS(ReplyMode::FULL);
  Capture(val);
}

void CapturingReplyBuilder::SendDouble(double val) {
  SKIP_LESS(ReplyMode::FULL);
  Capture(val);
}

void CapturingReplyBuilder::SendSimpleString(std::string_view str) {
  SKIP_LESS(ReplyMode::FULL);
  Capture(SimpleString{string{str}});
}

void CapturingReplyBuilder::SendBulkString(std::string_view str) {
  SKIP_LESS(ReplyMode::FULL);
  Capture(BulkString{string{str}});
}

void CapturingReplyBuilder::SendScoredArray(absl::Span<const std::pair<std::string, double>> arr,
                                            bool with_scores) {
  SKIP_LESS(ReplyMode::FULL);
  std::vector<std::pair<std::string, double>> values(arr.begin(), arr.end());
  Capture(ScoredArray{std::move(values), with_scores});
}

void CapturingReplyBuilder::StartCollection(unsigned len, CollectionType type) {
  SKIP_LESS(ReplyMode::FULL);
  stack_.emplace(make_unique<CollectionPayload>(len, type), type == MAP ? len * 2 : len);

  // If we added an empty collection, it must be collapsed immediately.
  CollapseFilledCollections();
}

CapturingReplyBuilder::Payload CapturingReplyBuilder::Take() {
  CHECK(stack_.empty());
  Payload pl = std::move(current_);
  current_ = monostate{};
  return pl;
}

void CapturingReplyBuilder::SendDirect(Payload&& val) {
  has_replied_ = !holds_alternative<monostate>(val);
  bool is_err = holds_alternative<Error>(val) || holds_alternative<OpStatus>(val);
  ReplyMode min_mode = is_err ? ReplyMode::ONLY_ERR : ReplyMode::FULL;
  if (reply_mode_ >= min_mode) {
    DCHECK_EQ(current_.index(), 0u);
    current_ = std::move(val);
  } else {
    current_ = monostate{};
  }
}

void CapturingReplyBuilder::Capture(Payload val) {
  if (!stack_.empty()) {
    stack_.top().first->arr.push_back(std::move(val));
    stack_.top().second--;
  } else {
    DCHECK_EQ(current_.index(), 0u);
    current_ = std::move(val);
  }

  // Check if we filled up a collection.
  CollapseFilledCollections();
}

void CapturingReplyBuilder::CollapseFilledCollections() {
  while (!stack_.empty() && stack_.top().second == 0) {
    auto pl = std::move(stack_.top());
    stack_.pop();
    Capture(std::move(pl.first));
  }
}

CapturingReplyBuilder::CollectionPayload::CollectionPayload(unsigned len, CollectionType type)
    : len{len}, type{type}, arr{} {
  arr.reserve(type == MAP ? len * 2 : len);
}

struct CaptureVisitor {
  void operator()(monostate) {
  }

  void operator()(long v) {
    rb->SendLong(v);
  }

  void operator()(double v) {
    rb->SendDouble(v);
  }

  void operator()(const CapturingReplyBuilder::SimpleString& ss) {
    rb->SendSimpleString(ss);
  }

  void operator()(const CapturingReplyBuilder::BulkString& bs) {
    rb->SendBulkString(bs);
  }

  void operator()(CapturingReplyBuilder::Null) {
    rb->SendNull();
  }

  void operator()(CapturingReplyBuilder::Error err) {
    rb->SendError(err.first, err.second);
  }

  void operator()(OpStatus status) {
    rb->SendError(status);
  }

  void operator()(const CapturingReplyBuilder::StrArrPayload& sa) {
    if (sa.simple)
      rb->SendSimpleStrArr(sa.arr);
    else
      rb->SendStringArr(sa.arr, sa.type);
  }

  void operator()(const unique_ptr<CapturingReplyBuilder::CollectionPayload>& cp) {
    if (!cp) {
      rb->SendNullArray();
      return;
    }
    if (cp->len == 0 && cp->type == RedisReplyBuilder::ARRAY) {
      rb->SendEmptyArray();
      return;
    }
    rb->StartCollection(cp->len, cp->type);
    for (auto& pl : cp->arr)
      visit(*this, std::move(pl));
  }

  void operator()(SinkReplyBuilder::MGetResponse resp) {
    rb->SendMGetResponse(std::move(resp));
  }

  void operator()(const CapturingReplyBuilder::ScoredArray& sarr) {
    rb->SendScoredArray(sarr.arr, sarr.with_scores);
  }

  RedisReplyBuilder* rb;
};

void CapturingReplyBuilder::Apply(Payload&& pl, RedisReplyBuilder* rb) {
  if (auto* crb = dynamic_cast<CapturingReplyBuilder*>(rb); crb != nullptr) {
    crb->SendDirect(std::move(pl));
    return;
  }

  CaptureVisitor cv{rb};
  visit(cv, std::move(pl));
  // Consumed and printed by InvokeCmd. We just send the actual error here
  std::ignore = rb->ConsumeLastError();
}

void CapturingReplyBuilder::SetReplyMode(ReplyMode mode) {
  reply_mode_ = mode;
  current_ = monostate{};
}

optional<CapturingReplyBuilder::ErrorRef> CapturingReplyBuilder::TryExtractError(
    const Payload& pl) {
  if (auto* err = get_if<Error>(&pl); err != nullptr) {
    return ErrorRef{err->first, err->second};
  }
  return nullopt;
}

}  // namespace facade
