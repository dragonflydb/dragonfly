// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "facade/reply_capture.h"

#include "absl/types/span.h"
#include "base/logging.h"
#include "reply_capture.h"

#define SKIP_LESS(needed)     \
  replies_recorded_++;        \
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

void CapturingReplyBuilder::SendNullArray() {
  SKIP_LESS(ReplyMode::FULL);
  Capture(unique_ptr<CollectionPayload>{nullptr});
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
  ConsumeLastError();
  return pl;
}

void CapturingReplyBuilder::SendDirect(Payload&& val) {
  replies_recorded_ += !holds_alternative<monostate>(val);
  bool is_err = holds_alternative<Error>(val);
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
  rb->ConsumeLastError();
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
