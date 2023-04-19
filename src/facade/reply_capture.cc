// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "facade/reply_capture.h"

#include "base/logging.h"
#include "reply_capture.h"

#define CHECK_MODE(needed)    \
  if (reply_mode_ < needed) { \
    current_ = monostate{};   \
    return;                   \
  }
namespace facade {

using namespace std;

void CapturingReplyBuilder::SendError(std::string_view str, std::string_view type) {
  CHECK_MODE(ReplyMode::ONLY_ERR);
  Capture(Error{str, type});
}

void CapturingReplyBuilder::SendMGetResponse(absl::Span<const OptResp> arr) {
  CHECK_MODE(ReplyMode::FULL);
  Capture(vector<OptResp>{arr.begin(), arr.end()});
}

void CapturingReplyBuilder::SendError(OpStatus status) {
  CHECK_MODE(ReplyMode::ONLY_ERR);
  Capture(status);
}

void CapturingReplyBuilder::SendNullArray() {
  CHECK_MODE(ReplyMode::FULL);
  Capture(unique_ptr<CollectionPayload>{nullptr});
}

void CapturingReplyBuilder::SendEmptyArray() {
  CHECK_MODE(ReplyMode::FULL);
  Capture(make_unique<CollectionPayload>(0, ARRAY));
}

void CapturingReplyBuilder::SendSimpleStrArr(StrSpan arr) {
  CHECK_MODE(ReplyMode::FULL);
  DCHECK_EQ(current_.index(), 0u);

  WrappedStrSpan warr{arr};
  vector<string> sarr(warr.Size());
  for (unsigned i = 0; i < warr.Size(); i++)
    sarr[i] = warr[i];

  Capture(StrArrPayload{true, ARRAY, move(sarr)});
}

void CapturingReplyBuilder::SendStringArr(StrSpan arr, CollectionType type) {
  CHECK_MODE(ReplyMode::FULL);
  DCHECK_EQ(current_.index(), 0u);

  // TODO: 1. Allocate all strings at once 2. Allow movable types
  WrappedStrSpan warr{arr};
  vector<string> sarr(warr.Size());
  for (unsigned i = 0; i < warr.Size(); i++)
    sarr[i] = warr[i];

  Capture(StrArrPayload{false, type, move(sarr)});
}

void CapturingReplyBuilder::SendNull() {
  CHECK_MODE(ReplyMode::FULL);
  Capture(nullptr_t{});
}

void CapturingReplyBuilder::SendLong(long val) {
  CHECK_MODE(ReplyMode::FULL);
  Capture(val);
}

void CapturingReplyBuilder::SendDouble(double val) {
  CHECK_MODE(ReplyMode::FULL);
  Capture(val);
}

void CapturingReplyBuilder::SendSimpleString(std::string_view str) {
  CHECK_MODE(ReplyMode::FULL);
  Capture(SimpleString{string{str}});
}

void CapturingReplyBuilder::SendBulkString(std::string_view str) {
  CHECK_MODE(ReplyMode::FULL);
  Capture(BulkString{string{str}});
}

void CapturingReplyBuilder::SendScoredArray(const std::vector<std::pair<std::string, double>>& arr,
                                            bool with_scores) {
  CHECK_MODE(ReplyMode::FULL);
  Capture(ScoredArray{arr, with_scores});
}

void CapturingReplyBuilder::StartCollection(unsigned len, CollectionType type) {
  CHECK_MODE(ReplyMode::FULL);
  stack_.emplace(make_unique<CollectionPayload>(len, type), type == MAP ? len * 2 : len);

  // If we added an empty collection, it must be collapsed immediately.
  CollapseFilledCollections();
}

CapturingReplyBuilder::Payload CapturingReplyBuilder::Take() {
  CHECK(stack_.empty());
  Payload pl = move(current_);
  current_ = monostate{};
  return pl;
}

void CapturingReplyBuilder::SendDirect(Payload&& val) {
  bool is_err = holds_alternative<Error>(val) || holds_alternative<OpStatus>(val);
  ReplyMode min_mode = is_err ? ReplyMode::ONLY_ERR : ReplyMode::FULL;
  if (reply_mode_ >= min_mode) {
    DCHECK_EQ(current_.index(), 0u);
    current_ = move(val);
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
    auto pl = move(stack_.top());
    stack_.pop();
    Capture(move(pl.first));
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
      visit(*this, pl);
  }

  void operator()(const vector<RedisReplyBuilder::OptResp>& mget) {
    rb->SendMGetResponse(mget);
  }

  void operator()(const CapturingReplyBuilder::ScoredArray& sarr) {
    rb->SendScoredArray(sarr.arr, sarr.with_scores);
  }

  RedisReplyBuilder* rb;
};

void CapturingReplyBuilder::Apply(Payload&& pl, RedisReplyBuilder* rb) {
  if (auto* crb = dynamic_cast<CapturingReplyBuilder*>(rb); crb != nullptr) {
    crb->SendDirect(move(pl));
    return;
  }

  CaptureVisitor cv{rb};
  visit(cv, pl);
}

void CapturingReplyBuilder::SetReplyMode(ReplyMode mode) {
  reply_mode_ = mode;
  current_ = monostate{};
}

optional<CapturingReplyBuilder::ErrorRef> CapturingReplyBuilder::GetError(const Payload& pl) {
  if (auto* err = get_if<Error>(&pl); err != nullptr) {
    return ErrorRef{err->first, err->second};
  }
  return nullopt;
}

}  // namespace facade
