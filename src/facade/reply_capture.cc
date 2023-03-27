#include "facade/reply_capture.h"

#include "base/logging.h"
#include "reply_capture.h"

namespace facade {

using namespace std;

void CapturingReplyBuilder::SendError(std::string_view str, std::string_view type) {
  Capture(Error{str, type});
}

void CapturingReplyBuilder::SendMGetResponse(const OptResp* resp, uint32_t count) {
  // TODO
}

void CapturingReplyBuilder::SendStored() {
  // TODO
}

void CapturingReplyBuilder::SendSetSkipped() {
  // TODO
}

void CapturingReplyBuilder::SendError(OpStatus status) {
  Capture(status);
}

void CapturingReplyBuilder::SendNullArray() {
  Capture(unique_ptr<CollectionPayload>{nullptr});
}

void CapturingReplyBuilder::SendEmptyArray() {
  Capture(make_unique<CollectionPayload>(CollectionPayload{0, ARRAY, vector<Payload>{}}));
}

void CapturingReplyBuilder::SendSimpleStrArr(StrSpan arr) {
  DCHECK_EQ(current_.index(), 0u);

  WrappedStrSpan warr{arr};
  vector<string> sarr(warr.Size());
  for (unsigned i = 0; i < warr.Size(); i++)
    sarr[i] = warr[i];

  Capture(StrArrPayload{true, ARRAY, move(sarr)});
}

void CapturingReplyBuilder::SendStringArr(StrSpan arr, CollectionType type) {
  DCHECK_EQ(current_.index(), 0u);

  // TODO: 1. Allocate all strings at once 2. Allow movable types
  WrappedStrSpan warr{arr};
  vector<string> sarr(warr.Size());
  for (unsigned i = 0; i < warr.Size(); i++)
    sarr[i] = warr[i];

  Capture(StrArrPayload{false, type, move(sarr)});
}

void CapturingReplyBuilder::SendNull() {
  Capture(nullptr_t{});
}

void CapturingReplyBuilder::SendLong(long val) {
  Capture(val);
}

void CapturingReplyBuilder::SendDouble(double val) {
  Capture(val);
}

void CapturingReplyBuilder::SendSimpleString(std::string_view str) {
  Capture(SimpleString{string{str}});
}

void CapturingReplyBuilder::SendBulkString(std::string_view str) {
  Capture(BulkString{string{str}});
}

void CapturingReplyBuilder::SendScoredArray(const std::vector<std::pair<std::string, double>>& arr,
                                            bool with_scores) {
  CHECK(false) << "Not implemented";
}

void CapturingReplyBuilder::StartCollection(unsigned len, CollectionType type) {
  Capture(make_unique<CollectionPayload>(CollectionPayload{len, type, vector<Payload>{}}));
}

CapturingReplyBuilder::Payload CapturingReplyBuilder::Get() {
  CHECK(stack_.empty());
  return move(current_);
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

  RedisReplyBuilder* rb;
};

void CapturingReplyBuilder::Apply(Payload&& pl, RedisReplyBuilder* rb) {
  CaptureVisitor cv{rb};
  visit(cv, pl);
}

}  // namespace facade
