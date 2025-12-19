// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/parsed_command.h"

#include "base/logging.h"
#include "facade/conn_context.h"
#include "facade/dragonfly_connection.h"
#include "facade/reply_builder.h"

namespace facade {

using namespace std;

namespace {

struct PayloadVisitor {
  void operator()(monostate) {
  }

  void operator()(long v) {
    rb->SendLong(v);
  }

  void operator()(double v) {
    LOG(FATAL) << "Double replies are not supported in MC protocol";
  }

  void operator()(const payload::SimpleString& ss) {
    rb->SendSimpleString(ss);
  }

  void operator()(const payload::BulkString& bs) {
    LOG(FATAL) << "TBD";
  }

  void operator()(payload::Null) {
    LOG(FATAL) << "TBD";
  }

  void operator()(const payload::Error& err) {
    rb->SendError(err->first, err->second);
  }

  void operator()(payload::StoredReply sr) {
    if (sr.ok) {
      rb->SendStored();
    } else {
      rb->SendSetSkipped();
    }
  }

  void operator()(const unique_ptr<payload::CollectionPayload>& cp) {
    LOG(FATAL) << "TBD";
  }

  SinkReplyBuilder* rb;
};

}  // namespace

void ParsedCommand::ResetForReuse() {
  reply_direct_ = true;
  reply_payload_ = std::monostate{};
  dispatch_async_ = false;
  state_.store(0, std::memory_order_relaxed);
  offsets_.clear();
  if (HeapMemory() > 1024) {
    storage_.clear();  // also deallocates the heap.
    offsets_.shrink_to_fit();
  }
}

void ParsedCommand::SendError(std::string_view str, std::string_view type) {
  if (reply_direct_) {
    rb_->SendError(str, type);
  } else {
    reply_payload_ = payload::make_error(str, type);
    NotifyReplied();
  }
}

void ParsedCommand::SendError(facade::OpStatus status) {
  if (reply_direct_) {
    if (status == OpStatus::OK)
      rb_->SendSimpleString("OK");
    else
      rb_->SendError(StatusToMsg(status));
  } else {
    if (status == OpStatus::OK)
      reply_payload_ = payload::SimpleString{"OK"};
    else
      reply_payload_ = payload::make_error(StatusToMsg(status));
    NotifyReplied();
  }
}

void ParsedCommand::SendError(const facade::ErrorReply& error) {
  if (error.status)
    return SendError(*error.status);
  SendError(error.ToSv(), error.kind);
}

void ParsedCommand::SendStored(bool ok) {
  if (reply_direct_) {
    if (ok)
      rb_->SendStored();
    else
      rb_->SendSetSkipped();
  } else {
    reply_payload_ = payload::StoredReply{ok};
    NotifyReplied();
  }
}

void ParsedCommand::SendSimpleString(std::string_view str) {
  if (reply_direct_) {
    rb_->SendSimpleString(str);
  } else {
    reply_payload_ = payload::SimpleString{std::string(str)};
    NotifyReplied();
  }
}

bool ParsedCommand::SendPayload() {
  if (IsReplyCached()) {
    PayloadVisitor pv{rb_};
    std::visit(pv, reply_payload_);

    reply_payload_ = {};
    return true;
  }
  return false;
}

bool ParsedCommand::CheckDoneAndMarkHead() {
  uint8_t state = state_.load(std::memory_order_acquire);

  while ((state & ASYNC_REPLY_DONE) == 0) {
    // If we marked it as head already, return false.
    if (state & HEAD_REPLY) {
      return false;
    }

    // Mark it as head. If succeeded (i.e ASYNC_REPLY_DONE is still not set), return false
    if (state_.compare_exchange_weak(state, state | HEAD_REPLY, std::memory_order_acq_rel)) {
      return false;
    }
    // Otherwise, retry with updated state.
  }

  // ASYNC_REPLY_DONE is set, return true.
  return true;
}

void ParsedCommand::NotifyReplied() {
  // A synchronization point. We set ASYNC_REPLY_DONE to mark it's safe now to read the payload.
  uint8_t prev_state = state_.fetch_or(ASYNC_REPLY_DONE, std::memory_order_acq_rel);

  // If it was marked as head already, notify the connection that the head is done.
  if (prev_state & HEAD_REPLY) {
    // TODO: this might crash as we currently do not wait for async commands on connection close.
    DCHECK(conn_cntx_);
    conn_cntx_->conn()->Notify();
  }
}

}  // namespace facade
