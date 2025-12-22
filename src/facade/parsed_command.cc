// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/parsed_command.h"

#include "base/logging.h"
#include "facade/conn_context.h"
#include "facade/dragonfly_connection.h"
#include "facade/reply_builder.h"
#include "facade/reply_capture.h"

namespace facade {

using namespace std;

string MCRender::RenderNotFound() const {
  if (flags_.no_reply)
    return {};
  return flags_.meta ? "NF" : "NOT_FOUND";
}

string MCRender::RenderGetEnd() const {
  if (flags_.no_reply || flags_.meta)
    return {};
  return "END";
}

string MCRender::RenderMiss() const {
  if (flags_.no_reply || !flags_.meta)
    return {};
  return "EN";
}

string MCRender::RenderDeleted() const {
  if (flags_.no_reply)
    return {};
  return flags_.meta ? "HD" : "DELETED";
}

void ParsedCommand::ResetForReuse() {
  allow_async_execution_ = false;
  is_deferred_reply_ = false;
  reply_payload_ = std::monostate{};

  state_.store(0, std::memory_order_relaxed);
  offsets_.clear();
  if (HeapMemory() > 1024) {
    storage_.clear();  // also deallocates the heap.
    offsets_.shrink_to_fit();
  }
}

void ParsedCommand::SendError(std::string_view str, std::string_view type) {
  if (!is_deferred_reply_) {
    rb_->SendError(str, type);
  } else {
    reply_payload_ = payload::make_error(str, type);
    NotifyReplied();
  }
}

void ParsedCommand::SendError(facade::OpStatus status) {
  if (!is_deferred_reply_) {
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
  if (!is_deferred_reply_) {
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
  if (!is_deferred_reply_) {
    if (!str.empty())  // empty string means no-reply
      rb_->SendSimpleString(str);
  } else {
    reply_payload_ = payload::make_simple_or_noreply(str);
    NotifyReplied();
  }
}

bool ParsedCommand::SendPayload() {
  if (is_deferred_reply_) {
    CapturingReplyBuilder::Apply(std::move(reply_payload_), rb_);
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

  DVLOG(1) << "ParsedCommand::NotifyReplied with state " << unsigned(prev_state);

  if (prev_state & DELETE_INTENT) {
    delete this;
    return;
  }
  // If it was marked as head already, notify the connection that the head is done.
  if (prev_state & HEAD_REPLY) {
    // TODO: this might crash as we currently do not wait for async commands on connection close.
    DCHECK(conn_cntx_);
    conn_cntx_->conn()->Notify();
  }
}

}  // namespace facade
