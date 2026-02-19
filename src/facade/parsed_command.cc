// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/parsed_command.h"

#include "base/logging.h"
#include "core/overloaded.h"
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

std::string MCRender::RenderStored(bool ok) const {
  if (flags_.no_reply)
    return {};
  if (ok)
    return flags_.meta ? "HD" : "STORED";
  return flags_.meta ? "NS" : "NOT_STORED";
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
  is_deferred_reply_ = false;
  reply_ = std::monostate{};
  is_blocking_cmd = false;

  offsets_.clear();
  if (HeapMemory() > 1024) {
    storage_.clear();  // also deallocates the heap.
    offsets_.shrink_to_fit();
  }
  ReuseInternal();
}

void ParsedCommand::SendError(std::string_view str, std::string_view type) {
  if (!is_deferred_reply_) {
    rb_->SendError(str, type);
  } else {
    reply_ = payload::make_error(str, type);
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
      reply_ = payload::SimpleString{"OK"};
    else
      reply_ = payload::make_error(StatusToMsg(status));
  }
}

void ParsedCommand::SendError(const facade::ErrorReply& error) {
  if (error.status)
    return SendError(*error.status);
  SendError(error.ToSv(), error.kind);
}

void ParsedCommand::SendSimpleString(std::string_view str) {
  if (!is_deferred_reply_) {
    rb_->SendSimpleString(str);
  } else {
    reply_ = payload::make_simple_or_noreply(str);
  }
}

void ParsedCommand::SendLong(long val) {
  if (is_deferred_reply_) {
    reply_ = long(val);
  } else {
    rb_->SendLong(val);
  }
}

void ParsedCommand::SendNull() {
  if (is_deferred_reply_) {
    reply_ = payload::Null{};
  } else {
    DCHECK(mc_cmd_ == nullptr);  // RESP only
    static_cast<RedisReplyBuilder*>(rb_)->SendNull();
  }
}

void ParsedCommand::SendEmptyArray() {
  if (is_deferred_reply_) {
    reply_ = make_unique<payload::CollectionPayload>(0, CollectionType::ARRAY);
  } else {
    DCHECK(mc_cmd_ == nullptr);  // RESP only
    static_cast<RedisReplyBuilder*>(rb_)->SendEmptyArray();
  }
}

bool ParsedCommand::CanReply() const {
  DCHECK(is_deferred_reply_);
  dfly::Overloaded ov{[](const payload::Payload& pl) { return pl.index() > 0 /* not monostate */; },
                      [](const AsyncTask& task) { return task.blocker->IsCompleted(); }};
  return std::visit(ov, reply_);
}

void ParsedCommand::SendReply() {
  dfly::Overloaded ov{
      [this](payload::Payload& pl) { CapturingReplyBuilder::Apply(std::move(pl), rb_); },
      [this](AsyncTask& task) { return task.replier(rb_); }};
  return std::visit(ov, reply_);
}

}  // namespace facade
