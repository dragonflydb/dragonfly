// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/executor.h"

#include "base/logging.h"
#include "server/main_service.h"

namespace dfly {

JournalExecutor::JournalExecutor(Service* service)
    : service_{service}, conn_context_{&null_sink_, nullptr} {
  conn_context_.is_replicating = true;
  conn_context_.journal_emulated = true;
}

void JournalExecutor::Execute(std::vector<journal::ParsedEntry>& entries) {
  DCHECK_GT(entries.size(), 1);
  conn_context_.conn_state.db_index = entries.front().dbid;

  std::string multi_cmd = {"MULTI"};
  auto ms = MutableSlice{&multi_cmd[0], multi_cmd.size()};
  auto span = CmdArgList{&ms, 1};
  service_->DispatchCommand(span, &conn_context_);

  for (auto& entry : entries) {
    if (entry.payload) {
      DCHECK_EQ(entry.dbid, conn_context_.conn_state.db_index);
      span = CmdArgList{entry.payload->data(), entry.payload->size()};

      service_->DispatchCommand(span, &conn_context_);
    }
  }

  std::string exec_cmd = {"EXEC"};
  ms = {&exec_cmd[0], exec_cmd.size()};
  span = {&ms, 1};
  service_->DispatchCommand(span, &conn_context_);
}

void JournalExecutor::Execute(journal::ParsedEntry& entry) {
  conn_context_.conn_state.db_index = entry.dbid;
  if (entry.payload) {  // TODO - when this is false?
    auto span = CmdArgList{entry.payload->data(), entry.payload->size()};

    service_->DispatchCommand(span, &conn_context_);
  }
}

}  // namespace dfly
