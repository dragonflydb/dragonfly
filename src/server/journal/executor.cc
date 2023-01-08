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

void JournalExecutor::Execute(DbIndex dbid, std::vector<journal::ParsedEntry::CmdData>& cmds) {
  conn_context_.conn_state.db_index = dbid;
  for (auto& cmd : cmds) {
    Execute(cmd);
  }
}

void JournalExecutor::Execute(DbIndex dbid, journal::ParsedEntry::CmdData& cmd) {
  conn_context_.conn_state.db_index = dbid;
  Execute(cmd);
}

void JournalExecutor::Execute(journal::ParsedEntry::CmdData& cmd) {
  auto span = CmdArgList{cmd.cmd_args.data(), cmd.cmd_args.size()};
  service_->DispatchCommand(span, &conn_context_);
}

}  // namespace dfly
