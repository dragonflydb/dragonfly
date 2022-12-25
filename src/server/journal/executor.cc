// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/executor.h"

#include "base/logging.h"
#include "server/main_service.h"

namespace dfly {

JournalExecutor::JournalExecutor(Service* service) : service_{service} {
}

void JournalExecutor::Execute(journal::ParsedEntry& entry) {
  if (entry.payload) {
    io::NullSink null_sink;
    ConnectionContext conn_context{&null_sink, nullptr};
    conn_context.is_replicating = true;
    conn_context.journal_emulated = true;
    conn_context.conn_state.db_index = entry.dbid;

    auto& [cmd_buffer, cmd_arg_vec] = *entry.payload;
    auto span = CmdArgList{cmd_arg_vec.data(), cmd_arg_vec.size()};

    service_->DispatchCommand(span, &conn_context);
  }
}

}  // namespace dfly
