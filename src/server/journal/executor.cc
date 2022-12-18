// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/executor.h"

#include "base/logging.h"
#include "server/main_service.h"

namespace dfly {

JournalExecutor::JournalExecutor(Service* service) : service_{service} {
}

void JournalExecutor::Execute(journal::ParsedEntry&& entry) {
  if (entry.payload) {
    io::NullSink null_sink;
    ConnectionContext conn_context{&null_sink, nullptr};
    conn_context.is_replicating = true;
    conn_context.journal_emulated = true;
    conn_context.conn_state.db_index = entry.dbid;

    auto span = CmdArgList{entry.payload->data(), entry.payload->size()};

    service_->DispatchCommand(span, &conn_context);
  }
}

}  // namespace dfly
