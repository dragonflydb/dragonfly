// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/interpreter_utils.h"

#include "base/logging.h"
#include "server/conn_context.h"
#include "server/server_state.h"
#include "server/transaction.h"

namespace dfly {

BorrowedInterpreter::BorrowedInterpreter(Transaction* tx, ConnectionState* state) {
  // Ensure squashing ignores EVAL. We can't run on a stub context, because it doesn't have our
  // preborrowed interpreter (which can't be shared on multiple threads).
  CHECK(!tx->IsSquashedStub());

  if (auto borrowed = state->exec_info.preborrowed_interpreter; borrowed) {
    // Ensure a preborrowed interpreter is only set for an already running MULTI transaction.
    CHECK_EQ(state->exec_info.state, ConnectionState::ExecInfo::EXEC_RUNNING);

    interpreter_ = borrowed;
  } else {
    // A scheduled transaction occupies a place in the transaction queue and holds locks,
    // preventing other transactions from progressing. Blocking below can deadlock!
    CHECK(!tx->IsScheduled());

    interpreter_ = ServerState::tlocal()->BorrowInterpreter();
    owned_ = true;
  }
}

BorrowedInterpreter::~BorrowedInterpreter() {
  if (owned_)
    ServerState::tlocal()->ReturnInterpreter(interpreter_);
}

}  // namespace dfly
