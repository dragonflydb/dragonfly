// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/index_builder.h"

#include "server/db_slice.h"
#include "server/search/doc_accessors.h"

namespace dfly::search {

void IndexBuilder::Start(const OpArgs& op_args, std::function<void()> on_complete) {
  using namespace util::fb2;
  auto table = op_args.GetDbSlice().CopyDBTablePtr(op_args.db_cntx.db_index);
  DCHECK(table.get());

  auto cb = [this, table, db_cntx = op_args.db_cntx, on_complete = std::move(on_complete)] {
    CursorLoop(table.get(), db_cntx);

    // TODO: make it step by step + wire cancellation inside
    if (state_.IsRunning())
      index_->indices_->FinalizeInitialization();

    // Finish by clearing the fiber reference and calling on_complete as its last action
    {
      util::FiberAtomicGuard guard{};  // preserve cancellation
      fiber_.Detach();                 // builder is now safely deleteable
      if (!state_.IsCancelled())
        on_complete();
    }
  };

  fiber_ = Fiber{std::move(cb)};
}

void IndexBuilder::Cancel() {
  state_.Cancel();
  util::fb2::Fiber{std::move(fiber_)}.JoinIfNeeded();  // steal and wait for finish
}

util::fb2::Fiber IndexBuilder::Worker() {
  return std::move(fiber_);
}

void IndexBuilder::CursorLoop(dfly::DbTable* table, DbContext db_cntx) {
  auto cb = [this, db_cntx, scratch = std::string{}](PrimeTable::iterator it) mutable {
    PrimeValue& pv = it->second;
    std::string_view key = it->first.GetSlice(&scratch);

    if (index_->Matches(key, pv.ObjType()))
      index_->AddDoc(key, db_cntx, pv);
  };

  PrimeTable::Cursor cursor;
  do {
    cursor = table->prime.Traverse(cursor, cb);
    if (base::CycleClock::ToUsec(util::ThisFiber::GetRunningTimeCycles()) > 500)
      util::ThisFiber::Yield();
  } while (cursor && state_.IsRunning());
}
}  // namespace dfly::search
