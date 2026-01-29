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
    MainLoopFb(table.get(), db_cntx);

    fiber_.Detach();  // Detach self to be safely deletable
    on_complete();
  };

  fiber_ = Fiber{std::move(cb)};
}

void IndexBuilder::MainLoopFb(dfly::DbTable* table, DbContext db_cntx) {
  const auto doc_index = index_->GetInfo().base_index;

  auto cb = [this, doc_index, db_cntx, scratch = std::string{}](PrimeTable::iterator it) mutable {
    PrimeValue& pv = it->second;
    std::string_view key = it->first.GetSlice(&scratch);

    if (doc_index.Matches(key, pv.ObjType()))
      index_->AddDoc(key, db_cntx, pv);
  };

  PrimeTable::Cursor cursor;
  do {
    cursor = table->prime.Traverse(cursor, cb);
    if (base::CycleClock::ToUsec(util::ThisFiber::GetRunningTimeCycles()) > 500)
      util::ThisFiber::Yield();
  } while (cursor);
}
}  // namespace dfly::search
