// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/detail/table.h"
#include "core/expire_period.h"

namespace dfly {

using PrimeKey = detail::PrimeKey;
using PrimeValue = detail::PrimeValue;

using PrimeTable = DashTable<PrimeKey, PrimeValue, detail::PrimeTablePolicy>;
using ExpireTable = DashTable<PrimeKey, ExpirePeriod, detail::ExpireTablePolicy>;

/// Iterators are invalidated when new keys are added to the table or some entries are deleted.
/// Iterators are still valid  if a different entry in the table was mutated.
using PrimeIterator = PrimeTable::iterator;
using ExpireIterator = ExpireTable::iterator;

inline bool IsValid(PrimeIterator it) {
  return !it.is_done();
}

inline bool IsValid(ExpireIterator it) {
  return !it.is_done();
}

}  // namespace dfly
