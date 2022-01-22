// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "io/file.h"
#include "server/table.h"
#include "util/fibers/simple_channel.h"

namespace dfly {

class RdbSerializer;

class RdbSnapshot {
 public:
  using StringChannel =
      ::util::fibers_ext::SimpleChannel<std::string, base::mpmc_bounded_queue<std::string>>;

  RdbSnapshot(PrimeTable* prime, ExpireTable* et, StringChannel* dest);
  ~RdbSnapshot();

  void Start();
  void Join();

 private:
  void FiberFunc();
  void FlushSfile();
  void PhysicalCb(MainIterator it);

  ::boost::fibers::fiber fb_;

  std::unique_ptr<io::StringFile> sfile_;
  std::unique_ptr<RdbSerializer> rdb_serializer_;

  PrimeTable* prime_table_;
  StringChannel* dest_;

  uint64_t processed_ = 0;
};

}  // namespace dfly
