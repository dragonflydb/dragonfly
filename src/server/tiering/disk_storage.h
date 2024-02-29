// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "core/external_alloc.h"
#include "server/io_mgr.h"
#include "server/tiering/common.h"

namespace dfly::tiering {

class DiskStorage : public Storage {
 public:
  void Open(std::string_view path);
  void Shutdown();

  std::string Read(BlobLocator loc) override;
  BlobLocator Store(std::string_view blob) override;
  void Delete(BlobLocator loc) override;

 private:
  IoMgr io_mgr_;
  ExternalAllocator alloc_;
};

};  // namespace dfly::tiering
