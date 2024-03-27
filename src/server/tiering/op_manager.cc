// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "op_manager.h"

#include "base/logging.h"
#include "core/compact_object.h"
#include "core/overloaded.h"
#include "server/tiered_storage.h"
#include "server/tiering/common.h"

namespace dfly::tiering {

OpManager::~OpManager() {
  storage_.Close();
}

Future<std::string> OpManager::Read(const CompactObj& co) {
  auto [offset, length] = co.GetExternalSlice();
  auto* info = PrepareRead({offset, length});

  tiering::Future<std::string> future;
  info->actions.emplace_back(future);
  return future;
}

void OpManager::Modify(const CompactObj& co, std::function<std::string(std::string)> func) {
  auto [offset, len] = co.GetExternalSlice();
  PrepareRead({offset, len})->actions.emplace_back(std::move(func));
}

void OpManager::Delete(std::string_view key, CompactObj& co) {
  if (co.HasIoPending()) {
    DCHECK(!co.IsExternal());
    DCHECK(pending_stashes_.count(key));

    // Invalidate current stash operation by incrementing version
    ++pending_stashes_[key];
    return;
  }

  // If no IO is pending, it must be offloaded
  DCHECK(co.IsExternal());
  auto [offset, length] = co.GetExternalSlice();

  // If a read is pending, set delete_requested flag, else mark as free immediately
  if (auto it = pending_reads_.find(offset); it != pending_reads_.end()) {
    DCHECK_EQ(it->second.segment.length, length);
    it->second.delete_requested = true;
  } else {
    storage_.MarkAsFree({offset, length});
  }
}

void OpManager::Stash(std::string_view key, CompactObj& co) {
  unsigned version = ++pending_stashes_[key];

  auto* value = new char[co.Size()];  // unique_ptr can't be captured
  co.GetString(value);

  co.SetIoPending(true);

  auto cb = [this, version, key, value](DiskSegment segment) {
    delete[] value;
    ProcessStashed(key, version, segment);
  };
  storage_.Stash({reinterpret_cast<uint8_t*>(value), co.Size()}, std::move(cb));
}

std::error_code OpManager::Open(std::string_view file) {
  storage_.Open(file);
  return {};
}

OpManager::ReadInfo* OpManager::PrepareRead(DiskSegment segment) {
  ReadInfo& info = pending_reads_[segment.offset];

  if (info.actions.empty()) {
    info.segment = segment;
    storage_.Read(segment,
                  [this, segment](std::string_view value) { ProcessRead(segment.offset, value); });
  } else {
    DCHECK_EQ(info.segment.length, segment.length);
  }

  return &info;
}

void OpManager::ProcessStashed(std::string_view key, unsigned version, DiskSegment segment) {
  if (auto it = pending_stashes_.find(key); it->second == version) {
    pending_stashes_.erase(it);
  } else {
    storage_.MarkAsFree(segment);
    return;
  }

  if (auto it = FindByKey(key); IsValid(it)) {
    it->second.SetIoPending(false);
    it->second.SetExternal(segment.offset, segment.length);
  }
}

void OpManager::ProcessRead(size_t offset, std::string_view value) {
  auto node = pending_reads_.extract(offset);
  ReadInfo& info = node.mapped();

  auto read_cb = [&info, value](tiering::Future<std::string>& fut) {
    fut.Resolve(std::string{value});
  };
  auto update_cb = [&info](std::function<std::string(std::string)>& func) {
    // info.read_buf = func(std::move(info.read_buf));
  };
  for (auto& fut : info.actions) {
    visit(Overloaded{read_cb, update_cb}, fut);
  }

  if (info.delete_requested) {
    storage_.MarkAsFree(info.segment);
  } else {
    // REPORT FETCHED
  }
}

}  // namespace dfly::tiering
