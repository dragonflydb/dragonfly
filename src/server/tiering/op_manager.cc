// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiering/op_manager.h"

#include "base/logging.h"
#include "core/overloaded.h"
#include "server/tiering/common.h"

namespace dfly::tiering {

namespace {

OpManager::OwnedEntryId ToOwned(OpManager::EntryId id) {
  Overloaded convert{[](unsigned i) -> OpManager::OwnedEntryId { return i; },
                     [](std::string_view s) -> OpManager::OwnedEntryId { return std::string{s}; }};
  return std::visit(convert, id);
}

OpManager::EntryId Borrowed(const OpManager::OwnedEntryId& id) {
  return std::visit([](const auto& v) -> OpManager::EntryId { return v; }, id);
}

}  // namespace

OpManager::~OpManager() {
  storage_.Close();
}

tiering::Future<std::string> OpManager::Read(EntryId id, DiskSegment segment) {
  return PrepareRead(segment)->ForId(id, segment)->futures.emplace_back();
}

void OpManager::Delete(EntryId id, std::optional<DiskSegment> segment) {
  if (!segment.has_value()) {
    // If the item isn't offloaded, it has io pending, so cancel it
    DCHECK(pending_stashes_.count(ToOwned(id)));
    ++pending_stashes_[ToOwned(id)];
  } else if (auto it = pending_reads_.find(segment->offset); it != pending_reads_.end()) {
    // If a read is pending, it will be deleted once the read finished
    DCHECK_EQ(it->second.segment.length, segment->length);
    it->second.delete_requested = true;
  } else {
    // Otherwise, delete it immediately
    storage_.MarkAsFree(*segment);
  }
}

std::error_code OpManager::Stash(EntryId id_ref, std::string_view value) {
  auto id = ToOwned(id_ref);
  unsigned version = ++pending_stashes_[id];

  auto* value_ptr = new char[value.size()];  // unique_ptr can't be captured
  memcpy(value_ptr, value.data(), value.length());

  auto cb = [this, version, id, value_ptr](DiskSegment segment) {
    delete[] value_ptr;
    ProcessStashed(Borrowed(id), version, segment);
  };
  auto ec = storage_.Stash({reinterpret_cast<uint8_t*>(value_ptr), value.length()}, std::move(cb));
  if (ec)
    delete[] value_ptr;
  return ec;
}

std::error_code OpManager::Open(std::string_view file) {
  return storage_.Open(file);
}

OpManager::ReadOp* OpManager::PrepareRead(DiskSegment segment) {
  // If a read for a small key is requested, read the whole page
  if (segment.offset % 4_KB != 0)
    segment = {segment.offset / 4_KB * 4_KB, 4_KB};

  auto [it, inserted] = pending_reads_.try_emplace(segment.offset, segment);
  if (inserted) {
    auto cb = [this, segment](std::string_view value) { ProcessRead(segment.offset, value); };
    storage_.Read(segment, std::move(cb));
  }
  return &it->second;
}

void OpManager::ProcessStashed(EntryId id, unsigned version, DiskSegment segment) {
  if (auto it = pending_stashes_.find(ToOwned(id));
      it != pending_stashes_.end() && it->second == version) {
    pending_stashes_.erase(it);
    ReportStashed(id, segment);
  } else {
    storage_.MarkAsFree(segment);
  }
}

void OpManager::ProcessRead(size_t offset, std::string_view value) {
  auto node = pending_reads_.extract(offset);
  ReadOp& info = node.mapped();

  for (auto& ko : info.key_ops) {
    auto key_value = value.substr(ko.segment.offset - info.segment.offset, ko.segment.length);
    for (auto& fut : ko.futures)
      fut.Resolve(std::string{key_value});
    ReportFetched(Borrowed(ko.id), key_value, ko.segment);
  }

  if (info.delete_requested)
    storage_.MarkAsFree(info.segment);
}

OpManager::EntryOps* OpManager::ReadOp::ForId(EntryId id, DiskSegment key_segment) {
  DCHECK_GE(key_segment.offset, segment.offset);
  DCHECK_LE(key_segment.length, segment.length);

  for (auto& ops : key_ops) {
    if (Borrowed(ops.id) == id)
      return &ops;
  }
  return &key_ops.emplace_back(ToOwned(id), key_segment);
}

}  // namespace dfly::tiering
