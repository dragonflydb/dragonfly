// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiering/op_manager.h"

#include <variant>

#include "base/logging.h"
#include "core/overloaded.h"
#include "io/io.h"
#include "server/tiering/common.h"
#include "server/tiering/disk_storage.h"

namespace dfly::tiering {

namespace {

OpManager::OwnedEntryId ToOwned(OpManager::EntryId id) {
  Overloaded convert{[](unsigned i) -> OpManager::OwnedEntryId { return i; },
                     [](std::pair<DbIndex, std::string_view> p) -> OpManager::OwnedEntryId {
                       return std::make_pair(p.first, std::string{p.second});
                     }};
  return std::visit(convert, id);
}

OpManager::EntryId Borrowed(const OpManager::OwnedEntryId& id) {
  return std::visit([](const auto& v) -> OpManager::EntryId { return v; }, id);
}

}  // namespace

OpManager::OpManager(size_t max_size) : storage_{max_size} {
}

std::error_code OpManager::Open(std::string_view file) {
  return storage_.Open(file);
}

void OpManager::Close() {
  storage_.Close();
}

void OpManager::Enqueue(EntryId id, DiskSegment segment, ReadCallback cb) {
  // Fill pages for prepared read as it has no penalty and potentially covers more small segments
  PrepareRead(segment.FillPages()).ForSegment(segment, id).callbacks.emplace_back(std::move(cb));
}

void OpManager::Delete(EntryId id) {
  // If the item isn't offloaded, it has io pending, so cancel it
  DCHECK(pending_stash_ver_.count(ToOwned(id)));
  pending_stash_ver_.erase(ToOwned(id));
}

void OpManager::Delete(DiskSegment segment) {
  EntryOps* pending_op = nullptr;
  if (auto it = pending_reads_.find(segment.offset); it != pending_reads_.end())
    pending_op = it->second.Find(segment);

  if (pending_op) {
    pending_op->deleting = true;
  } else if (ReportDelete(segment)) {
    storage_.MarkAsFree(segment.FillPages());
  }
}

std::error_code OpManager::Stash(EntryId id_ref, std::string_view value) {
  auto id = ToOwned(id_ref);
  unsigned version = pending_stash_ver_[id] = ++pending_stash_counter_;

  io::Bytes buf_view{reinterpret_cast<const uint8_t*>(value.data()), value.length()};
  auto io_cb = [this, version, id = std::move(id)](DiskSegment segment, std::error_code ec) {
    ProcessStashed(Borrowed(id), version, segment, ec);
  };
  return storage_.Stash(buf_view, std::move(io_cb));
}

OpManager::ReadOp& OpManager::PrepareRead(DiskSegment aligned_segment) {
  DCHECK_EQ(aligned_segment.offset % kPageSize, 0u);
  DCHECK_EQ(aligned_segment.length % kPageSize, 0u);

  auto [it, inserted] = pending_reads_.try_emplace(aligned_segment.offset, aligned_segment);
  if (inserted) {
    auto io_cb = [this, aligned_segment](std::string_view value, std::error_code ec) {
      ProcessRead(aligned_segment.offset, value);
    };
    storage_.Read(aligned_segment, io_cb);
  }
  return it->second;
}

void OpManager::ProcessStashed(EntryId id, unsigned version, DiskSegment segment,
                               std::error_code ec) {
  if (auto it = pending_stash_ver_.find(ToOwned(id));
      it != pending_stash_ver_.end() && it->second == version) {
    pending_stash_ver_.erase(it);
    ReportStashed(id, segment, ec);
  } else if (!ec) {
    // Throw away the value because it's no longer up-to-date even if no error occured
    storage_.MarkAsFree(segment);
  }
}

void OpManager::ProcessRead(size_t offset, std::string_view value) {
  util::FiberAtomicGuard guard;  // atomically update items, no in-between states should be possible
  ReadOp* info = &pending_reads_.at(offset);

  bool deleting_full = false;
  std::string key_value;
  for (size_t i = 0; i < info->key_ops.size(); i++) {  // more items can be read while iterating
    auto& ko = info->key_ops[i];
    key_value = value.substr(ko.segment.offset - info->segment.offset, ko.segment.length);

    bool modified = false;
    for (auto& cb : ko.callbacks)
      modified |= cb(&key_value);

    // If the item is not being deleted, report is as fetched to be cached potentially.
    // In case it's cached, we might need to delete it.
    if (!ko.deleting)
      ko.deleting |= ReportFetched(Borrowed(ko.id), key_value, ko.segment, modified);

    // If the item is being deleted, check if the full page needs to be deleted.
    if (ko.deleting)
      deleting_full |= ReportDelete(ko.segment);
  }

  if (deleting_full)
    storage_.MarkAsFree(info->segment);

  pending_reads_.erase(offset);
}

OpManager::EntryOps& OpManager::ReadOp::ForSegment(DiskSegment key_segment, EntryId id) {
  DCHECK_GE(key_segment.offset, segment.offset);
  DCHECK_LE(key_segment.length, segment.length);

  for (auto& ops : key_ops) {
    if (ops.segment.offset == key_segment.offset)
      return ops;
  }
  return key_ops.emplace_back(ToOwned(id), key_segment);
}

OpManager::EntryOps* OpManager::ReadOp::Find(DiskSegment key_segment) {
  for (auto& ops : key_ops) {
    if (ops.segment.offset == key_segment.offset)
      return &ops;
  }
  return nullptr;
}

OpManager::Stats OpManager::GetStats() const {
  return {.disk_stats = storage_.GetStats(),
          .pending_read_cnt = pending_reads_.size(),
          .pending_stash_cnt = pending_stash_ver_.size()};
}

}  // namespace dfly::tiering
