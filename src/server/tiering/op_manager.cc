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
#include "util/fibers/fibers.h"
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

OpManager::~OpManager() {
  DCHECK(pending_stash_ver_.empty());
  DCHECK(pending_reads_.empty());
}

std::error_code OpManager::Open(std::string_view file) {
  return storage_.Open(file);
}

void OpManager::Close() {
  storage_.Close();
  DCHECK(pending_stash_ver_.empty());
  DCHECK(pending_reads_.empty());
}

void OpManager::Enqueue(EntryId id, DiskSegment segment, ReadCallback cb) {
  // Fill pages for prepared read as it has no penalty and potentially covers more small segments
  PrepareRead(segment.ContainingPages())
      .ForSegment(segment, id)
      .callbacks.emplace_back(std::move(cb));
}

void OpManager::Delete(EntryId id) {
  // If the item isn't offloaded, it has io pending, so cancel it
  DCHECK(pending_stash_ver_.count(ToOwned(id)));
  pending_stash_ver_.erase(ToOwned(id));
}

void OpManager::DeleteOffloaded(DiskSegment segment) {
  EntryOps* pending_read = nullptr;

  auto base_it = pending_reads_.find(segment.ContainingPages().offset);
  if (base_it != pending_reads_.end())
    pending_read = base_it->second.Find(segment);

  if (pending_read) {
    // Mark that the read operation must finilize with deletion.
    pending_read->deleting = true;
  } else if (NotifyDelete(segment) && base_it == pending_reads_.end()) {
    storage_.MarkAsFree(segment.ContainingPages());
  }
}

std::error_code OpManager::Stash(EntryId id_ref, std::string_view value, io::Bytes footer) {
  auto id = ToOwned(id_ref);
  unsigned version = pending_stash_ver_[id] = ++pending_stash_counter_;

  io::Bytes buf_view = io::Buffer(value);
  auto io_cb = [this, version, id = std::move(id)](io::Result<DiskSegment> segment) {
    ProcessStashed(Borrowed(id), version, segment);
  };

  // May block due to blocking call to Grow.
  auto ec = storage_.Stash(buf_view, footer, std::move(io_cb));
  if (ec)
    pending_stash_ver_.erase(ToOwned(id_ref));
  return ec;
}

OpManager::ReadOp& OpManager::PrepareRead(DiskSegment aligned_segment) {
  DCHECK_EQ(aligned_segment.offset % kPageSize, 0u);
  DCHECK_EQ(aligned_segment.length % kPageSize, 0u);

  auto [it, inserted] = pending_reads_.try_emplace(aligned_segment.offset, aligned_segment);
  if (inserted) {
    auto io_cb = [this, aligned_segment](io::Result<std::string_view> result) {
      CHECK(result) << result.error();  // TODO: to handle this gracefully.
      ProcessRead(aligned_segment.offset, *result);
    };
    storage_.Read(aligned_segment, io_cb);
  }
  return it->second;
}

void OpManager::ProcessStashed(EntryId id, unsigned version,
                               const io::Result<DiskSegment>& segment) {
  if (auto it = pending_stash_ver_.find(ToOwned(id));
      it != pending_stash_ver_.end() && it->second == version) {
    pending_stash_ver_.erase(it);
    NotifyStashed(id, segment);
  } else if (segment) {
    // Throw away the value because it's no longer up-to-date even if no error occured
    storage_.MarkAsFree(*segment);
  }
}

void OpManager::ProcessRead(size_t offset, std::string_view page) {
  util::FiberAtomicGuard guard;  // atomically update items, no in-between states should be possible
  ReadOp* info = &pending_reads_.at(offset);

  // Reorder base read (offset 0) to be last, so reads for defragmentation are handled last.
  // If we already have a page read for defragmentation pending and some other read for the
  // sub-segment is enqueued, we first must handle the sub-segment read, only then the full page
  // read
  for (size_t i = 0; i + 1 < info->key_ops.size(); i++) {
    if (info->key_ops[i].segment.offset % kPageSize == 0) {
      std::swap(info->key_ops[i], info->key_ops.back());
      break;
    }
  }

  bool deleting_full = false;
  std::string key_value;

  // Report functions in the loop may append items to info->key_ops during the traversal
  for (size_t i = 0; i < info->key_ops.size(); i++) {
    auto& ko = info->key_ops[i];
    key_value = page.substr(ko.segment.offset - info->segment.offset, ko.segment.length);

    bool modified = false;
    for (auto& cb : ko.callbacks)
      modified |= cb(!modified, &key_value);

    bool delete_from_storage = ko.deleting;

    // If the item is not being deleted, report is as fetched to be cached potentially.
    // In case it's cached, we might need to delete it.
    if (!delete_from_storage)
      delete_from_storage |= NotifyFetched(Borrowed(ko.id), key_value, ko.segment, modified);

    // If the item is being deleted, check if the full page needs to be deleted.
    if (delete_from_storage)
      deleting_full |= NotifyDelete(ko.segment);
  }

  if (deleting_full) {
    storage_.MarkAsFree(info->segment);
  }

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
