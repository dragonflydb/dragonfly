// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiering/op_manager.h"

#include "base/logging.h"
#include "core/overloaded.h"
#include "io/io.h"
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

std::error_code OpManager::Open(std::string_view file) {
  return storage_.Open(file);
}

void OpManager::Close() {
  storage_.Close();
}

util::fb2::Future<std::string> OpManager::Read(EntryId id, DiskSegment segment) {
  // Fill pages for prepared read as it has no penalty and potentially covers more small segments
  return PrepareRead(segment.FillPages()).ForId(id, segment).futures.emplace_back().get_future();
}

void OpManager::Delete(EntryId id) {
  // If the item isn't offloaded, it has io pending, so cancel it
  DCHECK(pending_stash_ver_.count(ToOwned(id)));
  ++pending_stash_ver_[ToOwned(id)];
}

void OpManager::Delete(DiskSegment segment) {
  DCHECK_EQ(segment.offset % kPageSize, 0u);
  if (auto it = pending_reads_.find(segment.offset); it != pending_reads_.end()) {
    // If a read is pending, it will be deleted once the read finished
    it->second.delete_requested = true;
  } else {
    // Otherwise, delete it immediately
    storage_.MarkAsFree(segment);
  }
}

std::error_code OpManager::Stash(EntryId id_ref, std::string_view value) {
  auto id = ToOwned(id_ref);
  unsigned version = ++pending_stash_ver_[id];

  std::shared_ptr<char[]> buf(new char[value.length()]);
  memcpy(buf.get(), value.data(), value.length());

  io::MutableBytes buf_view{reinterpret_cast<uint8_t*>(buf.get()), value.length()};
  return storage_.Stash(buf_view, [this, version, id = std::move(id), buf](DiskSegment segment) {
    ProcessStashed(Borrowed(id), version, segment);
  });
}

OpManager::ReadOp& OpManager::PrepareRead(DiskSegment aligned_segment) {
  DCHECK_EQ(aligned_segment.offset % kPageSize, 0u);
  DCHECK_EQ(aligned_segment.length % kPageSize, 0u);

  auto [it, inserted] = pending_reads_.try_emplace(aligned_segment.offset, aligned_segment);
  if (inserted) {
    storage_.Read(aligned_segment, [this, aligned_segment](std::string_view value) {
      ProcessRead(aligned_segment.offset, value);
    });
  }
  return it->second;
}

void OpManager::ProcessStashed(EntryId id, unsigned version, DiskSegment segment) {
  if (auto it = pending_stash_ver_.find(ToOwned(id));
      it != pending_stash_ver_.end() && it->second == version) {
    pending_stash_ver_.erase(it);
    ReportStashed(id, segment);
  } else {
    storage_.MarkAsFree(segment);
  }
}

void OpManager::ProcessRead(size_t offset, std::string_view value) {
  ReadOp* info = &pending_reads_.at(offset);

  for (auto& ko : info->key_ops) {
    auto key_value = value.substr(ko.segment.offset - info->segment.offset, ko.segment.length);

    for (auto& fut : ko.futures)
      fut.set_value(std::string{key_value});

    ReportFetched(Borrowed(ko.id), key_value, ko.segment);
  }

  if (info->delete_requested)
    storage_.MarkAsFree(info->segment);
  pending_reads_.erase(offset);
}

OpManager::EntryOps& OpManager::ReadOp::ForId(EntryId id, DiskSegment key_segment) {
  DCHECK_GE(key_segment.offset, segment.offset);
  DCHECK_LE(key_segment.length, segment.length);

  for (auto& ops : key_ops) {
    if (Borrowed(ops.id) == id)
      return ops;
  }
  return key_ops.emplace_back(ToOwned(id), key_segment);
}

}  // namespace dfly::tiering
