// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiered_storage.h"

extern "C" {
#include "redis/object.h"
}

#include <mimalloc.h>

#include "base/flags.h"
#include "base/logging.h"
#include "server/db_slice.h"
#include "util/proactor_base.h"

ABSL_FLAG(uint32_t, tiered_storage_max_pending_writes, 32,
          "Maximal number of pending writes per thread");

namespace dfly {
using namespace std;
using absl::GetFlag;

struct IndexKey {
  DbIndex db_indx;
  PrimeKey key;

  IndexKey() {
  }

  // We define here a weird copy constructor because map uses pair<const PrimeKey,..>
  // and "const" prevents moving IndexKey.
  IndexKey(const IndexKey& o) : db_indx(o.db_indx), key(o.key.AsRef()) {
  }

  IndexKey(IndexKey&&) = default;

  IndexKey(DbIndex i, PrimeKey k) : db_indx(i), key(std::move(k)) {
  }

  bool operator==(const IndexKey& ik) const {
    return ik.db_indx == db_indx && ik.key == key;
  }

  // IndexKey& operator=(IndexKey&&) {}
  // IndexKey& operator=(const IndexKey&) =delete;
};

struct EntryHash {
  size_t operator()(const IndexKey& ik) const {
    return ik.key.HashCode() ^ (size_t(ik.db_indx) << 16);
  }
};

const size_t kBatchSize = 4096;
const size_t kPageAlignment = 4096;

struct TieredStorage::ActiveIoRequest {
  size_t file_offset;

  size_t batch_offs;
  char* block_ptr;

  // entry -> offset
  /*absl::flat_hash_map<IndexKey, size_t, EntryHash, std::equal_to<>,
                      mi_stl_allocator<std::pair<const IndexKey, size_t>>>*/
  absl::flat_hash_map<IndexKey, size_t, EntryHash, std::equal_to<>> entries;

  explicit ActiveIoRequest(size_t file_offs) : file_offset(file_offs), batch_offs(0) {
    block_ptr = (char*)mi_malloc_aligned(kBatchSize, kPageAlignment);
    DCHECK_EQ(0u, intptr_t(block_ptr) % kPageAlignment);
  }

  ~ActiveIoRequest() {
    mi_free(block_ptr);
  }

  bool CanAccomodate(size_t length) const {
    return batch_offs + length <= kBatchSize;
  }

  void Serialize(IndexKey ikey, const CompactObj& co);
};

// we need to support migration of keys to other pages.
// for that we store hash id of each serialized entry (8 bytes) as a back reference to
// it in the PrimeTable.
// Each 4k batch will contain at most 56 entries (56*64 + 56*8 = 4032).
// we will need 56*8=448 bytes header for hash entries.
constexpr size_t kHeaderSize = 448;

void TieredStorage::ActiveIoRequest::Serialize(IndexKey ikey, const CompactObj& co) {
  DCHECK(!co.HasIoPending());

  size_t item_size = co.Size();
  DCHECK_LE(item_size + batch_offs, kBatchSize);
  bool single_item = false;
  if (batch_offs == 0) {
    DCHECK_EQ(0u, file_offset % 4096);

    if (item_size < kBatchSize / 2) {
      batch_offs = kHeaderSize;
    } else {
      single_item = true;
    }
  }
  co.GetString(block_ptr + batch_offs);

  bool added = entries.emplace(move(ikey), file_offset + batch_offs).second;
  CHECK(added);
  if (single_item) {
    batch_offs = kBatchSize;
  } else {
    uint64_t hc = co.HashCode();
    unsigned entry_index = entries.size() - 1;
    absl::little_endian::Store64(block_ptr + entry_index * 8, hc);
    batch_offs += item_size;  // saved into opened block.
  }
}

TieredStorage::TieredStorage(DbSlice* db_slice) : db_slice_(*db_slice), pending_req_(256) {
}

TieredStorage::~TieredStorage() {
  for (auto* db : db_arr_)
    delete db;
}

error_code TieredStorage::Open(const string& path) {
  error_code ec = io_mgr_.Open(path);
  if (!ec) {
    if (io_mgr_.Span()) {  // Add initial storage.
      alloc_.AddStorage(0, io_mgr_.Span());
    }
  }
  return ec;
}

std::error_code TieredStorage::Read(size_t offset, size_t len, char* dest) {
  stats_.external_reads++;

  return io_mgr_.Read(offset, io::MutableBytes{reinterpret_cast<uint8_t*>(dest), len});
}

void TieredStorage::Free(DbIndex db_indx, size_t offset, size_t len) {
  if (offset % 4096 == 0) {
    alloc_.Free(offset, len);
  } else {
    size_t offs_page = offset / 4096;
    auto it = multi_cnt_.find(offs_page);
    CHECK(it != multi_cnt_.end()) << offs_page;
    MultiBatch& mb = it->second;
    CHECK_GE(mb.used, len);
    mb.used -= len;
    if (mb.used == 0) {
      alloc_.Free(offs_page * 4096, ExternalAllocator::kMinBlockSize);
      VLOG(1) << "multi_cnt_ erase " << it->first;
      multi_cnt_.erase(it);
    }
  }

  auto* stats = db_slice_.MutableStats(db_indx);
  stats->external_entries -= 1;
  stats->external_size -= len;
}

void TieredStorage::Shutdown() {
  io_mgr_.Shutdown();
}

TieredStats TieredStorage::GetStats() const {
  TieredStats res = stats_;
  res.storage_capacity = alloc_.capacity();
  res.storage_reserved = alloc_.allocated_bytes();

  return res;
}

void TieredStorage::SendIoRequest(ActiveIoRequest* req) {
#if 1
  // static string tmp(4096, 'x');
  // string_view sv{tmp};
  string_view sv{req->block_ptr, kBatchSize};

  active_req_sem_.await(
      [this] { return num_active_requests_ <= GetFlag(FLAGS_tiered_storage_max_pending_writes); });

  auto cb = [this, req](int res) { FinishIoRequest(res, req); };
  ++num_active_requests_;
  io_mgr_.WriteAsync(req->file_offset, sv, move(cb));
  ++stats_.external_writes;

#else
  FinishIoRequest(0, req);
#endif
}

void TieredStorage::FinishIoRequest(int io_res, ActiveIoRequest* req) {
  if (io_res < 0) {
    LOG(ERROR) << "Error writing into ssd file: " << util::detail::SafeErrorMessage(-io_res);
    for (const auto& k_v : req->entries) {
      const IndexKey& ikey = k_v.first;
      PrimeTable* pt = db_slice_.GetTables(ikey.db_indx).first;
      PrimeIterator it = pt->Find(ikey.key);
      CHECK(it->second.HasIoPending());
      it->second.SetIoPending(false);
    }
  } else {
    uint16_t used_total = 0;

    for (const auto& k_v : req->entries) {
      const IndexKey& ikey = k_v.first;

      size_t item_offset = k_v.second;
      CHECK_EQ(item_offset / 4096, req->file_offset / 4096);
      PrimeTable* pt = db_slice_.GetTables(ikey.db_indx).first;
      PrimeIterator it = pt->Find(ikey.key);
      CHECK(!it.is_done()) << "TBD";
      CHECK(it->second.HasIoPending());

      it->second.SetIoPending(false);
      size_t item_size = it->second.Size();
      SetExternal(ikey.db_indx, item_offset, &it->second);
      used_total += item_size;
    }

    CHECK_GT(req->entries.size(), 1u);  // multi-item batch
    MultiBatch mb{used_total};
    VLOG(1) << "multi_cnt_ emplace " << req->file_offset / 4096;
    multi_cnt_.emplace(req->file_offset / 4096, mb);
  }

  delete req;
  --num_active_requests_;
  if (num_active_requests_ == GetFlag(FLAGS_tiered_storage_max_pending_writes)) {
    active_req_sem_.notifyAll();
  }

  VLOG_IF(1, num_active_requests_ == 0) << "Finished active requests";
}

void TieredStorage::SetExternal(DbIndex db_index, size_t item_offset, PrimeValue* dest) {
  auto* stats = db_slice_.MutableStats(db_index);

  size_t heap_size = dest->MallocUsed();
  size_t item_size = dest->Size();

  stats->obj_memory_usage -= heap_size;
  stats->strval_memory_usage -= heap_size;

  dest->SetExternal(item_offset, item_size);

  stats->external_entries += 1;
  stats->external_size += item_size;
}

bool TieredStorage::ShouldFlush() {
  if (num_active_requests_ >= GetFlag(FLAGS_tiered_storage_max_pending_writes))
    return false;

  return pending_req_.size() > pending_req_.capacity() / 2;
}

error_code TieredStorage::UnloadItem(DbIndex db_index, PrimeIterator it) {
  CHECK_EQ(OBJ_STRING, it->second.ObjType());

  size_t blob_len = it->second.Size();
  if (blob_len >= kBatchSize / 2 &&
      num_active_requests_ < GetFlag(FLAGS_tiered_storage_max_pending_writes)) {
    LOG(FATAL) << "TBD";
  }
  error_code ec;

  pending_req_.EmplaceOrOverride(PendingReq{it.bucket_cursor().value(), db_index});
  // db->pending_upload[it.bucket_cursor().value()] += blob_len;

  // size_t grow_size = 0;

  if (ShouldFlush()) {
    FlushPending();
  }

  // if we reached high utilization of the file range - try to grow the file.
  if (alloc_.allocated_bytes() > size_t(alloc_.capacity() * 0.85)) {
    InitiateGrow(1ULL << 28);
  }

  return ec;
}

bool IsObjFitToUnload(const PrimeValue& pv) {
  return pv.ObjType() == OBJ_STRING && !pv.IsExternal() && pv.Size() >= 64 && !pv.HasIoPending();
};

void TieredStorage::FlushPending() {
  DCHECK(!io_mgr_.grow_pending() && !pending_req_.empty());

  vector<pair<DbIndex, uint64_t>> canonic_req;
  canonic_req.reserve(pending_req_.size());

  for (size_t i = 0; i < pending_req_.size(); ++i) {
    const PendingReq* req = pending_req_.GetItem(i);
    canonic_req.emplace_back(req->db_indx, req->cursor);
  }
  pending_req_.ConsumeHead(pending_req_.size());
  // remove duplicates and sort.
  {
    sort(canonic_req.begin(), canonic_req.end());
    auto it = unique(canonic_req.begin(), canonic_req.end());
    canonic_req.resize(it - canonic_req.begin());
  }

  // TODO: we could add item size and sort from largest to smallest before
  // the aggregation.
  constexpr size_t kMaxBatchLen = 64;
  PrimeTable::iterator single_batch[kMaxBatchLen];
  unsigned batch_len = 0;

  auto tr_cb = [&](PrimeTable::iterator it) {
    if (IsObjFitToUnload(it->second)) {
      CHECK_LT(batch_len, kMaxBatchLen);
      single_batch[batch_len++] = it;
    }
  };

  ActiveIoRequest* active_req = nullptr;

  for (size_t i = 0; i < canonic_req.size(); ++i) {
    DbIndex db_ind = canonic_req[i].first;
    uint64_t cursor_val = canonic_req[i].second;
    PrimeTable::cursor curs(cursor_val);
    db_slice_.GetTables(db_ind).first->Traverse(curs, tr_cb);

    for (unsigned j = 0; j < batch_len; ++j) {
      PrimeIterator it = single_batch[j];
      size_t item_size = it->second.Size();
      DCHECK_GT(item_size, 0u);

      if (!active_req || !active_req->CanAccomodate(item_size)) {
        if (active_req) {  // need to close
          // save the block asynchronously.
          ++submitted_io_writes_;
          submitted_io_write_size_ += kBatchSize;

          SendIoRequest(active_req);
          active_req = nullptr;
        }

        int64_t res = alloc_.Malloc(item_size);
        if (res < 0) {
          InitiateGrow(-res);
          return;
        }

        size_t batch_size = ExternalAllocator::GoodSize(item_size);
        DCHECK_EQ(batch_size, ExternalAllocator::GoodSize(batch_size));

        active_req = new ActiveIoRequest(res);
      }

      active_req->Serialize(IndexKey{db_ind, it->first.AsRef()}, it->second);
      it->second.SetIoPending(true);
    }
    batch_len = 0;
  }

  if (active_req) {
    if (active_req->batch_offs >= kBatchSize / 2) {
      SendIoRequest(active_req);
    } else {
      // not enough data to fill the page.
      // rollback the pending bit.
      for (auto& k_v : active_req->entries) {
        const IndexKey& ikey = k_v.first;
        PrimeTable* pt = db_slice_.GetTables(ikey.db_indx).first;
        PrimeIterator it = pt->Find(ikey.key);
        it->second.SetIoPending(false);
        // TODO: we could enqueue those back to pending_req.
      }
      delete active_req;
    }
  }
}

void TieredStorage::InitiateGrow(size_t grow_size) {
  if (io_mgr_.grow_pending())
    return;
  DCHECK_GT(grow_size, 0u);

  size_t start = io_mgr_.Span();

  auto cb = [start, grow_size, this](int io_res) {
    if (io_res == 0) {
      alloc_.AddStorage(start, grow_size);
    } else {
      LOG_FIRST_N(ERROR, 10) << "Error enlarging storage " << io_res;
    }
  };

  error_code ec = io_mgr_.GrowAsync(grow_size, move(cb));
  CHECK(!ec) << "TBD";  // TODO
}

}  // namespace dfly
