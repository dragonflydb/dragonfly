// Copyright 2022, DragonflyDB authors.  All rights reserved.
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

string BackingFileName(string_view base, unsigned index) {
  return absl::StrCat(base, "-", absl::Dec(index, absl::kZeroPad4), ".ssd");
}

#if 0
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
#endif

const size_t kBatchSize = 4096;
const size_t kPageAlignment = 4096;

// we must support defragmentation of small entries.
// This is similar to in-memory external defragmentation:
// some of the values are deleted but the page is still used.
// In order to allow moving entries to another pages we keep hash id of each
// serialized entry (8 bytes) as a "back reference" to its PrimeTable key.
// DashTable can uniquely determines segment id and the home bucket id based on the hash value
// but at the end it can not identify uniquely the entry based only its hash value
// (problematic use-case: when different keys with the same hash are hosted).
// It's fine because in that case
// we can check each candidate whether it points back to the hosted entry in the page.
// Each 4k batch will contain at most 56 entries (56*64 + 56*8 = 4032).
// Our header will be:
//   1 byte for number of items N (1-56)
//   N*8 - hash values of the items
//
// To allow serializing dynamic number of entries we serialize them backwards
// from the end of the page and this is why batch_offs_ starts from kBatchSize.
// we will need maximum 1+56*8=449 bytes for the header.
// constexpr size_t kMaxHeaderSize = 448;

class TieredStorage::ActiveIoRequest {
  static constexpr unsigned kMaxEntriesCount = 56;

 public:
  explicit ActiveIoRequest(DbIndex db_index, size_t file_offs)
      : db_index_(db_index), file_offset_(file_offs), batch_offs_(kBatchSize) {
    block_ptr_ = (char*)mi_malloc_aligned(kBatchSize, kPageAlignment);
    DCHECK_EQ(0u, intptr_t(block_ptr_) % kPageAlignment);
  }

  ~ActiveIoRequest() {
    mi_free(block_ptr_);
  }

  bool CanAccommodate(size_t length) const {
    return batch_offs_ >= length + 8 + HeaderLength();
  }

  void Serialize(PrimeKey pkey, const PrimeValue& co);
  void WriteAsync(IoMgr* iomgr, std::function<void(int)> cb);
  void Undo(DbSlice* db_slice);

  // Returns total number of bytes being offloaded by externalized values.
  unsigned ExternalizeEntries(DbSlice* db_slice);

  ActiveIoRequest(const ActiveIoRequest&) = delete;
  ActiveIoRequest& operator=(const ActiveIoRequest&) = delete;

  const auto& entries() const {
    return entries_;
  }

  size_t page_index() const {
    return file_offset_ / kBatchSize;
  }

  DbIndex db_index() const {
    return db_index_;
  }

  size_t serialized_len() const {
    return kBatchSize - batch_offs_;
  }

 private:
  size_t HeaderLength() const {
    return 1 + 8 * entries_.size();
  }

  DbIndex db_index_;
  uint16_t used_size_ = 0;
  size_t file_offset_;

  size_t batch_offs_;
  char* block_ptr_;

  uint64_t hash_values_[kMaxEntriesCount];
  // key -> offset
  absl::flat_hash_map<string, size_t> entries_;
};

void TieredStorage::ActiveIoRequest::Serialize(PrimeKey pkey, const PrimeValue& co) {
  DCHECK(!co.HasIoPending());
  DCHECK_LT(entries_.size(), ABSL_ARRAYSIZE(hash_values_));

  size_t item_size = co.Size();
  DCHECK_LE(item_size + HeaderLength(), batch_offs_);
  used_size_ += item_size;
  batch_offs_ -= item_size;                // advance backwards
  co.GetString(block_ptr_ + batch_offs_);  // serialize the object

  string keystr;
  pkey.GetString(&keystr);
  uint64_t keyhash = CompactObj::HashCode(keystr);
  hash_values_[entries_.size()] = keyhash;
  bool added = entries_.emplace(std::move(keystr), file_offset_ + batch_offs_).second;
  CHECK(added);
}

void TieredStorage::ActiveIoRequest::WriteAsync(IoMgr* io_mgr, std::function<void(int)> cb) {
  DCHECK_LE(HeaderLength(), batch_offs_);
  DCHECK_LE(entries_.size(), kMaxEntriesCount);

  block_ptr_[0] = entries_.size();
  for (unsigned i = 0; i < entries_.size(); ++i) {
    absl::little_endian::Store64(block_ptr_ + 1 + i * 8, hash_values_[i]);
  }

  string_view sv{block_ptr_, kBatchSize};
  io_mgr->WriteAsync(file_offset_, sv, move(cb));
}

void TieredStorage::ActiveIoRequest::Undo(DbSlice* db_slice) {
  PrimeTable* pt = db_slice->GetTables(db_index_).first;
  for (const auto& [pkey, _] : entries_) {
    PrimeIterator it = pt->Find(pkey);

    // TODO: what happens when if the entry was deleted meanwhile
    // or it has been serialized again?
    CHECK(it->second.HasIoPending()) << "TBD: fix inconsistencies";

    it->second.SetIoPending(false);
  }
}

unsigned TieredStorage::ActiveIoRequest::ExternalizeEntries(DbSlice* db_slice) {
  PrimeTable* pt = db_slice->GetTables(db_index_).first;
  DbTableStats* stats = db_slice->MutableStats(db_index_);
  unsigned total_used = 0;

  for (const auto& k_v : entries_) {
    const auto& pkey = k_v.first;

    size_t item_offset = k_v.second;

    PrimeIterator it = pt->Find(pkey);

    // TODO: the key may be deleted or overriden. The last one is especially dangerous.
    // we should update active pending request with any change we make to the entry.
    // it should not be a problem since we have HasIoPending tag that mean we must
    // update the inflight request (or mark the entry as cancelled).
    CHECK(!it.is_done()) << "TBD";
    CHECK(it->second.HasIoPending());

    it->second.SetIoPending(false);

    PrimeValue& pv = it->second;
    size_t heap_size = pv.MallocUsed();
    size_t item_size = pv.Size();

    total_used += item_size;
    stats->obj_memory_usage -= heap_size;
    if (pv.ObjType() == OBJ_STRING)
      stats->strval_memory_usage -= heap_size;

    VLOG(2) << "SetExternal: " << pkey << " " << item_offset;
    pv.SetExternal(item_offset, item_size);

    stats->external_entries += 1;
    stats->external_size += item_size;
  }

  return total_used;
}

bool TieredStorage::PerDb::ShouldFlush() const {
  return bucket_cursors.size() > bucket_cursors.capacity() / 2;
}

TieredStorage::TieredStorage(DbSlice* db_slice) : db_slice_(*db_slice) {
}

TieredStorage::~TieredStorage() {
  for (auto* db : db_arr_) {
    delete db;
  }
}

error_code TieredStorage::Open(const string& base) {
  string path = BackingFileName(base, db_slice_.shard_id());

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
  DVLOG(1) << "Read " << offset << " " << len;

  return io_mgr_.Read(offset, io::MutableBytes{reinterpret_cast<uint8_t*>(dest), len});
}

void TieredStorage::Free(size_t offset, size_t len) {
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

  active_req_sem_.await(
      [this] { return num_active_requests_ <= GetFlag(FLAGS_tiered_storage_max_pending_writes); });

  auto cb = [this, req](int res) { FinishIoRequest(res, req); };

  ++num_active_requests_;
  req->WriteAsync(&io_mgr_, move(cb));
  ++stats_.external_writes;

#else
  FinishIoRequest(0, req);
#endif
}

void TieredStorage::FinishIoRequest(int io_res, ActiveIoRequest* req) {
  if (io_res < 0) {
    LOG(ERROR) << "Error writing into ssd file: " << util::detail::SafeErrorMessage(-io_res);
    req->Undo(&db_slice_);
  } else {
    uint16_t used_total = req->ExternalizeEntries(&db_slice_);

    CHECK_GT(req->entries().size(), 1u);  // multi-item batch
    MultiBatch mb{used_total};
    VLOG(1) << "multi_cnt_ emplace " << req->page_index();
    multi_cnt_.emplace(req->page_index(), mb);
  }

  delete req;
  --num_active_requests_;
  if (num_active_requests_ == GetFlag(FLAGS_tiered_storage_max_pending_writes)) {
    active_req_sem_.notifyAll();
  }

  VLOG_IF(1, num_active_requests_ == 0) << "Finished active requests";
}

error_code TieredStorage::UnloadItem(DbIndex db_index, PrimeIterator it) {
  CHECK_EQ(OBJ_STRING, it->second.ObjType());

  // Relevant only for OBJ_STRING, see CHECK above.
  size_t blob_len = it->second.Size();

  if (blob_len >= kBatchSize / 2 &&
      num_active_requests_ < GetFlag(FLAGS_tiered_storage_max_pending_writes)) {
    WriteSingle(db_index, it, blob_len);
    return error_code{};
  }

  if (db_arr_.size() <= db_index) {
    db_arr_.resize(db_index + 1);
  }

  if (db_arr_[db_index] == nullptr) {
    db_arr_[db_index] = new PerDb;
  }

  PerDb* db = db_arr_[db_index];
  db->bucket_cursors.EmplaceOrOverride(it.bucket_cursor().value());
  // db->pending_upload[it.bucket_cursor().value()] += blob_len;

  // size_t grow_size = 0;
  if (db->ShouldFlush()) {
    if (num_active_requests_ < GetFlag(FLAGS_tiered_storage_max_pending_writes)) {
      FlushPending(db_index);

      // if we reached high utilization of the file range - try to grow the file.
      if (alloc_.allocated_bytes() > size_t(alloc_.capacity() * 0.85)) {
        InitiateGrow(1ULL << 28);
      }
    }
  }

  return error_code{};
}

bool IsObjFitToUnload(const PrimeValue& pv) {
  return pv.ObjType() == OBJ_STRING && !pv.IsExternal() && pv.Size() >= 64 && !pv.HasIoPending();
};

void TieredStorage::WriteSingle(DbIndex db_index, PrimeIterator it, size_t blob_len) {
  DCHECK(!it->second.HasIoPending());

  int64_t res = alloc_.Malloc(blob_len);
  if (res < 0) {
    InitiateGrow(-res);
    return;
  }

  constexpr size_t kMask = kPageAlignment - 1;
  size_t page_size = (blob_len + kMask) & (~kMask);

  DCHECK_GE(page_size, blob_len);
  DCHECK_EQ(0u, page_size % kPageAlignment);

  struct SingleRequest {
    char* block_ptr = nullptr;
    PrimeTable* pt = nullptr;
    size_t blob_len = 0;
    off_t offset = 0;
    string key;
  } req;

  char* block_ptr = (char*)mi_malloc_aligned(page_size, kPageAlignment);

  req.blob_len = blob_len;
  req.offset = res;
  req.key = it->first.ToString();
  req.pt = db_slice_.GetTables(db_index).first;
  req.block_ptr = block_ptr;

  it->second.GetString(block_ptr);
  it->second.SetIoPending(true);

  auto cb = [req = std::move(req)](int io_res) {
    PrimeIterator it = req.pt->Find(req.key);
    CHECK(!it.is_done());

    // TODO: what happens when if the entry was deleted meanwhile
    // or it has been serialized again?
    CHECK(it->second.HasIoPending()) << "TBD: fix inconsistencies";
    it->second.SetIoPending(false);

    if (io_res < 0) {
      LOG(ERROR) << "Error writing to ssd storage " << util::detail::SafeErrorMessage(-io_res);
      return;
    }
    it->second.SetExternal(req.offset, req.blob_len);
    mi_free(req.block_ptr);
  };

  io_mgr_.WriteAsync(res, string_view{block_ptr, page_size}, std::move(cb));
}

void TieredStorage::FlushPending(DbIndex db_index) {
  PerDb* db = db_arr_[db_index];

  DCHECK(!io_mgr_.grow_pending() && !db->bucket_cursors.empty());

  vector<uint64_t> canonic_req;
  canonic_req.reserve(db->bucket_cursors.size());

  for (size_t i = 0; i < db->bucket_cursors.size(); ++i) {
    canonic_req.push_back(*db->bucket_cursors.GetItem(i));
  }
  db->bucket_cursors.ConsumeHead(canonic_req.size());

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
    uint64_t cursor_val = canonic_req[i];
    PrimeTable::Cursor curs(cursor_val);
    db_slice_.GetTables(db_index).first->Traverse(curs, tr_cb);

    for (unsigned j = 0; j < batch_len; ++j) {
      PrimeIterator it = single_batch[j];
      size_t item_size = it->second.Size();
      DCHECK_GT(item_size, 0u);

      if (!active_req || !active_req->CanAccommodate(item_size)) {
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

        active_req = new ActiveIoRequest(db_index, res);
      }

      active_req->Serialize(it->first.AsRef(), it->second);
      it->second.SetIoPending(true);
    }
    batch_len = 0;
  }

  // flush or undo the pending request.
  if (active_req) {
    if (active_req->serialized_len() >= kBatchSize / 2) {
      SendIoRequest(active_req);
    } else {
      // data is too small. rollback active_req.
      active_req->Undo(&db_slice_);
      // TODO: we could enqueue those back to pending_req.
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
