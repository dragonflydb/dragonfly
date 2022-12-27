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
#include "server/engine_shard_set.h"
#include "util/proactor_base.h"

ABSL_FLAG(uint32_t, tiered_storage_max_pending_writes, 32,
          "Maximal number of pending writes per thread");

namespace dfly {

using namespace std;
using absl::GetFlag;

constexpr size_t kBlockLen = 4096;
constexpr size_t kBlockAlignment = 4096;

constexpr unsigned kSmallBinLen = 34;
constexpr unsigned kMaxSmallBin = 2032;

constexpr unsigned kSmallBins[kSmallBinLen] = {
    72,  80,  88,  96,  104, 112, 120, 128, 136, 144, 152, 160, 168, 176, 184,  192,  200,
    216, 232, 248, 264, 280, 304, 328, 360, 400, 440, 504, 576, 672, 808, 1016, 1352, 2040,
};

constexpr unsigned SmallToBin(unsigned len) {
  unsigned indx = (len + 7) / 8;
  if (indx <= 9)
    return 0;

  indx -= 9;
  if (indx < 18)
    return indx;

  unsigned rev_indx = (kBlockLen / len) - 1;
  indx = kSmallBinLen - rev_indx;
  if (kSmallBins[indx] < len)
    ++indx;
  return indx;
}

// Compile-time tests for SmallToBin.
constexpr bool CheckBins() {
  for (unsigned i = 64; i <= 2032; ++i) {
    unsigned indx = SmallToBin(i);
    if (kSmallBins[indx] < i)
      return false;
    if (indx > 0 && kSmallBins[indx - 1] > i)
      return false;
  }

  for (unsigned j = 0; j < kSmallBinLen; ++j) {
    if (SmallToBin(kSmallBins[j]) != j)
      return false;
  }
  return true;
}

static_assert(CheckBins());
static_assert(SmallToBin(kMaxSmallBin) == kSmallBinLen - 1);

constexpr unsigned NumEntriesInSmallBin(unsigned bin_size) {
  return kBlockLen / (bin_size + 8);  // 8 for the hash value.
}

static_assert(NumEntriesInSmallBin(72) == 51);

static string BackingFileName(string_view base, unsigned index) {
  return absl::StrCat(base, "-", absl::Dec(index, absl::kZeroPad4), ".ssd");
}

static size_t ExternalizeEntry(size_t item_offset, DbTableStats* stats, PrimeValue* entry) {
  CHECK(entry->HasIoPending());

  entry->SetIoPending(false);

  size_t heap_size = entry->MallocUsed();
  size_t item_size = entry->Size();

  stats->obj_memory_usage -= heap_size;
  if (entry->ObjType() == OBJ_STRING)
    stats->strval_memory_usage -= heap_size;

  entry->SetExternal(item_offset, item_size);

  stats->tiered_entries += 1;
  stats->tiered_size += item_size;

  return item_size;
}

struct PrimeHasher {
  size_t operator()(const PrimeKey& o) const {
    return o.HashCode();
  }
};

struct TieredStorage::PerDb {
  PerDb(const PerDb&) = delete;
  PerDb& operator=(const PerDb&) = delete;
  PerDb() = default;

  using InflightMap = absl::flat_hash_map<string_view, InflightWriteRequest*>;

  struct BinRecord {
    // Those that wait to be serialized. Must be less than NumEntriesInSmallBin for each bin.
    absl::flat_hash_set<CompactObjectView, PrimeHasher> pending_entries;

    // Entries that were scheduled to write but have not completed yet.
    InflightMap enqueued_entries;
  };

  BinRecord bin_map[kSmallBinLen];
};

class TieredStorage::InflightWriteRequest {
 public:
  InflightWriteRequest(DbIndex db_index, unsigned bin_index, uint32_t page_index);
  ~InflightWriteRequest();

  InflightWriteRequest(const InflightWriteRequest&) = delete;
  InflightWriteRequest& operator=(const InflightWriteRequest&) = delete;

  void Add(const PrimeKey& pk, const PrimeValue& pv);

  // returns how many entries were offloaded.
  unsigned ExternalizeEntries(PerDb::BinRecord* bin_record, DbSlice* db_slice);

  void Undo(PerDb::BinRecord* bin_record, DbSlice* db_slice);

  string_view block() const {
    return string_view{block_start_, kBlockLen};
  }

  uint32_t page_index() const {
    return page_index_;
  }

  DbIndex db_index() const {
    return db_index_;
  }

  unsigned bin_index() const {
    return bin_index_;
  }

  const vector<string_view>& entries() const {
    return entries_;
  }

  void SetKeyBlob(size_t len) {
    key_blob_.resize(len);
    next_key_ = key_blob_.data();
  }

 private:
  DbIndex db_index_;
  uint32_t bin_index_;
  uint32_t page_index_;

  char* block_start_;
  char* next_key_ = nullptr;
  std::vector<char> key_blob_;

  vector<string_view> entries_;
};

TieredStorage::InflightWriteRequest::InflightWriteRequest(DbIndex db_index, unsigned bin_index,
                                                          uint32_t page_index)
    : db_index_(db_index), bin_index_(bin_index), page_index_(page_index) {
  block_start_ = (char*)mi_malloc_aligned(kBlockLen, kBlockAlignment);
  DCHECK_EQ(0u, intptr_t(block_start_) % kBlockAlignment);
}

TieredStorage::InflightWriteRequest::~InflightWriteRequest() {
  mi_free(block_start_);
}

void TieredStorage::InflightWriteRequest::Add(const PrimeKey& pk, const PrimeValue& pv) {
  DCHECK(!pv.IsExternal());

  unsigned bin_size = kSmallBins[bin_index_];
  unsigned max_entries = NumEntriesInSmallBin(bin_size);

  char* next_hash = block_start_ + entries_.size();
  char* next_data = block_start_ + max_entries * 8 + entries_.size() * bin_size;

  DCHECK_LE(pv.Size(), bin_size);
  DCHECK_LE(next_data + bin_size, block_start_ + kBlockLen);

  uint64_t hash = pk.HashCode();
  absl::little_endian::Store64(next_hash, hash);
  pv.GetString(next_data);

  size_t key_size = pk.Size();
  char* end = key_blob_.data() + key_blob_.size();
  DCHECK_LE(next_key_ + key_size, end);

  pk.GetString(next_key_);
  // preserves the order.
  entries_.push_back(string_view{next_key_, key_size});
  next_key_ += key_size;
}

unsigned TieredStorage::InflightWriteRequest::ExternalizeEntries(PerDb::BinRecord* bin_record,
                                                                 DbSlice* db_slice) {
  PrimeTable* pt = db_slice->GetTables(db_index_).first;
  DbTableStats* stats = db_slice->MutableStats(db_index_);
  unsigned externalized = 0;

  unsigned bin_size = kSmallBins[bin_index_];
  unsigned max_entries = NumEntriesInSmallBin(bin_size);
  size_t offset = max_entries * 8;

  for (size_t i = 0; i < entries_.size(); ++i) {
    string_view pkey = entries_[i];
    auto it = bin_record->enqueued_entries.find(pkey);
    if (it != bin_record->enqueued_entries.end() && it->second == this) {
      ++externalized;
    }
  }

  if (externalized <= entries_.size() / 2) {
    Undo(bin_record, db_slice);
    return 0;
  }

  for (size_t i = 0; i < entries_.size(); ++i) {
    string_view pkey = entries_[i];
    auto it = bin_record->enqueued_entries.find(pkey);

    if (it != bin_record->enqueued_entries.end() && it->second == this) {
      PrimeIterator pit = pt->Find(pkey);
      size_t item_offset = page_index_ * 4096 + offset + i * bin_size;

      // TODO: the key may be deleted or overriden. The last one is especially dangerous.
      // we should update active pending request with any change we make to the entry.
      // it should not be a problem since we have HasIoPending tag that mean we must
      // update the inflight request (or mark the entry as cancelled).
      CHECK(!pit.is_done()) << "TBD";

      ExternalizeEntry(item_offset, stats, &pit->second);
      bin_record->enqueued_entries.erase(it);
    }
  }

  return externalized;
}

void TieredStorage::InflightWriteRequest::Undo(PerDb::BinRecord* bin_record, DbSlice* db_slice) {
  PrimeTable* pt = db_slice->GetTables(db_index_).first;
  for (const auto& pkey : entries_) {
    auto it = bin_record->enqueued_entries.find(pkey);
    if (it != bin_record->enqueued_entries.end() && it->second == this) {
      PrimeIterator pit = pt->Find(pkey);

      // TODO: what happens when if the entry was deleted meanwhile
      // or it has been serialized again?
      CHECK(pit->second.HasIoPending()) << "TBD: fix inconsistencies";

      pit->second.SetIoPending(false);

      bin_record->enqueued_entries.erase(it);
    }
  }
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
  stats_.tiered_reads++;
  DVLOG(1) << "Read " << offset << " " << len;

  return io_mgr_.Read(offset, io::MutableBytes{reinterpret_cast<uint8_t*>(dest), len});
}

void TieredStorage::Free(size_t offset, size_t len) {
  if (offset % kBlockLen == 0) {
    alloc_.Free(offset, len);
  } else {
    uint32_t offs_page = offset / kBlockLen;
    auto it = page_refcnt_.find(offs_page);
    CHECK(it != page_refcnt_.end()) << offs_page;
    CHECK_GT(it->second, 0u);

    if (--it->second == 0) {
      alloc_.Free(offs_page * kBlockLen, kBlockLen);
      page_refcnt_.erase(it);
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

void TieredStorage::FinishIoRequest(int io_res, InflightWriteRequest* req) {
  PerDb* db = db_arr_[req->db_index()];
  auto& bin_record = db->bin_map[req->bin_index()];
  if (io_res < 0) {
    LOG(ERROR) << "Error writing into ssd file: " << util::detail::SafeErrorMessage(-io_res);
    alloc_.Free(req->page_index() * kBlockLen, kBlockLen);
    req->Undo(&bin_record, &db_slice_);
    ++stats_.aborted_write_cnt;
  } else {
    // Also removes the entries from bin_record.
    uint16_t entries_serialized = req->ExternalizeEntries(&bin_record, &db_slice_);

    if (entries_serialized == 0) {  // aborted
      ++stats_.aborted_write_cnt;
      alloc_.Free(req->page_index() * kBlockLen, kBlockLen);
    } else {  // succeeded.
      VLOG(2) << "page_refcnt emplace " << req->page_index();
      auto res = page_refcnt_.emplace(req->page_index(), entries_serialized);
      CHECK(res.second);
    }
  }

  delete req;
  --num_active_requests_;
  VLOG_IF(2, num_active_requests_ == 0) << "Finished active requests";
}

error_code TieredStorage::ScheduleOffload(DbIndex db_index, PrimeIterator it) {
  CHECK_EQ(OBJ_STRING, it->second.ObjType());
  DCHECK(!it->second.IsExternal());
  DCHECK(!it->second.HasIoPending());

  // Relevant only for OBJ_STRING, see CHECK above.
  size_t blob_len = it->second.Size();

  if (blob_len > kMaxSmallBin) {
    if (num_active_requests_ < GetFlag(FLAGS_tiered_storage_max_pending_writes)) {
      WriteSingle(db_index, it, blob_len);
    }  // otherwise skip
    return error_code{};
  }

  if (db_arr_.size() <= db_index) {
    db_arr_.resize(db_index + 1);
  }

  if (db_arr_[db_index] == nullptr) {
    db_arr_[db_index] = new PerDb;
  }

  PerDb* db = db_arr_[db_index];

  unsigned bin_index = SmallToBin(blob_len);

  DCHECK_LT(bin_index, kSmallBinLen);

  unsigned max_entries = NumEntriesInSmallBin(kSmallBins[bin_index]);
  auto& bin_record = db->bin_map[bin_index];

  // TODO: we need to track in stats all the cases where we omit offloading attempt.
  CHECK_LT(bin_record.pending_entries.size(), max_entries);

  bin_record.pending_entries.insert(it->first);

  if (bin_record.pending_entries.size() < max_entries)
    return error_code{};  // gather more.

  bool flush_succeeded = false;
  if (num_active_requests_ < GetFlag(FLAGS_tiered_storage_max_pending_writes)) {
    flush_succeeded = FlushPending(db_index, bin_index);

    // if we reached high utilization of the file range - try to grow the file.
    if (alloc_.allocated_bytes() > size_t(alloc_.capacity() * 0.85)) {
      InitiateGrow(1ULL << 28);
    }
  }

  if (!flush_succeeded) {
    // we could not flush because I/O is saturated, so lets remove the last item.
    bin_record.pending_entries.erase(it->first.AsRef());
    ++stats_.flush_skip_cnt;
  }

  return error_code{};
}

void TieredStorage::CancelIo(DbIndex db_index, PrimeIterator it) {
  DCHECK_EQ(OBJ_STRING, it->second.ObjType());

  auto& prime_value = it->second;

  DCHECK(!prime_value.IsExternal());
  DCHECK(prime_value.HasIoPending());

  prime_value.SetIoPending(false);  // remove io flag.
  PerDb* db = db_arr_[db_index];
  size_t blob_len = prime_value.Size();
  unsigned bin_index = SmallToBin(blob_len);
  auto& bin_record = db->bin_map[bin_index];
  auto pending_it = bin_record.pending_entries.find(it->first);
  if (pending_it != bin_record.pending_entries.end()) {
    bin_record.pending_entries.erase(pending_it);
    return;
  }

  string key = it->first.ToString();
  CHECK(bin_record.enqueued_entries.erase(key));
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

  constexpr size_t kMask = kBlockAlignment - 1;
  size_t page_size = (blob_len + kMask) & (~kMask);

  DCHECK_GE(page_size, blob_len);
  DCHECK_EQ(0u, page_size % kBlockAlignment);

  struct SingleRequest {
    char* block_ptr = nullptr;
    PrimeTable* pt = nullptr;
    size_t blob_len = 0;
    off_t offset = 0;
    string key;
  } req;

  char* block_ptr = (char*)mi_malloc_aligned(page_size, kBlockAlignment);

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

bool TieredStorage::FlushPending(DbIndex db_index, unsigned bin_index) {
  PerDb* db = db_arr_[db_index];

  int64_t res = alloc_.Malloc(kBlockLen);
  if (res < 0) {
    InitiateGrow(-res);
    return false;
  }

  DCHECK_EQ(res % kBlockLen, 0u);

  off64_t file_offset = res;
  PrimeTable* pt = db_slice_.GetTables(db_index).first;
  auto& bin_record = db->bin_map[bin_index];

  DCHECK_EQ(bin_record.pending_entries.size(), NumEntriesInSmallBin(kSmallBins[bin_index]));
  DbSlice::Context db_context{db_index, GetCurrentTimeMs()};

  DCHECK_LT(bin_record.pending_entries.size(), 60u);

  InflightWriteRequest* req = new InflightWriteRequest(db_index, bin_index, res / kBlockLen);

  size_t keys_size = 0;
  for (auto key_view : bin_record.pending_entries) {
    keys_size += key_view->Size();
  }
  req->SetKeyBlob(keys_size);

  for (auto key_view : bin_record.pending_entries) {
    PrimeIterator it = pt->Find(key_view);
    DCHECK(IsValid(it));
    if (it->second.HasExpire()) {
      auto [pit, exp_it] = db_slice_.ExpireIfNeeded(db_context, it);
      CHECK(!pit.is_done()) << "TBD: should abort in case of expired keys";
    }

    req->Add(it->first, it->second);
    it->second.SetIoPending(true);

    auto res = bin_record.enqueued_entries.emplace(req->entries().back(), req);
    CHECK(res.second);
  }

  auto cb = [this, req](int io_res) { this->FinishIoRequest(io_res, req); };

  ++num_active_requests_;
  io_mgr_.WriteAsync(file_offset, req->block(), move(cb));
  ++stats_.tiered_writes;

  bin_record.pending_entries.clear();

  return true;
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
