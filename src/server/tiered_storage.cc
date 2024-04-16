// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiered_storage.h"

#include <mimalloc.h>

#include <memory>
#include <optional>
#include <variant>

#include "absl/cleanup/cleanup.h"
#include "absl/flags/internal/flag.h"
#include "base/flags.h"
#include "base/logging.h"
#include "server/common.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/table.h"
#include "server/tiering/common.h"
#include "server/tiering/op_manager.h"
#include "server/tiering/small_bins.h"

ABSL_FLAG(uint32_t, tiered_storage_max_pending_writes, 32,
          "Maximal number of pending writes per thread");
ABSL_FLAG(uint32_t, tiered_storage_throttle_us, 1,
          "Slow down tiered storage writes for at most this usec in case of I/O saturation "
          "specified by tiered_storage_max_pending_writes. 0 - do not throttle.");

ABSL_FLAG(bool, tiered_storage_v2_cache_fetched, true,
          "WIP: Load results of offloaded reads to memory");

namespace dfly {

using namespace std;
using namespace util;
using absl::GetFlag;

using namespace tiering::literals;

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

  stats->AddTypeMemoryUsage(entry->ObjType(), -heap_size);

  entry->SetExternal(item_offset, item_size);

  stats->tiered_entries += 1;
  stats->tiered_size += item_size;

  return item_size;
}

struct SingleRequest {
  SingleRequest(size_t blob_len, int64 offset, string key)
      : blob_len(blob_len), offset(offset), key(std::move(key)) {
    constexpr size_t kMask = kBlockAlignment - 1;
    page_size = (blob_len + kMask) & (~kMask);
    DCHECK_GE(page_size, blob_len);
    DCHECK_EQ(0u, page_size % kBlockAlignment);
    block_ptr = (char*)mi_malloc_aligned(page_size, kBlockAlignment);
  }
  char* block_ptr;
  size_t blob_len;
  size_t page_size;
  off_t offset;
  string key;
  bool cancel = false;
};

struct TieredStorage::PerDb {
  PerDb(const PerDb&) = delete;
  PerDb& operator=(const PerDb&) = delete;
  PerDb() = default;
  void CancelAll();

  using InflightMap = absl::flat_hash_map<string_view, InflightWriteRequest*>;

  struct BinRecord {
    // Those that wait to be serialized. Must be less than NumEntriesInSmallBin for each bin.
    absl::flat_hash_set<CompactObjectView> pending_entries;

    // Entries that were scheduled to write but have not completed yet.
    InflightMap enqueued_entries;
  };
  // Big bin entries that were scheduled to write but have not completed yet.
  absl::flat_hash_map<string_view, SingleRequest*> bigbin_enqueued_entries;

  BinRecord bin_map[kSmallBinLen];
};

void TieredStorage::PerDb::CancelAll() {
  for (size_t i = 0; i < kSmallBinLen; ++i) {
    bin_map[i].pending_entries.clear();
    // It is safe to clear enqueued_entries, because when we will finish writing to disk
    // InflightWriteRequest::ExternalizeEntries will be executed and it will undo the externalize of
    // the entries and free the allocated page.
    bin_map[i].enqueued_entries.clear();
  }
  for (auto& req : bigbin_enqueued_entries) {
    req.second->cancel = true;
  }
  bigbin_enqueued_entries.clear();
}

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

  char* next_hash = block_start_ + entries_.size() * 8;
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
      size_t item_offset = size_t(page_index_) * 4096 + offset + i * bin_size;
      CHECK(!pit.is_done());

      ExternalizeEntry(item_offset, stats, &pit->second);
      VLOG(2) << "ExternalizeEntry: " << it->first;
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

      CHECK(pit->second.HasIoPending());
      VLOG(2) << "Undo key:" << pkey;
      pit->second.SetIoPending(false);

      bin_record->enqueued_entries.erase(it);
    }
  }
}

TieredStorage::TieredStorage(DbSlice* db_slice, size_t max_file_size)
    : db_slice_(*db_slice), max_file_size_(max_file_size) {
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
    size_t initial_size = io_mgr_.Span();
    if (initial_size) {  // Add initial storage.
      allocated_size_ += initial_size;
      alloc_.AddStorage(0, initial_size);
    }
  }
  return ec;
}

std::error_code TieredStorage::Read(size_t offset, size_t len, char* dest) {
  DVLOG(1) << "Read " << offset << " " << len;

  return io_mgr_.Read(offset, io::MutableBytes{reinterpret_cast<uint8_t*>(dest), len});
}

void TieredStorage::Free(PrimeIterator it, DbTableStats* stats) {
  PrimeValue& entry = it->second;
  CHECK(entry.IsExternal());
  DCHECK_EQ(entry.ObjType(), OBJ_STRING);
  auto [offset, len] = entry.GetExternalSlice();

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

  bool has_expire = entry.HasExpire();
  entry.Reset();
  entry.SetExpire(has_expire);  // we keep expire data

  stats->tiered_entries -= 1;
  stats->tiered_size -= len;
}

void TieredStorage::Shutdown() {
  VLOG(1) << "Shutdown TieredStorage";
  shutdown_ = true;
  io_mgr_.Shutdown();
}

TieredStats TieredStorage::GetStats() const {
  TieredStats res = stats_;
  res.storage_capacity = alloc_.capacity();
  res.storage_reserved = alloc_.allocated_bytes();

  return res;
}

void TieredStorage::FinishIoRequest(int io_res, InflightWriteRequest* req) {
  if (shutdown_) {
    return;
  }
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
  if (IoDeviceUnderloaded()) {
    this->throttle_ec_.notifyAll();
  }
  VLOG_IF(2, num_active_requests_ == 0) << "Finished active requests";
}

PrimeIterator TieredStorage::Load(DbIndex db_index, PrimeIterator it, string_view key) {
  PrimeValue* entry = &it->second;
  CHECK(entry->IsExternal());
  DCHECK_EQ(entry->ObjType(), OBJ_STRING);
  auto [offset, size] = entry->GetExternalSlice();
  string res(size, '\0');
  auto ec = Read(offset, size, res.data());
  CHECK(!ec) << "TBD";

  // Read will preempt, update iterator if needed.
  DbTable* table = db_slice_.GetDBTable(db_index);
  it = table->Launder(it, key);
  if (it.is_done()) {
    // Entry was remove from db while reading from disk. (background expire task)
    return it;
  }
  entry = &it->second;

  if (!entry->IsExternal()) {
    // Because 2 reads can happen at the same time, then if the other read
    // already loaded the data from disk to memory we don't need to do anything now just return.
    // TODO we can register to reads with multiple callbacks so if there is already a callback
    // reading the data from disk we will not run read twice.
    return it;
  }

  auto* stats = db_slice_.MutableStats(db_index);
  Free(it, stats);
  entry->SetString(res);

  size_t heap_size = entry->MallocUsed();
  stats->AddTypeMemoryUsage(entry->ObjType(), heap_size);
  return it;
}

bool TieredStorage::PrepareForOffload(DbIndex db_index, PrimeIterator it) {
  CHECK_EQ(OBJ_STRING, it->second.ObjType());
  DCHECK(!it->second.IsExternal());
  DCHECK(!it->second.HasIoPending());

  // Relevant only for OBJ_STRING, see CHECK above.
  size_t blob_len = it->second.Size();

  if (db_arr_.size() <= db_index) {
    db_arr_.resize(db_index + 1);
  }
  if (db_arr_[db_index] == nullptr) {
    db_arr_[db_index] = new PerDb;
  }

  if (blob_len > kMaxSmallBin) {
    return true;
  }

  PerDb* db = db_arr_[db_index];

  unsigned bin_index = SmallToBin(blob_len);

  DCHECK_LT(bin_index, kSmallBinLen);

  unsigned max_entries = NumEntriesInSmallBin(kSmallBins[bin_index]);
  auto& bin_record = db->bin_map[bin_index];

  if (bin_record.pending_entries.size() == max_entries) {
    // This bin is full and was not offloaded yet, can not set this entry for offload.
    return false;
  }

  VLOG(2) << "ScheduleOffload:" << it->first.ToString();
  bin_record.pending_entries.insert(it->first.AsRef());
  it->second.SetIoPending(true);

  if (bin_record.pending_entries.size() == max_entries) {
    return true;
  }
  return false;  // Gather more entries for bin before offload.
}

void TieredStorage::CancelOffload(DbIndex db_index, PrimeIterator it) {
  size_t blob_len = it->second.Size();
  if (blob_len > kMaxSmallBin) {
    return;
  }
  PerDb* db = db_arr_[db_index];
  unsigned bin_index = SmallToBin(blob_len);
  auto& bin_record = db->bin_map[bin_index];
  bin_record.pending_entries.erase(it->first.AsRef());
  it->second.SetIoPending(false);
  ++stats_.flush_skip_cnt;
}

error_code TieredStorage::ScheduleOffloadWithThrottle(DbIndex db_index, PrimeIterator it,
                                                      string_view key) {
  bool schedule_offload = PrepareForOffload(db_index, it);
  if (!schedule_offload) {
    return error_code{};
  }
  auto [schedule, res_it] = ThrottleWrites(db_index, it, key);
  if (schedule) {
    return ScheduleOffloadInternal(db_index, res_it);
  } else {
    CancelOffload(db_index, res_it);
  }
  return error_code{};
}

error_code TieredStorage::ScheduleOffload(DbIndex db_index, PrimeIterator it) {
  bool schedule_offload = PrepareForOffload(db_index, it);
  if (!schedule_offload) {
    return error_code{};
  }
  if (IoDeviceUnderloaded()) {
    return ScheduleOffloadInternal(db_index, it);
  } else {
    CancelOffload(db_index, it);
  }
  return error_code{};
}

error_code TieredStorage::ScheduleOffloadInternal(DbIndex db_index, PrimeIterator it) {
  size_t blob_len = it->second.Size();

  if (blob_len > kMaxSmallBin) {
    WriteSingle(db_index, it, blob_len);
    return error_code{};
  }

  unsigned bin_index = SmallToBin(blob_len);
  bool flashed = FlushPending(db_index, bin_index);
  if (!flashed) {
    CancelOffload(db_index, it);
  }

  // if we reached high utilization of the file range - try to grow the file.
  if (alloc_.allocated_bytes() > size_t(alloc_.capacity() * 0.85)) {
    InitiateGrow(1ULL << 28);
  }

  return error_code{};
}

void TieredStorage::CancelIo(DbIndex db_index, PrimeIterator it) {
  DCHECK_EQ(OBJ_STRING, it->second.ObjType());
  VLOG(2) << "CancelIo: " << it->first.ToString();
  auto& prime_value = it->second;

  DCHECK(!prime_value.IsExternal());
  DCHECK(prime_value.HasIoPending());

  prime_value.SetIoPending(false);  // remove io flag.

  size_t blob_len = prime_value.Size();
  PerDb* db = db_arr_[db_index];
  if (blob_len > kMaxSmallBin) {
    string key = it->first.ToString();
    auto& enqueued_entries = db->bigbin_enqueued_entries;
    auto entry_it = enqueued_entries.find(key);
    CHECK(entry_it != enqueued_entries.end());
    entry_it->second->cancel = true;
    CHECK(enqueued_entries.erase(key));
    return;
  }

  unsigned bin_index = SmallToBin(blob_len);
  auto& bin_record = db->bin_map[bin_index];
  auto pending_it = bin_record.pending_entries.find(it->first);
  if (pending_it != bin_record.pending_entries.end()) {
    VLOG(2) << "CancelIo from pending: " << it->first.ToString();
    bin_record.pending_entries.erase(pending_it);
    return;
  }

  string key = it->first.ToString();
  VLOG(2) << "CancelIo from enqueue: " << key;
  CHECK(bin_record.enqueued_entries.erase(key));
}

void TieredStorage::CancelAllIos(DbIndex db_index) {
  VLOG(2) << "CancelAllIos " << db_index;
  if (db_index >= db_arr_.size()) {
    return;
  }
  PerDb* db = db_arr_[db_index];
  if (db) {
    VLOG(2) << "Clear db " << db_index;
    db->CancelAll();
  }
}

bool IsObjFitToUnload(const PrimeValue& pv) {
  return pv.ObjType() == OBJ_STRING && !pv.IsExternal() && pv.Size() >= 64 && !pv.HasIoPending();
};

void TieredStorage::WriteSingle(DbIndex db_index, PrimeIterator it, size_t blob_len) {
  VLOG(2) << "WriteSingle " << blob_len;
  DCHECK(!it->second.HasIoPending());

  int64_t res = alloc_.Malloc(blob_len);
  if (res < 0) {
    InitiateGrow(-res);
    return;
  }

  SingleRequest* req = new SingleRequest(blob_len, res, it->first.ToString());

  auto& enqueued_entries = db_arr_[db_index]->bigbin_enqueued_entries;
  auto emplace_res = enqueued_entries.emplace(req->key, req);
  CHECK(emplace_res.second);

  it->second.GetString(req->block_ptr);
  it->second.SetIoPending(true);

  auto cb = [this, req, db_index](int io_res) {
    if (shutdown_) {
      return;
    }
    PrimeTable* pt = db_slice_.GetTables(db_index).first;

    absl::Cleanup cleanup = [this, req]() {
      mi_free(req->block_ptr);
      delete req;
      --num_active_requests_;
      if (IoDeviceUnderloaded()) {
        this->throttle_ec_.notifyAll();
      }
    };

    // In case entry was canceled free allocated.
    if (req->cancel) {
      alloc_.Free(req->offset, req->blob_len);
      return;
    }

    PrimeIterator it = pt->Find(req->key);
    CHECK(!it.is_done());
    CHECK(it->second.HasIoPending());

    auto& enqueued_entries = db_arr_[db_index]->bigbin_enqueued_entries;
    auto req_it = enqueued_entries.find(req->key);
    CHECK(req_it != enqueued_entries.end());
    CHECK_EQ(req_it->second, req);

    if (io_res < 0) {
      LOG(ERROR) << "Error writing to ssd storage " << util::detail::SafeErrorMessage(-io_res);
      it->second.SetIoPending(false);
      alloc_.Free(req->offset, req->blob_len);
      enqueued_entries.erase(req->key);
      return;
    }

    enqueued_entries.erase(req->key);
    ExternalizeEntry(req->offset, db_slice_.MutableStats(db_index), &it->second);
    VLOG_IF(2, num_active_requests_ == 0) << "Finished active requests";
  };
  ++num_active_requests_;

  io_mgr_.WriteAsync(res, string_view{req->block_ptr, req->page_size}, std::move(cb));
  ++stats_.tiered_writes;
}

std::pair<bool, PrimeIterator> TieredStorage::ThrottleWrites(DbIndex db_index, PrimeIterator it,
                                                             string_view key) {
  unsigned throttle_usec = GetFlag(FLAGS_tiered_storage_throttle_us);
  PrimeIterator res_it = it;
  if (!IoDeviceUnderloaded() && throttle_usec > 0) {
    chrono::steady_clock::time_point next =
        chrono::steady_clock::now() + chrono::microseconds(throttle_usec);
    stats_.throttled_write_cnt++;

    throttle_ec_.await_until([&]() { return IoDeviceUnderloaded(); }, next);

    PrimeTable* pt = db_slice_.GetTables(db_index).first;
    if (!it.IsOccupied() || it->first != key) {
      res_it = pt->Find(key);
      // During the database write flow, when offloading a value, we acquire a write lock on the
      // key. No other operations are allowed to modify or remove the key until the lock is
      // released.
      CHECK(!res_it.is_done());
      VLOG(1) << "Update iterator after await";
    }
  }

  return std::make_pair(IoDeviceUnderloaded(), res_it);
}

bool TieredStorage::IoDeviceUnderloaded() const {
  return num_active_requests_ < GetFlag(FLAGS_tiered_storage_max_pending_writes);
}

bool TieredStorage::FlushPending(DbIndex db_index, unsigned bin_index) {
  PerDb* db = db_arr_[db_index];

  int64_t res = alloc_.Malloc(kBlockLen);
  VLOG(2) << "FlushPending Malloc:" << res;
  if (res < 0) {
    InitiateGrow(-res);
    return false;
  }

  DCHECK_EQ(res % kBlockLen, 0u);

  int64_t file_offset = res;
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
      auto [pit, exp_it] = db_slice_.ExpireIfNeeded(db_context, DbSlice::Iterator::FromPrime(it));
      CHECK(!pit.is_done()) << "TBD: should abort in case of expired keys";
    }

    req->Add(it->first, it->second);
    VLOG(2) << "add to enqueued_entries: " << req->entries().back();
    auto res = bin_record.enqueued_entries.emplace(req->entries().back(), req);
    CHECK(res.second);
  }

  auto cb = [this, req](int io_res) { this->FinishIoRequest(io_res, req); };

  ++num_active_requests_;
  io_mgr_.WriteAsync(file_offset, req->block(), std::move(cb));
  ++stats_.tiered_writes;

  bin_record.pending_entries.clear();

  return true;
}

void TieredStorage::InitiateGrow(size_t grow_size) {
  if (io_mgr_.grow_pending() || allocated_size_ + grow_size > max_file_size_)
    return;
  DCHECK_GT(grow_size, 0u);

  size_t start = io_mgr_.Span();

  auto cb = [start, grow_size, this](int io_res) {
    if (io_res == 0) {
      alloc_.AddStorage(start, grow_size);
      allocated_size_ += grow_size;
    } else {
      LOG_FIRST_N(ERROR, 10) << "Error enlarging storage " << io_res;
    }
  };

  error_code ec = io_mgr_.GrowAsync(grow_size, std::move(cb));
  CHECK(!ec) << "TBD";  // TODO
}

bool TieredStorage::CanExternalizeEntry(PrimeIterator it) {
  return it->first.ObjType() == OBJ_STRING && !it->second.HasIoPending() &&
         !it->second.IsExternal() && EligibleForOffload(it->second.Size());
}

class TieredStorageV2::ShardOpManager : public tiering::OpManager {
  friend class TieredStorageV2;

 public:
  ShardOpManager(TieredStorageV2* ts, DbSlice* db_slice) : ts_{ts}, db_slice_{db_slice} {
    cache_fetched_ = absl::GetFlag(FLAGS_tiered_storage_v2_cache_fetched);
  }

  // Find entry by key in db_slice and store external segment in place of original value
  void SetExternal(std::string_view key, tiering::DiskSegment segment) {
    if (auto pv = Find(key); pv) {
      pv->SetIoPending(false);
      pv->SetExternal(segment.offset, segment.length);  // TODO: Handle memory stats

      stats_.total_stashes++;
    }
  }

  void ClearIoPending(std::string_view key) {
    if (auto pv = Find(key); pv)
      pv->SetIoPending(false);
  }

  // Find entry by key and store it's up-to-date value in place of external segment
  void SetInMemory(std::string_view key, std::string_view value) {
    if (auto pv = Find(key); pv) {
      pv->Reset();  // TODO: account for memory
      pv->SetString(value);

      stats_.total_fetches++;
    }
  }

  void ReportStashed(EntryId id, tiering::DiskSegment segment) override {
    if (holds_alternative<string_view>(id)) {
      SetExternal(get<string_view>(id), segment);
    } else {
      for (const auto& [sub_key, sub_segment] :
           ts_->bins_->ReportStashed(get<tiering::SmallBins::BinId>(id), segment))
        SetExternal(string_view{sub_key}, sub_segment);
    }
  }

  void ReportFetched(EntryId id, std::string_view value, tiering::DiskSegment segment) override {
    DCHECK(holds_alternative<string_view>(id));  // we never issue reads for bins

    if (!cache_fetched_)
      return;

    SetInMemory(get<string_view>(id), value);

    // Delete value
    if (segment.length >= TieredStorageV2::kMinValueSize) {
      Delete(segment);
    } else {
      if (auto bin_segment = ts_->bins_->Delete(segment); bin_segment)
        Delete(*bin_segment);
    }
  }

  TieredStatsV2 GetStats() const {
    auto stats = stats_;
    stats.allocated_bytes = OpManager::storage_.GetStats().allocated_bytes;
    return stats;
  }

 private:
  PrimeValue* Find(std::string_view key) {
    // TODO: Get DbContext for transaction for correct dbid and time
    auto it = db_slice_->FindMutable(DbContext{}, key);
    return IsValid(it.it) ? &it.it->second : nullptr;
  }

  bool cache_fetched_ = false;

  TieredStatsV2 stats_;

  TieredStorageV2* ts_;
  DbSlice* db_slice_;
};

TieredStorageV2::TieredStorageV2(DbSlice* db_slice)
    : op_manager_{make_unique<ShardOpManager>(this, db_slice)},
      bins_{make_unique<tiering::SmallBins>()} {
}

TieredStorageV2::~TieredStorageV2() {
}

std::error_code TieredStorageV2::Open(string_view path) {
  return op_manager_->Open(path);
}

void TieredStorageV2::Close() {
  op_manager_->Close();
}

util::fb2::Future<std::string> TieredStorageV2::Read(string_view key, const PrimeValue& value) {
  DCHECK(value.IsExternal());
  return op_manager_->Read(key, value.GetExternalSlice());
}

void TieredStorageV2::Stash(string_view key, PrimeValue* value) {
  string buf;
  string_view value_sv = value->GetSlice(&buf);
  value->SetIoPending(true);

  if (value->Size() >= kMinValueSize) {
    if (auto ec = op_manager_->Stash(key, value_sv); ec)
      value->SetIoPending(false);
  } else if (auto bin = bins_->Stash(key, value_sv); bin) {
    if (auto ec = op_manager_->Stash(bin->first, bin->second); ec) {
      for (const string& key : bins_->ReportStashAborted(bin->first))
        op_manager_->ClearIoPending(key);  // clear IO_PENDING flag
    }
  }
}
void TieredStorageV2::Delete(string_view key, PrimeValue* value) {
  if (value->IsExternal()) {
    tiering::DiskSegment segment = value->GetExternalSlice();
    if (segment.length >= kMinValueSize) {
      op_manager_->Delete(segment);
    } else if (auto bin = bins_->Delete(segment); bin) {
      op_manager_->Delete(*bin);
    }
  } else {
    if (value->Size() >= kMinValueSize) {
      op_manager_->Delete(key);
    } else if (auto bin = bins_->Delete(key); bin) {
      op_manager_->Delete(*bin);
    }
  }
}

TieredStatsV2 TieredStorageV2::GetStats() const {
  return op_manager_->GetStats();
}

}  // namespace dfly
