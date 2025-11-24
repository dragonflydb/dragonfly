// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/engine_shard.h"

#include <absl/strings/escaping.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>

#include <memory>

#include "base/flags.h"
#include "core/huff_coder.h"
#include "core/page_usage_stats.h"
#include "io/proc_reader.h"

extern "C" {
#include "redis/zmalloc.h"
}
#include "server/engine_shard_set.h"
#include "server/journal/journal.h"
#include "server/namespaces.h"
#include "server/search/doc_index.h"
#include "server/server_state.h"
#include "server/tiered_storage.h"
#include "server/transaction.h"
#include "util/fibers/proactor_base.h"

using namespace std;

ABSL_FLAG(float, mem_defrag_threshold, 0.7,
          "Minimum percentage of used memory relative to maxmemory cap before running "
          "defragmentation");

ABSL_FLAG(uint32_t, mem_defrag_check_sec_interval, 60,
          "Number of seconds between every defragmentation necessity check");

ABSL_FLAG(float, mem_defrag_waste_threshold, 0.2,
          "The ratio of wasted/committed memory above which we run defragmentation");

ABSL_FLAG(float, mem_defrag_page_utilization_threshold, 0.8,
          "memory page under utilization threshold. Ratio between used and committed size, below "
          "this, memory in this page will defragmented");

ABSL_FLAG(int32_t, hz, 100,
          "Base frequency at which the server performs other background tasks. "
          "Warning: not advised to decrease in production.");

ABSL_FLAG(string, tiered_prefix, "",
          "Enables tiered storage if set. "
          "The string denotes the path and prefix of the files "
          " associated with tiered storage. Stronly advised to use "
          "high performance NVME ssd disks for this. Also, seems that pipeline_squash does "
          "not work well with tiered storage, so it's advised to set it to 0.");

ABSL_FLAG(bool, enable_heartbeat_eviction, true,
          "Enable eviction during heartbeat when memory is under pressure.");
ABSL_FLAG(bool, enable_heartbeat_rss_eviction, true,
          "Enable eviction during heartbeat when rss memory is under pressure. Eviction based "
          "on used_memory will still be enabled.");
ABSL_FLAG(double, eviction_memory_budget_threshold, 0.1,
          "Eviction starts when the free memory (including RSS memory) drops below "
          "eviction_memory_budget_threshold * max_memory_limit.");
ABSL_FLAG(bool, background_heartbeat, false, "Whether to run heartbeat as a background fiber");
ABSL_DECLARE_FLAG(uint32_t, max_eviction_per_heartbeat);

namespace dfly {

using absl::GetFlag;
using namespace util;

namespace {

constexpr uint64_t kCursorDoneState = 0u;

bool HasContendedLocks(ShardId shard_id, Transaction* trx, const DbTable* table) {
  auto is_contended = [table](LockFp fp) { return table->trans_locks.Find(fp)->IsContended(); };

  if (trx->IsMulti()) {
    auto fps = trx->GetMultiFps();
    for (const auto& [sid, fp] : fps) {
      if (sid == shard_id && is_contended(fp))
        return true;
    }
  } else {
    KeyLockArgs lock_args = trx->GetLockArgs(shard_id);
    for (size_t i = 0; i < lock_args.fps.size(); ++i) {
      if (is_contended(lock_args.fps[i]))
        return true;
    }
  }

  return false;
}

constexpr size_t kQueueLen = 64;

optional<uint32_t> GetPeriodicCycleMs() {
  int hz = GetFlag(FLAGS_hz);
  if (hz <= 0)
    return nullopt;

  uint32_t clock_cycle_ms = 1000 / hz;
  if (clock_cycle_ms == 0)
    clock_cycle_ms = 1;
  return clock_cycle_ms;
}

size_t CalculateHowManyBytesToEvictOnShard(size_t global_memory_limit, size_t global_used_memory,
                                           size_t shard_memory_threshold) {
  if (global_used_memory > global_memory_limit) {
    // Used memory is above the limit, we need to evict all bytes
    return (global_used_memory - global_memory_limit) / shard_set->size() + shard_memory_threshold;
  }

  const size_t shard_budget = (global_memory_limit - global_used_memory) / shard_set->size();
  return shard_budget < shard_memory_threshold ? (shard_memory_threshold - shard_budget) : 0;
}

// Background task that analyzes key patterns to build Huffman encoding tables for compression.
// Runs once on shard 0, incrementally scanning keys with a cursor to build character frequency
// histograms. Returns -1 when complete, or 4 to continue with low priority on next idle cycle.
class HuffmanCheckTask {
 public:
  HuffmanCheckTask() {
    hist_.fill(0);
  }

  int32_t Run(DbSlice* db_slice);

 private:
  PrimeTable::Cursor cursor_;

  static constexpr unsigned kMaxSymbol = 255;
  array<unsigned, kMaxSymbol + 1> hist_;  // histogram of symbols.
  string scratch_;
};

int32_t HuffmanCheckTask::Run(DbSlice* db_slice) {
  DbTable* db_table = db_slice->GetDBTable(0);  // we currently support only default db.
  if (!db_table)
    return -1;

  // incrementally aggregate frequency histogram.
  auto& prime = db_table->prime;

  constexpr uint32_t kMaxTraverses = 512;
  uint32_t traverses_count = 0;
  do {
    cursor_ = prime.Traverse(cursor_, [&](PrimeIterator it) {
      if (!it->first.IsInline()) {
        string_view val = it->first.GetSlice(&scratch_);
        for (unsigned char c : val) {
          hist_[c]++;
        }
      }
    });
    traverses_count++;
  } while (traverses_count < kMaxTraverses && cursor_);

  if (cursor_)
    return 4;  // priority to continue later.

  // Finished scanning the table, now normalize the table.
  constexpr unsigned kMaxFreqTotal = static_cast<unsigned>((1U << 31) * 0.9);
  size_t total_freq = std::accumulate(hist_.begin(), hist_.end(), 0UL);

  // to avoid overflow.
  double scale = total_freq > kMaxFreqTotal ? static_cast<double>(total_freq) / kMaxFreqTotal : 1.0;
  for (unsigned i = 0; i <= kMaxSymbol; i++) {
    hist_[i] = static_cast<unsigned>(hist_[i] / scale);
    if (hist_[i] == 0) {
      hist_[i] = 1;  // Avoid zero frequency symbols.
    }
  }

  // Build the huffman table. We currently output the table to logs and just increase
  // the metric counter to signal that we built a table.

  HuffmanEncoder huff_enc;
  string error_msg;
  if (huff_enc.Build(hist_.data(), kMaxSymbol, &error_msg)) {
    size_t compressed_size = huff_enc.EstimateCompressedSize(hist_.data(), kMaxSymbol);
    LOG(INFO) << "Huffman table built, reducing memory usage from " << total_freq << " to "
              << compressed_size << " bytes, ratio " << double(compressed_size) / total_freq;
    string bintable = huff_enc.Export();
    LOG(INFO) << "Huffman binary table: " << absl::Base64Escape(bintable);
    db_slice->shard_owner()->stats().huffman_tables_built++;
  } else {
    LOG(WARNING) << "Huffman build failed: " << error_msg;
  }

  return -1;  // task completed.
}

}  // namespace

__thread EngineShard* EngineShard::shard_ = nullptr;
uint64_t TEST_current_time_ms = 0;

string EngineShard::TxQueueInfo::Format() const {
  string res;

  if (tx_total > 0) {
    absl::StrAppend(&res, "tx armed ", tx_armed, ", total: ", tx_total, ",global:", tx_global,
                    ",runnable:", tx_runnable, "\n");
    absl::StrAppend(&res, ", head: ", head.debug_id_info, "\n");
  }
  if (total_locks > 0) {
    absl::StrAppend(&res, "locks total:", total_locks, ",contended:", contended_locks, "\n");
  }
  if (max_contention_score > 0) {
    absl::StrAppend(&res, "max contention score: ", max_contention_score,
                    ", lock: ", max_contention_lock, "\n");
  }

  return res;
}

EngineShard::Stats& EngineShard::Stats::operator+=(const EngineShard::Stats& o) {
  static_assert(sizeof(Stats) == 104);

#define ADD(x) x += o.x

  ADD(defrag_attempt_total);
  ADD(defrag_realloc_total);
  ADD(defrag_task_invocation_total);
  ADD(poll_execution_total);
  ADD(tx_ooo_total);
  ADD(tx_optimistic_total);
  ADD(tx_batch_schedule_calls_total);
  ADD(tx_batch_scheduled_items_total);
  ADD(total_heartbeat_expired_keys);
  ADD(total_heartbeat_expired_bytes);
  ADD(total_heartbeat_expired_calls);
  ADD(total_migrated_keys);
  ADD(huffman_tables_built);

#undef ADD
  return *this;
}

void EngineShard::DefragTaskState::UpdateScanState(uint64_t cursor_val) {
  cursor = cursor_val;
  // Once we're done with a db, jump to the next
  if (cursor == kCursorDoneState) {
    dbid++;
  }
}

void EngineShard::DefragTaskState::ResetScanState() {
  dbid = cursor = 0u;
}

// This function checks 3 things:
// 1. Don't try memory fragmentation if we don't use "enough" memory (control by
// mem_defrag_threshold flag)
// 2. We have memory blocks that can be better utilized (there is a "wasted memory" in them).
// 3. in case the above is OK, make sure that we have a "gap" between usage and commited memory
// (control by mem_defrag_waste_threshold flag)
bool EngineShard::DefragTaskState::CheckRequired() {
  if (cursor > kCursorDoneState) {
    VLOG(2) << "cursor: " << cursor;
    return true;
  }

  size_t limit = max_memory_limit.load(memory_order_relaxed);

  const std::size_t memory_per_shard = limit / shard_set->size();
  if (memory_per_shard < (1 << 16)) {  // Too small.
    return false;
  }

  thread_local fragmentation_info finfo{
      .committed = 0, .committed_golden = 0, .wasted = 0, .bin = 0};

  const std::size_t global_threshold = double(limit) * GetFlag(FLAGS_mem_defrag_threshold);
  if (global_threshold > rss_mem_current.load(memory_order_relaxed)) {
    finfo.bin = 0;  // reset.
    return false;
  }

  if (finfo.bin == 0) {  // did not start the iterative checking yet
    const auto now = time(nullptr);
    const auto seconds_from_prev_check = now - last_check_time;
    const auto mem_defrag_interval = GetFlag(FLAGS_mem_defrag_check_sec_interval);

    if (seconds_from_prev_check < mem_defrag_interval) {
      return false;
    }

    // start checking.
    finfo.committed = finfo.committed_golden = 0;
    finfo.wasted = 0;
    page_utilization_threshold = GetFlag(FLAGS_mem_defrag_page_utilization_threshold);
  }

  uint64_t start = absl::GetCurrentTimeNanos();
  int res = zmalloc_get_allocator_fragmentation_step(page_utilization_threshold, &finfo);
  uint64_t duration = absl::GetCurrentTimeNanos() - start;
  VLOG(1) << "Reading memory usage took " << duration << " ns on bin " << finfo.bin - 1;

  if (res == 0) {
    // finished checking.
    last_check_time = time(nullptr);

    const double waste_threshold = GetFlag(FLAGS_mem_defrag_waste_threshold);
    if (finfo.wasted > size_t(finfo.committed * waste_threshold)) {
      VLOG(1) << "memory fragmentation issue found: " << finfo.wasted << " " << finfo.committed;
      return true;
    }
  }

  return false;
}

std::optional<CollectedPageStats> EngineShard::DoDefrag(CollectPageStats collect_page_stats,
                                                        const float threshold) {
  // --------------------------------------------------------------------------
  // NOTE: This task is running with exclusive access to the shard.
  // i.e. - Since we are using shared nothing access here, and all access
  // are done using fibers, This fiber is run only when no other fiber in the
  // context of the controlling thread will access this shard!
  // --------------------------------------------------------------------------

  constexpr size_t kMaxTraverses = 40;

  // TODO: enable tiered storage on non-default db slice
  DbSlice& slice = namespaces->GetDefaultNamespace().GetDbSlice(shard_->shard_id());

  // If we moved to an invalid db, skip as long as it's not the last one
  while (!slice.IsDbValid(defrag_state_.dbid) && defrag_state_.dbid + 1 < slice.db_array_size())
    defrag_state_.dbid++;

  // If we found no valid db, we finished traversing and start from scratch next time
  if (!slice.IsDbValid(defrag_state_.dbid)) {
    defrag_state_.ResetScanState();
    return std::nullopt;
  }

  DCHECK(slice.IsDbValid(defrag_state_.dbid));
  auto [prime_table, expire_table] = slice.GetTables(defrag_state_.dbid);
  PrimeTable::Cursor cur{defrag_state_.cursor};
  uint64_t reallocations = 0;
  unsigned traverses_count = 0;
  uint64_t attempts = 0;

  PageUsage page_usage{collect_page_stats, threshold};
  do {
    cur = prime_table->Traverse(cur, [&](PrimeIterator it) {
      // for each value check whether we should move it because it
      // seats on underutilized page of memory, and if so, do it.
      bool did = it->second.DefragIfNeeded(&page_usage);
      attempts++;
      if (did) {
        reallocations++;
      }
    });
    traverses_count++;
  } while (traverses_count < kMaxTraverses && cur && namespaces);

  defrag_state_.UpdateScanState(cur.token());

  if (reallocations > 0) {
    VLOG(2) << "shard " << slice.shard_id() << ": successfully defrag  " << reallocations
            << " times, did it in " << traverses_count << " cursor is at the "
            << (defrag_state_.cursor == kCursorDoneState ? "end" : "in progress");
  } else {
    VLOG(2) << "shard " << slice.shard_id() << ": run the defrag " << traverses_count
            << " times out of maximum " << kMaxTraverses << ", with cursor at "
            << (defrag_state_.cursor == kCursorDoneState ? "end" : "in progress")
            << " but no location for defrag were found";
  }

  stats_.defrag_realloc_total += reallocations;
  stats_.defrag_task_invocation_total++;
  stats_.defrag_attempt_total += attempts;

  return page_usage.CollectedStats();
}

// the memory defragmentation task is as follow:
//  1. Check if memory usage is high enough
//  2. Check if diff between commited and used memory is high enough
//  3. if all the above pass -> scan this shard and try to find whether we can move pointer to
//  underutilized pages values
//     if the cursor returned from scan is not in done state, schedule the task to run at high
//     priority.
//     otherwise lower the task priority so that it would not use the CPU when not required
uint32_t EngineShard::DefragTask() {
  constexpr uint32_t kRunAtLowPriority = 0u;
  if (!namespaces) {
    return kRunAtLowPriority;
  }

  if (defrag_state_.CheckRequired()) {
    VLOG(2) << shard_id_ << ": need to run defrag memory cursor state: " << defrag_state_.cursor;
    static const float threshold = GetFlag(FLAGS_mem_defrag_page_utilization_threshold);
    if (DoDefrag(CollectPageStats::NO, threshold)) {
      // we didn't finish the scan
      return util::ProactorBase::kOnIdleMaxLevel;
    }
  }
  return 6;  // priority.
}

EngineShard::EngineShard(util::ProactorBase* pb, mi_heap_t* heap)
    : txq_([](const Transaction* t) { return t->txid(); }),
      queue_(kQueueLen, 1, 1),
      queue2_(kQueueLen / 2, 2, 2),
      shard_id_(pb->GetPoolIndex()),
      mi_resource_(heap) {
  queue_.Start(absl::StrCat("shard_queue_", shard_id()));
  queue2_.Start(absl::StrCat("l2_queue_", shard_id()));
}

void EngineShard::Shutdown() {
  DVLOG(1) << "EngineShard::Shutdown";

  queue_.Shutdown();
  queue2_.Shutdown();
  DCHECK(!fiber_heartbeat_periodic_.IsJoinable());
  DCHECK(!fiber_shard_handler_periodic_.IsJoinable());
}

void EngineShard::StopPeriodicFiber() {
  ProactorBase::me()->RemoveOnIdleTask(defrag_task_id_);
  ProactorBase::me()->RemoveOnIdleTask(huffman_check_task_id_);

  fiber_heartbeat_periodic_done_.Notify();
  if (fiber_heartbeat_periodic_.IsJoinable()) {
    fiber_heartbeat_periodic_.Join();
  }
  fiber_shard_handler_periodic_done_.Notify();
  if (fiber_shard_handler_periodic_.IsJoinable()) {
    fiber_shard_handler_periodic_.Join();
  }
}

static void RunFPeriodically(std::function<void()> f, std::chrono::milliseconds period_ms,
                             std::string_view error_msg, util::fb2::Done* waiter) {
  int64_t last_heartbeat_ms = INT64_MAX;

  while (true) {
    if (waiter->WaitFor(period_ms)) {
      VLOG(2) << "finished running engine shard periodic task";
      return;
    }

    int64_t now_ms = fb2::ProactorBase::GetMonotonicTimeNs() / 1000000;
    if (now_ms - 5 * period_ms.count() > last_heartbeat_ms) {
      VLOG(1) << "This " << error_msg << " step took " << now_ms - last_heartbeat_ms << "ms";
    }
    f();
    last_heartbeat_ms = fb2::ProactorBase::GetMonotonicTimeNs() / 1000000;
  }
}

void EngineShard::StartPeriodicHeartbeatFiber(util::ProactorBase* pb) {
  auto cycle_ms = GetPeriodicCycleMs();
  if (!cycle_ms) {
    return;
  }
  auto heartbeat = [this]() { Heartbeat(); };

  eviction_state_.rss_eviction_enabled = GetFlag(FLAGS_enable_heartbeat_rss_eviction);
  std::chrono::milliseconds period_ms(*cycle_ms);

  fb2::Fiber::Opts fb_opts{.priority = absl::GetFlag(FLAGS_background_heartbeat)
                                           ? fb2::FiberPriority::BACKGROUND
                                           : fb2::FiberPriority::NORMAL,
                           .name = absl::StrCat("heartbeat_periodic", pb->GetPoolIndex())};
  fiber_heartbeat_periodic_ = fb2::Fiber(fb_opts, [this, period_ms, heartbeat]() mutable {
    RunFPeriodically(heartbeat, period_ms, "heartbeat", &fiber_heartbeat_periodic_done_);
  });
  defrag_task_id_ = pb->AddOnIdleTask([this]() { return DefragTask(); });
}

void EngineShard::StartPeriodicShardHandlerFiber(util::ProactorBase* pb,
                                                 std::function<void()> shard_handler) {
  auto clock_cycle_ms = GetPeriodicCycleMs();
  if (!clock_cycle_ms) {
    return;
  }

  // Minimum 100ms
  std::chrono::milliseconds period_ms(std::max(100u, *clock_cycle_ms));
  fiber_shard_handler_periodic_ = MakeFiber(
      [this, index = pb->GetPoolIndex(), period_ms, handler = std::move(shard_handler)]() mutable {
        ThisFiber::SetName(absl::StrCat("shard_handler_periodic", index));
        RunFPeriodically(std::move(handler), period_ms, "shard handler",
                         &fiber_shard_handler_periodic_done_);
      });
}

void EngineShard::InitThreadLocal(ProactorBase* pb) {
  CHECK(shard_ == nullptr) << pb->GetPoolIndex();

  mi_heap_t* data_heap = ServerState::tlocal()->data_heap();
  void* ptr = mi_heap_malloc_aligned(data_heap, sizeof(EngineShard), alignof(EngineShard));
  shard_ = new (ptr) EngineShard(pb, data_heap);

  CompactObj::InitThreadLocal(shard_->memory_resource());
  SmallString::InitThreadLocal(data_heap);

  shard_->shard_search_indices_ = std::make_unique<ShardDocIndices>();
}

void EngineShard::InitTieredStorage(ProactorBase* pb, size_t max_file_size) {
  if (string backing_prefix = GetFlag(FLAGS_tiered_prefix); !backing_prefix.empty()) {
    LOG_IF(FATAL, pb->GetKind() != ProactorBase::IOURING)
        << "Only ioring based backing storage is supported. Exiting...";

    // TODO: enable tiered storage on non-default namespace
    DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard_id());
    auto* shard = EngineShard::tlocal();
    shard->tiered_storage_ = make_unique<TieredStorage>(max_file_size, &db_slice);
    error_code ec = shard->tiered_storage_->Open(backing_prefix);
    CHECK(!ec) << ec.message();
  }
}

void EngineShard::DestroyThreadLocal() {
  if (!shard_)
    return;

  uint32_t shard_id = shard_->shard_id();
  mi_heap_t* tlh = shard_->mi_resource_.heap();

  shard_->Shutdown();

  shard_->~EngineShard();
  mi_free(shard_);
  shard_ = nullptr;
  CompactObj::InitThreadLocal(nullptr);
  mi_heap_delete(tlh);
  VLOG(1) << "Shard reset " << shard_id;
}

// Is called by Transaction::ExecuteAsync in order to run transaction tasks.
// Only runs in its own thread.
void EngineShard::PollExecution(const char* context, Transaction* trans) {
  DVLOG(2) << "PollExecution " << context << " " << (trans ? trans->DebugId() : "") << " "
           << txq_.size() << " " << (continuation_trans_ ? continuation_trans_->DebugId() : "");

  ShardId sid = shard_id();
  stats_.poll_execution_total++;

  // If any of the following flags are present, we are guaranteed to run in this function:
  // 1. AWAKED_Q -> Blocking transactions are executed immediately after waking up, they don't
  // occupy a place in txq and have highest priority
  // 2. WAS_SUSPENDED -> Suspended transactions are run to clean up and finalize blocking keys
  // 3. OUT_OF_ORDER -> Transactions without conflicting keys can run earlier than their position in
  // txq is reached
  uint16_t flags = Transaction::AWAKED_Q | Transaction::WAS_SUSPENDED | Transaction::OUT_OF_ORDER;
  auto [trans_mask, disarmed] =
      trans ? trans->DisarmInShardWhen(sid, flags) : make_pair(uint16_t(0), false);

  if (trans && trans_mask == 0)  // If not armed, it means that this poll task expired
    return;

  if (trans_mask & Transaction::AWAKED_Q) {
    CHECK(trans->GetNamespace().GetBlockingController(shard_id_)->HasAwakedTransaction());
    CHECK(continuation_trans_ == nullptr)
        << continuation_trans_->DebugId() << " when polling " << trans->DebugId()
        << "cont_mask: " << continuation_trans_->DEBUG_GetLocalMask(sid) << " vs "
        << trans->DEBUG_GetLocalMask(sid);

    // Commands like BRPOPLPUSH don't conclude immediately
    if (!trans->RunInShard(this, true)) {
      // execution is blocked while HasAwakedTransaction() returns true, so no need to set
      // continuation_trans_. Moreover, setting it for wakened multi-hop transactions may lead to
      // inconcistency, see BLMoveSimultaneously test.
      // continuation_trans_ = trans;
      return;
    }

    trans = nullptr;  // Avoid handling the caller below
  }

  bool update_stats = false;
  ++poll_concurrent_factor_;

  auto run = [this, &update_stats](Transaction* tx, bool allow_removal) -> bool /* concluding */ {
    update_stats = true;
    return tx->RunInShard(this, allow_removal);
  };

  // Check the currently running transaction, we have to handle it first until it concludes
  if (continuation_trans_) {
    bool is_self = continuation_trans_ == trans;
    if (is_self)
      trans = nullptr;

    if ((is_self && disarmed) || continuation_trans_->DisarmInShard(sid)) {
      if (bool concludes = run(continuation_trans_, true); concludes) {
        continuation_trans_ = nullptr;
        continuation_debug_id_.clear();
      } else {
        continuation_debug_id_ = continuation_trans_->DebugId(sid);
      }
    }
  }

  // Progress on the transaction queue if no transaction is running currently.
  Transaction* head = nullptr;

  while (continuation_trans_ == nullptr && !txq_.Empty()) {
    head = get<Transaction*>(txq_.Front());

    // Break if there are any awakened transactions, as we must give way to them
    // before continuing to handle regular transactions from the queue.
    if (head->GetNamespace().GetBlockingController(shard_id_) &&
        head->GetNamespace().GetBlockingController(shard_id_)->HasAwakedTransaction())
      break;

    VLOG(2) << "Considering head " << head->DebugId()
            << " isarmed: " << head->DEBUG_IsArmedInShard(sid);

    // If the transaction isn't armed yet, it will be handled by a successive poll
    bool should_run = (head == trans && disarmed) || head->DisarmInShard(sid);
    if (!should_run)
      break;

    // Avoid processing the caller transaction below if we found it in the queue,
    // because it most likely won't have enough time to arm itself again.
    if (head == trans)
      trans = nullptr;

    TxId txid = head->txid();

    // Update commited_txid before running, because RunInShard might block on i/o.
    // This way scheduling transactions won't see an understated value.
    DCHECK_LT(committed_txid_, txid);  //  strictly increasing when processed via txq
    committed_txid_ = txid;

    DCHECK(!continuation_trans_);  // while() check above ensures this.
    if (bool concludes = run(head, true); !concludes) {
      DCHECK_EQ(head->DEBUG_GetTxqPosInShard(sid), TxQueue::kEnd) << head->DebugId(sid);
      continuation_trans_ = head;
      continuation_debug_id_ = head->DebugId(sid);
    }
  }

  // If we disarmed, but didn't find ourselves in the loop, run now.
  if (trans && disarmed) {
    // if WAS_SUSPENDED is true but not AWAKED_Q, it means the transaction was awaked
    // in another thread and this one just follows along.
    DCHECK(trans_mask & (Transaction::OUT_OF_ORDER | Transaction::WAS_SUSPENDED));
    CHECK(trans != continuation_trans_);

    bool is_ooo = trans_mask & Transaction::OUT_OF_ORDER;

    // For OOO transactions that are still in the queue, we can not remove them unless
    // they conclude.
    bool concludes = run(trans, !is_ooo);
    if (is_ooo && concludes) {
      stats_.tx_ooo_total++;
    }

    // If the transaction concluded, it must remove itself from the tx queue.
    // Otherwise it is required to stay there to keep the relative order.
    if (!concludes && is_ooo) {
      LOG_IF(DFATAL, trans->DEBUG_GetTxqPosInShard(sid) == TxQueue::kEnd);
    }
  }
  --poll_concurrent_factor_;
  if (update_stats) {
    CacheStats();
  }
}

void EngineShard::RemoveContTx(Transaction* tx) {
  if (continuation_trans_ == tx) {
    continuation_trans_ = nullptr;
    continuation_debug_id_.clear();
  }
}

void EngineShard::Heartbeat() {
  DVLOG(3) << " Hearbeat";
  DCHECK(namespaces);

  CacheStats();

  // TODO: iterate over all namespaces
  DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard_id());
  // Skip heartbeat if we are serializing a big value
  static auto start = std::chrono::system_clock::now();
  // Skip heartbeat if global transaction is in process.
  // This is determined by attempting to check if shard lock can be acquired.
  const bool can_acquire_global_lock = shard_lock()->Check(IntentLock::Mode::EXCLUSIVE);

  if (db_slice.WillBlockOnJournalWrite() || !can_acquire_global_lock) {
    const auto elapsed = std::chrono::system_clock::now() - start;
    if (elapsed > std::chrono::seconds(1)) {
      const auto elapsed_seconds = std::chrono::duration_cast<std::chrono::seconds>(elapsed);
      LOG_EVERY_T(WARNING, 5) << "Stalled heartbeat() fiber for " << elapsed_seconds.count()
                              << " seconds";
    }
    return;
  }

  thread_local bool check_huffman = (shard_id_ == 0);  // run it only on shard 0.
  if (check_huffman) {
    auto* ptr = db_slice.GetDBTable(0);
    if (ptr) {
      size_t key_usage = ptr->stats.memory_usage_by_type[OBJ_KEY];
      size_t obj_usage = ptr->stats.obj_memory_usage;

#ifdef NDEBUG
      constexpr size_t MB_THRESHOLD = 50 * 1024 * 1024;
#else
      constexpr size_t MB_THRESHOLD = 5 * 1024 * 1024;
#endif

      if (key_usage > MB_THRESHOLD && key_usage > obj_usage / 8) {
        VLOG(1) << "Scheduling huffman check task, key usage: " << key_usage
                << ", obj usage: " << obj_usage;

        check_huffman = false;  // trigger only once.

        // launch the task
        auto task = std::make_unique<HuffmanCheckTask>();
        huffman_check_task_id_ = ProactorBase::me()->AddOnIdleTask([task = std::move(task)] {
          if (!shard_ || !namespaces) {
            return -1;
          }

          DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard_->shard_id());
          return task->Run(&db_slice);
        });
      }
    }
  }

  if (!IsReplica()) {  // Never run expiry/evictions on replica.
    RetireExpiredAndEvict();
  }

  if (tiered_storage_ && tiered_storage_->ShouldOffload()) {
    VLOG(1) << "Running Offloading, memory=" << db_slice.memory_budget()
            << ", cool memory: " << tiered_storage_->CoolMemoryUsage();

    for (unsigned i = 0; i < db_slice.db_array_size(); ++i) {
      if (!db_slice.IsDbValid(i))
        continue;
      tiered_storage_->RunOffloading(i);
    }
  }
}

void EngineShard::RetireExpiredAndEvict() {
  // Disable flush journal changes to prevent preemtion
  journal::JournalFlushGuard journal_flush_guard(journal_);

  // TODO: iterate over all namespaces
  DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard_id());
  constexpr double kTtlDeleteLimit = 200;

  uint32_t traversed = GetMovingSum6(TTL_TRAVERSE);
  uint32_t deleted = GetMovingSum6(TTL_DELETE);
  unsigned ttl_delete_target = 5;

  if (deleted > 10) {
    // deleted should be <= traversed.
    // hence we map our delete/traversed ratio into a range [0, kTtlDeleteLimit).
    // The higher ttl_delete_target the more likely we have lots of expired items that need
    // to be deleted.
    ttl_delete_target = unsigned(kTtlDeleteLimit * double(deleted) / (double(traversed) + 10));
  }

  DbContext db_cntx;
  db_cntx.time_now_ms = GetCurrentTimeMs();

  size_t deleted_bytes = 0;
  size_t eviction_goal = GetFlag(FLAGS_enable_heartbeat_eviction) ? CalculateEvictionBytes() : 0;

  for (unsigned i = 0; i < db_slice.db_array_size(); ++i) {
    if (!db_slice.IsDbValid(i))
      continue;

    db_cntx.db_index = i;
    auto [pt, expt] = db_slice.GetTables(i);
    if (!expt->Empty()) {
      DbSlice::DeleteExpiredStats stats = db_slice.DeleteExpiredStep(db_cntx, ttl_delete_target);

      deleted_bytes += stats.deleted_bytes;
      eviction_goal -= std::min(eviction_goal, size_t(stats.deleted_bytes));
      counter_[TTL_TRAVERSE].IncBy(stats.traversed);
      counter_[TTL_DELETE].IncBy(stats.deleted);
      stats_.total_heartbeat_expired_keys += stats.deleted;
      stats_.total_heartbeat_expired_bytes += stats.deleted_bytes;
      ++stats_.total_heartbeat_expired_calls;
      VLOG(2) << "Heartbeat expired " << stats.deleted << " keys with total bytes "
              << stats.deleted_bytes << " with total expire flow calls "
              << stats_.total_heartbeat_expired_calls;
    }

    if (eviction_goal) {
      uint32_t starting_segment_id = rand() % pt->GetSegmentCount();
      auto [evicted_items, evicted_bytes] =
          db_slice.FreeMemWithEvictionStepAtomic(i, db_cntx, starting_segment_id, eviction_goal);

      VLOG(2) << "Heartbeat eviction: Expected to evict " << eviction_goal
              << " bytes. Actually evicted " << evicted_items << " items, " << evicted_bytes
              << " bytes. Max eviction per heartbeat: "
              << GetFlag(FLAGS_max_eviction_per_heartbeat);

      deleted_bytes += evicted_bytes;
      eviction_goal -= std::min(eviction_goal, evicted_bytes);
    }
  }

  // Track deleted bytes only if we expect to lower memory
  if (eviction_state_.track_deleted_bytes) {
    eviction_state_.deleted_bytes_at_prev_eviction = deleted_bytes;
  }
}

// Adjust deleted bytes w.r.t shard used memory. If we increase shard used
// memory in current heartbeat we can invalidate deleted_bytes. Otherwise we adjust deleted
// bytes by diff.
void EngineShard::EvictionTaskState::AdjustDeletedBytes(size_t shard_used_memory) {
  if (shard_used_memory >= shard_used_memory_at_prev_eviction) {
    deleted_bytes_at_prev_eviction = 0;
  } else {
    deleted_bytes_at_prev_eviction = std::min(
        deleted_bytes_at_prev_eviction, shard_used_memory_at_prev_eviction - shard_used_memory);
  }
}

// Check if adding value of previous deleted bytes will be higher than rss memory budget and
// limit if needed.
void EngineShard::EvictionTaskState::LimitAccumulatedDeletedBytes(
    size_t shard_rss_over_memory_budget) {
  const size_t next_acc_deleted_bytes =
      acc_deleted_bytes_during_eviction + deleted_bytes_at_prev_eviction;
  acc_deleted_bytes_during_eviction = shard_rss_over_memory_budget > next_acc_deleted_bytes
                                          ? next_acc_deleted_bytes
                                          : shard_rss_over_memory_budget;
}

// Once the rss memory is lowered we can start also decreasing accumulated total bytes.
void EngineShard::EvictionTaskState::AdjustAccumulatedDeletedBytes(size_t global_used_rss_memory) {
  if (global_used_rss_memory < global_rss_memory_at_prev_eviction) {
    auto decrease_delete_bytes_before_rss_update =
        std::min(acc_deleted_bytes_during_eviction,
                 (global_rss_memory_at_prev_eviction - global_used_rss_memory) / shard_set->size());
    VLOG(2) << "deleted_bytes_before_rss_update: " << acc_deleted_bytes_during_eviction
            << " decrease_delete_bytes_before_rss_update: "
            << decrease_delete_bytes_before_rss_update;
    acc_deleted_bytes_during_eviction -= decrease_delete_bytes_before_rss_update;
  }
  LOG_IF(DFATAL, global_used_rss_memory < (acc_deleted_bytes_during_eviction * shard_set->size()))
      << "RSS eviction underflow "
      << "global_used_rss_memory: " << global_used_rss_memory
      << " total_deleted_bytes_on_eviction: " << acc_deleted_bytes_during_eviction;
}

size_t EngineShard::CalculateEvictionBytes() {
  const size_t shards_count = shard_set->size();
  const double eviction_memory_budget_threshold = GetFlag(FLAGS_eviction_memory_budget_threshold);

  // Calculate threshold for both used_memory and rss_memory
  const size_t limit = max_memory_limit.load(memory_order_relaxed);
  const size_t shard_memory_budget_threshold =
      size_t(limit * eviction_memory_budget_threshold) / shards_count;

  const size_t global_used_memory = used_mem_current.load(memory_order_relaxed);

  // Calculate how many bytes we need to evict on this shard
  size_t goal_bytes =
      CalculateHowManyBytesToEvictOnShard(limit, global_used_memory, shard_memory_budget_threshold);

  VLOG_IF(2, goal_bytes > 0) << "Used memory goal bytes: " << goal_bytes
                             << ", used memory: " << global_used_memory
                             << ", memory limit: " << max_memory_limit;

  // Check for `enable_heartbeat_rss_eviction` flag since it dynamic. And reset
  // state if flag has changed.
  bool rss_eviction_enabled_flag = GetFlag(FLAGS_enable_heartbeat_rss_eviction);
  if (eviction_state_.rss_eviction_enabled != rss_eviction_enabled_flag) {
    eviction_state_.Reset(rss_eviction_enabled_flag);
  }
  if (eviction_state_.rss_eviction_enabled) {
    const size_t global_used_rss_memory = rss_mem_current.load(memory_order_relaxed);
    const size_t rss_memory_threshold_start = limit * (1. - eviction_memory_budget_threshold);
    const size_t shard_used_memory = UsedMemory();

    // Adjust previous deleted bytes
    eviction_state_.AdjustDeletedBytes(shard_used_memory);

    // Calculate memory budget that is higher than rss_memory_threshold_start. This is our limit
    // for accumulated_deleted_bytes.
    const size_t shard_rss_over_memory_budget =
        global_used_rss_memory > rss_memory_threshold_start
            ? (global_used_rss_memory - rss_memory_threshold_start) / shards_count
            : 0;
    eviction_state_.LimitAccumulatedDeletedBytes(shard_rss_over_memory_budget);

    // Once the rss memory is lowered we can start also decreasing accumulated total bytes.
    eviction_state_.AdjustAccumulatedDeletedBytes(global_used_rss_memory);

    // Update rss/used memory for this heartbeat
    eviction_state_.global_rss_memory_at_prev_eviction = global_used_rss_memory;
    eviction_state_.shard_used_memory_at_prev_eviction = shard_used_memory;

    // If we underflow use limit as used_memory
    size_t used_rss_memory_with_deleted_bytes = std::min(
        global_used_rss_memory - eviction_state_.acc_deleted_bytes_during_eviction * shards_count,
        limit);

    // Try to evict more bytes if we are close to the rss memory limit
    size_t rss_goal_bytes = CalculateHowManyBytesToEvictOnShard(
        limit, used_rss_memory_with_deleted_bytes, shard_memory_budget_threshold);

    // RSS evictions starts so we should start tracking deleted_bytes
    if (rss_goal_bytes) {
      eviction_state_.track_deleted_bytes = true;
    } else {
      // There is no RSS eviction goal and we have cleared tracked deleted bytes
      if (!eviction_state_.acc_deleted_bytes_during_eviction) {
        eviction_state_.track_deleted_bytes = false;
      }
    }

    VLOG_IF(2, rss_goal_bytes > 0)
        << "Rss memory goal bytes: " << rss_goal_bytes
        << ", rss used memory: " << global_used_rss_memory << ", rss memory limit: " << limit
        << ", accumulated_deleted_bytes_during_eviction: "
        << eviction_state_.acc_deleted_bytes_during_eviction;

    goal_bytes = std::max(goal_bytes, rss_goal_bytes);
  }

  return goal_bytes;
}

void EngineShard::CacheStats() {
  uint64_t now = fb2::ProactorBase::GetMonotonicTimeNs();
  if (last_mem_params_.updated_at + 1000000 > now)  // 1ms
    return;

  size_t used_mem = UsedMemory();
  DbSlice& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard_id());

  // Reflect local memory change on global value
  size_t delta = used_mem - last_mem_params_.used_mem;  // negative value wraps safely
  size_t current = used_mem_current.fetch_add(delta, memory_order_relaxed) + delta;
  ssize_t free_mem = max_memory_limit.load(memory_order_relaxed) - current;

  // Estimate bytes per object, excluding table memory
  size_t entries = db_slice.entries_count();
  size_t table_memory =
      db_slice.table_memory() + (tiered_storage_ ? tiered_storage_->CoolMemoryUsage() : 0);
  size_t obj_memory = table_memory <= used_mem ? used_mem - table_memory : 0;
  size_t bytes_per_obj = entries > 0 ? obj_memory / entries : 0;

  db_slice.UpdateMemoryParams(free_mem / shard_set->size(), bytes_per_obj);
  last_mem_params_ = {now, used_mem};
}

size_t EngineShard::UsedMemory() const {
  return mi_resource_.used() + zmalloc_used_memory_tl + SmallString::UsedThreadLocal() +
         search_indices()->GetUsedMemory();
}

bool EngineShard::ShouldThrottleForTiering() const {
  // Throttle if the tiered storage is busy offloading (at least 30% of allowed capacity)
  return tiered_storage_ && tiered_storage_->WriteDepthUsage() > 0.3 &&
         tiered_storage_->ShouldOffload();
}

void EngineShard::FinalizeMulti(Transaction* tx) {
  if (continuation_trans_ == tx) {
    continuation_trans_ = nullptr;
  }

  // Wake only if no tx queue head is currently running
  auto* bc = tx->GetNamespace().GetBlockingController(shard_id());
  if (bc && continuation_trans_ == nullptr)
    bc->NotifyPending();

  PollExecution("unlockmulti", nullptr);
}

EngineShard::TxQueueInfo EngineShard::AnalyzeTxQueue() const {
  const TxQueue* queue = txq();

  ShardId sid = shard_id();
  TxQueueInfo info;

  if (queue->Empty())
    return info;

  auto cur = queue->Head();
  info.tx_total = queue->size();
  unsigned max_db_id = 0;

  auto& db_slice = namespaces->GetDefaultNamespace().GetCurrentDbSlice();

  {
    auto value = queue->At(cur);
    Transaction* trx = std::get<Transaction*>(value);
    info.head.debug_id_info = trx->DebugId(sid);
  }

  do {
    auto value = queue->At(cur);
    Transaction* trx = std::get<Transaction*>(value);
    // find maximum index of databases used by transactions
    if (trx->GetDbIndex() > max_db_id) {
      max_db_id = trx->GetDbIndex();
    }

    bool is_armed = trx->DEBUG_IsArmedInShard(sid);
    DVLOG(1) << "Inspecting " << trx->DebugId() << " is_armed " << is_armed;
    if (is_armed) {
      info.tx_armed++;

      if (trx->IsGlobal() || (trx->IsMulti() && trx->GetMultiMode() == Transaction::GLOBAL)) {
        info.tx_global++;
      } else {
        const DbTable* table = db_slice.GetDBTable(trx->GetDbIndex());
        bool can_run = !HasContendedLocks(sid, trx, table);
        if (can_run) {
          info.tx_runnable++;
        }
      }
    }
    cur = queue->Next(cur);
  } while (cur != queue->Head());

  // Analyze locks
  for (unsigned i = 0; i <= max_db_id; ++i) {
    const DbTable* table = db_slice.GetDBTable(i);
    if (table == nullptr)
      continue;

    info.total_locks += table->trans_locks.Size();
    for (const auto& [key, lock] : table->trans_locks) {
      if (lock.IsContended()) {
        info.contended_locks++;
        if (lock.ContentionScore() > info.max_contention_score) {
          info.max_contention_score = lock.ContentionScore();
          info.max_contention_lock = key;
        }
      }
    }
  }

  return info;
}

}  // namespace dfly
