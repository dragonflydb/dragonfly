// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/mrmw_mutex.h"

#include <random>
#include <thread>

#include "absl/flags/flag.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "util/fibers/pool.h"

ABSL_FLAG(bool, force_epoll, false, "If true, uses epoll api instead iouring to run tests");

namespace dfly::search {

namespace {

// Helper function to simulate reading operation
void ReadTask(MRMWMutex* mutex, std::atomic<size_t>& read_count, size_t sleep_time) {
  read_count.fetch_add(1, std::memory_order_relaxed);
  MRMWMutexLock lock(mutex, MRMWMutex::LockMode::kReadLock);
  util::ThisFiber::SleepFor(std::chrono::milliseconds(sleep_time));
  read_count.fetch_sub(1, std::memory_order_relaxed);
}

// Helper function to simulate writing operation
void WriteTask(MRMWMutex* mutex, std::atomic<size_t>& write_count, size_t sleep_time) {
  write_count.fetch_add(1, std::memory_order_relaxed);
  MRMWMutexLock lock(mutex, MRMWMutex::LockMode::kWriteLock);
  util::ThisFiber::SleepFor(std::chrono::milliseconds(sleep_time));
  write_count.fetch_sub(1, std::memory_order_relaxed);
}

constexpr size_t kReadTaskSleepTime = 50;
constexpr size_t kWriteTaskSleepTime = 100;

}  // namespace

class MRMWMutexTest : public ::testing::Test {
 protected:
  MRMWMutex mutex_;
  std::mt19937 generator_;
  void SetUp() override {
#ifdef __linux__
    if (absl::GetFlag(FLAGS_force_epoll)) {
      pp_.reset(util::fb2::Pool::Epoll(2));
    } else {
      pp_.reset(util::fb2::Pool::IOUring(16, 2));
    }
#else
    pp_.reset(util::fb2::Pool::Epoll(2));
#endif
    pp_->Run();
  }
  void TearDown() override {
    pp_->Stop();
    pp_.reset();
  }
  std::unique_ptr<util::ProactorPool> pp_;
};

// Test 1: Multiple readers can lock concurrently
TEST_F(MRMWMutexTest, MultipleReadersConcurrently) {
  std::atomic<size_t> read_count(0);
  const int num_readers = 5;

  std::vector<util::fb2::Fiber> readers;
  readers.reserve(num_readers);

  for (int i = 0; i < num_readers; ++i) {
    readers.emplace_back(pp_->at(0)->LaunchFiber(util::fb2::Launch::post, [&] {
      ReadTask(&mutex_, std::ref(read_count), kReadTaskSleepTime);
    }));
  }

  // Wait for all reader threads to finish
  for (auto& t : readers) {
    t.Join();
  }

  // All readers should have been able to lock the mutex concurrently
  EXPECT_EQ(read_count.load(), 0);
}

// Test 2: Writer blocks readers and writer should get the lock exclusively
TEST_F(MRMWMutexTest, ReadersBlockWriters) {
  std::atomic<size_t> read_count(0);
  std::atomic<size_t> write_count(0);

  const int num_readers = 10;

  // Start multiple readers
  std::vector<util::fb2::Fiber> readers;
  readers.reserve(num_readers);

  for (int i = 0; i < num_readers; ++i) {
    readers.emplace_back(pp_->at(0)->LaunchFiber(util::fb2::Launch::post, [&] {
      ReadTask(&mutex_, std::ref(read_count), kReadTaskSleepTime);
    }));
  }

  // Give readers time to acquire the lock
  util::ThisFiber::SleepFor(std::chrono::milliseconds(10));

  pp_->at(1)
      ->LaunchFiber(util::fb2::Launch::post,
                    [&] { WriteTask(&mutex_, std::ref(write_count), kWriteTaskSleepTime); })
      .Join();

  // Wait for all reader threads to finish
  for (auto& t : readers) {
    t.Join();
  }

  EXPECT_EQ(read_count.load(), 0);
  EXPECT_EQ(write_count.load(), 0);
}

// Test 3: Unlock transitions correctly and wakes up waiting threads
TEST_F(MRMWMutexTest, ReaderAfterWriter) {
  std::atomic<size_t> write_count(0);
  std::atomic<size_t> read_count(0);

  // Start a writer thread
  auto writer = pp_->at(1)->LaunchFiber(util::fb2::Launch::post, [&] {
    WriteTask(&mutex_, std::ref(write_count), kWriteTaskSleepTime);
  });

  // Give writer time to acquire the lock
  util::ThisFiber::SleepFor(std::chrono::milliseconds(10));

  // Now start a reader task that will block until the writer is done
  pp_->at(0)
      ->LaunchFiber(util::fb2::Launch::post,
                    [&] { ReadTask(&mutex_, std::ref(read_count), kReadTaskSleepTime); })
      .Join();

  // Ensure that writer has completed
  writer.Join();

  EXPECT_EQ(read_count.load(), 0);
  EXPECT_EQ(write_count.load(), 0);
}

// Test 4: Ensure writer gets the lock after readers finish
TEST_F(MRMWMutexTest, WriterAfterReaders) {
  std::atomic<size_t> read_count(0);
  std::atomic<size_t> write_count(0);

  // Start multiple readers
  const int num_readers = 10;
  std::vector<util::fb2::Fiber> readers;
  readers.reserve(num_readers);

  for (int i = 0; i < num_readers; ++i) {
    readers.emplace_back(pp_->at(0)->LaunchFiber(util::fb2::Launch::post, [&] {
      ReadTask(&mutex_, std::ref(read_count), kReadTaskSleepTime);
    }));
  }

  // Wait for all readers to acquire and release the lock
  for (auto& t : readers) {
    t.Join();
  }

  // Start the writer after all readers are done
  pp_->at(1)
      ->LaunchFiber(util::fb2::Launch::post,
                    [&] { WriteTask(&mutex_, std::ref(write_count), kWriteTaskSleepTime); })
      .Join();

  EXPECT_EQ(read_count.load(), 0);
  EXPECT_EQ(write_count.load(), 0);
}

TEST_F(MRMWMutexTest, MixWritersReadersOnDifferentFibers) {
  std::atomic<size_t> read_count(0);
  std::atomic<size_t> write_count(0);

  // Start multiple readers and writers
  const int num_threads = 100;
  std::vector<util::fb2::Fiber> threads;
  threads.reserve(num_threads);

  for (int i = 0; i < num_threads; ++i) {
    if (rand() % 3) {
      threads.emplace_back(pp_->at(0)->LaunchFiber(util::fb2::Launch::post, [&] {
        ReadTask(&mutex_, std::ref(read_count), kReadTaskSleepTime);
      }));
    } else {
      threads.emplace_back(pp_->at(1)->LaunchFiber(util::fb2::Launch::post, [&] {
        WriteTask(&mutex_, std::ref(write_count), kWriteTaskSleepTime);
      }));
    }
  }

  // Wait for all readers to acquire and release the lock
  for (auto& t : threads) {
    t.Join();
  }
}

// TODO: Once we have fiber locking we can test scenario where we write/read on same fibers
// current implementation block thread so it is not possible to test this for now.

// Test 6: Mix of readers and writes on random fibers
// TEST_F(MRMWMutexTest, MixWritersReadersOnFibers) {
//   std::atomic<size_t> read_count(0);
//   std::atomic<size_t> write_count(0);

//   // Start multiple readers and writers
//   const int num_threads = 100;
//   std::vector<util::fb2::Fiber> threads;
//   threads.reserve(num_threads + 1);

//   // Add long read task that will block all write tasks
//   threads.emplace_back(
//       pp_->at(0)->LaunchFiber([&] { ReadTask(&mutex_, std::ref(read_count), 2000); }));

//   // Give long writer time to acquire the lock
//   util::ThisFiber::SleepFor(std::chrono::milliseconds(100));

//   size_t write_threads = 0;
//   for (int i = 0; i < num_threads; ++i) {
//     size_t fiber_id = rand() % 2;
//     if (rand() % 3) {
//       threads.emplace_back(pp_->at(fiber_id)->LaunchFiber(util::fb2::Launch::post, [&] {
//         ReadTask(&mutex_, std::ref(read_count), kReadTaskSleepTime);
//       }));
//     } else {
//       write_threads++;
//       threads.emplace_back(pp_->at(fiber_id)->LaunchFiber(util::fb2::Launch::post, [&] {
//         WriteTask(&mutex_, std::ref(write_count), kWriteTaskSleepTime);
//       }));
//     }
//   }

//   // All shorter threads should be done and only long one remains
//   util::ThisFiber::SleepFor(std::chrono::milliseconds(500));

//   EXPECT_EQ(read_count.load(), 1);

//   EXPECT_EQ(write_count.load(), write_threads);

//   // Wait for all readers to acquire and release the lock
//   for (auto& t : threads) {
//     t.Join();
//   }
// }

}  // namespace dfly::search
