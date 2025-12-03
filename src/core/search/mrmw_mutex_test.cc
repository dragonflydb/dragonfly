// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/mrmw_mutex.h"

#include <random>
#include <thread>

#include "base/gtest.h"
#include "base/logging.h"

namespace dfly::search {

namespace {

// Helper function to simulate reading operation
void ReadTask(MRMWMutex* mutex, std::atomic<size_t>& read_count, size_t sleep_time) {
  read_count.fetch_add(1, std::memory_order_relaxed);
  MRMWMutexLock lock(mutex, MRMWMutex::LockMode::kReadLock);
  std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
  read_count.fetch_sub(1, std::memory_order_relaxed);
}

// Helper function to simulate writing operation
void WriteTask(MRMWMutex* mutex, std::atomic<size_t>& write_count, size_t sleep_time) {
  write_count.fetch_add(1, std::memory_order_relaxed);
  MRMWMutexLock lock(mutex, MRMWMutex::LockMode::kWriteLock);
  std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
  write_count.fetch_sub(1, std::memory_order_relaxed);
}

constexpr size_t kReadTaskSleepTime = 50;
constexpr size_t kWriteTaskSleepTime = 100;

}  // namespace

class MRMWMutexTest : public ::testing::Test {
 protected:
  MRMWMutex mutex_;
  std::mt19937 generator_;
};

// Test 1: Multiple readers can lock concurrently
TEST_F(MRMWMutexTest, MultipleReadersConcurrently) {
  std::atomic<size_t> read_count(0);
  const int num_readers = 5;

  std::vector<std::thread> readers;
  readers.reserve(num_readers);

  for (int i = 0; i < num_readers; ++i) {
    readers.emplace_back(ReadTask, &mutex_, std::ref(read_count), kReadTaskSleepTime);
  }

  // Wait for all reader threads to finish
  for (auto& t : readers) {
    t.join();
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
  std::vector<std::thread> readers;
  readers.reserve(num_readers);

  for (int i = 0; i < num_readers; ++i) {
    readers.emplace_back(ReadTask, &mutex_, std::ref(read_count), kReadTaskSleepTime);
  }

  // Give readers time to acquire the lock
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  // Now start a writer thread that will be blocked on readers
  std::thread writer(WriteTask, &mutex_, std::ref(write_count), kWriteTaskSleepTime);

  // Wait for writer to finish
  writer.join();

  // Wait for all reader threads to finish
  for (auto& t : readers) {
    t.join();
  }

  EXPECT_EQ(read_count.load(), 0);
  EXPECT_EQ(write_count.load(), 0);
}

// Test 3: Unlock transitions correctly and wakes up waiting threads
TEST_F(MRMWMutexTest, UnlockTransitionsAndNotify) {
  std::atomic<size_t> write_count(0);
  std::atomic<size_t> read_count(0);

  // Start a writer thread
  std::thread writer(WriteTask, &mutex_, std::ref(write_count), kWriteTaskSleepTime);
  std::this_thread::sleep_for(std::chrono::milliseconds(10));  // Give writer a head start

  // Now start a reader task that will block until the writer is done
  std::thread reader(ReadTask, &mutex_, std::ref(read_count), kWriteTaskSleepTime);
  reader.join();

  // Ensure that writer has completed
  writer.join();

  EXPECT_EQ(read_count.load(), 0);
  EXPECT_EQ(write_count.load(), 0);
}

// Test 4: Ensure writer gets the lock after readers finish
TEST_F(MRMWMutexTest, WriterAfterReaders) {
  std::atomic<size_t> read_count(0);
  std::atomic<size_t> write_count(0);

  // Start multiple readers
  const int num_readers = 10;
  std::vector<std::thread> readers;
  readers.reserve(num_readers);

  for (int i = 0; i < num_readers; ++i) {
    readers.emplace_back(ReadTask, &mutex_, std::ref(read_count), kReadTaskSleepTime);
  }

  // Wait for all readers to acquire and release the lock
  for (auto& t : readers) {
    t.join();
  }

  // Start the writer after all readers are done
  std::thread writer(WriteTask, &mutex_, std::ref(write_count), kWriteTaskSleepTime);
  writer.join();

  EXPECT_EQ(read_count.load(), 0);
  EXPECT_EQ(write_count.load(), 0);
}

// Test 5: Mix of readers and writes
TEST_F(MRMWMutexTest, MixWritersReaders) {
  std::atomic<size_t> read_count(0);
  std::atomic<size_t> write_count(0);

  // Start multiple readers and writers
  const int num_threads = 100;
  std::vector<std::thread> threads;
  threads.reserve(num_threads);

  // Add long read task that will block all write tasks
  threads.emplace_back(ReadTask, &mutex_, std::ref(read_count), 2000);
  size_t write_threads = 0;

  for (int i = 0; i < num_threads; ++i) {
    if (rand() % 3) {
      threads.emplace_back(ReadTask, &mutex_, std::ref(read_count), kReadTaskSleepTime);
    } else {
      write_threads++;
      threads.emplace_back(WriteTask, &mutex_, std::ref(write_count), kWriteTaskSleepTime);
    }
  }

  // All shorter threads should be done and only long one remains
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  EXPECT_EQ(read_count.load(), 1);

  EXPECT_EQ(write_count.load(), write_threads);

  // Wait for all readers to acquire and release the lock
  for (auto& t : threads) {
    t.join();
  }
}

}  // namespace dfly::search
