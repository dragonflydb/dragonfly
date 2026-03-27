// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#ifdef __linux__

#include "facade/disk_backed_queue.h"

#include <absl/strings/str_cat.h>
#include <fcntl.h>
#include <gmock/gmock.h>
#include <unistd.h>

#include <memory>
#include <string>
#include <vector>

#include "base/flags.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "io/io.h"
#include "util/fibers/pool.h"

namespace dfly {
namespace {

using namespace facade;

class DiskBackedQueueTest : public testing::Test {
 protected:
  void SetUp() override {
    pp_.reset(util::fb2::Pool::IOUring(16, 1));
    pp_->Run();
  }

  void TearDown() override {
    pp_->Stop();
    pp_.reset();
  }

  std::unique_ptr<util::ProactorPool> pp_;
};

// Verifies that after reading >= 4096 bytes, punch_hole is called correctly
// and disk space is reclaimed.
TEST_F(DiskBackedQueueTest, PunchHoleReleasesSpace) {
  pp_->at(0)->Await([]() {
    // Use id=2 to avoid collision with ReadWrite test.
    DiskBackedQueue backing(2);
    ASSERT_FALSE(backing.Init());

    // Write 3 pages (12288 bytes) so the punch logic is triggered on reads.
    std::string data(12288, 'x');
    {
      util::fb2::Done done;
      DiskBackedQueue::Chunk chunk;
      chunk.data.assign(reinterpret_cast<uint8_t*>(data.data()),
                        reinterpret_cast<uint8_t*>(data.data()) + data.size());
      backing.PushAsync(std::move(chunk), [&done](std::error_code ec) {
        ASSERT_FALSE(ec);
        done.Notify();
      });
      done.Wait();
    }

    // Read all data back in 4096-byte chunks.
    std::string results;
    while (!backing.Empty()) {
      std::string buf(4096, '\0');
      auto out = io::MutableBytes(reinterpret_cast<uint8_t*>(buf.data()), buf.size());
      util::fb2::Done done;
      backing.PopAsync(out, [&done, &results, &buf](io::Result<size_t> res) {
        ASSERT_TRUE(res);
        results.append(buf.data(), *res);
        done.Notify();
      });
      done.Wait();
    }
    EXPECT_EQ(results, data);

    // After reading all 3 pages the punch should have freed the first 3 aligned pages.
    // SEEK_HOLE at offset 0 returns 0 when a hole starts at the beginning of the file.
    int check_fd = open("/tmp/2", O_RDONLY);
    ASSERT_GE(check_fd, 0);
    off_t hole_start = lseek(check_fd, 0, SEEK_HOLE);
    close(check_fd);
    EXPECT_EQ(hole_start, 0) << "Expected hole at start of file - punch_hole did not free space";

    ASSERT_FALSE(backing.Close());
  });
}

// Verifies that reading across multiple pages advances the punch offset correctly so that
// successive reads keep freeing space (not re-punching offset 0 or skipping blocks).
TEST_F(DiskBackedQueueTest, PunchHoleAdvancesOffset) {
  pp_->at(0)->Await([]() {
    DiskBackedQueue backing(3);
    ASSERT_FALSE(backing.Init());

    // Write 8 pages so we can do several reads and check the hole grows.
    std::string data(32768, 'y');
    {
      util::fb2::Done done;
      DiskBackedQueue::Chunk chunk;
      chunk.data.assign(reinterpret_cast<uint8_t*>(data.data()),
                        reinterpret_cast<uint8_t*>(data.data()) + data.size());
      backing.PushAsync(std::move(chunk), [&done](std::error_code ec) {
        ASSERT_FALSE(ec);
        done.Notify();
      });
      done.Wait();
    }

    // Read exactly 4096 bytes (1 page).
    {
      std::string buf(4096, '\0');
      auto out = io::MutableBytes(reinterpret_cast<uint8_t*>(buf.data()), buf.size());
      util::fb2::Done done;
      backing.PopAsync(out, [&done](io::Result<size_t> res) {
        ASSERT_TRUE(res);
        done.Notify();
      });
      done.Wait();
    }

    // After 1 page read the hole should start at 0 and the first non-hole (data) should be at
    // offset 4096 (i.e., lseek SEEK_DATA starting from 0 skips the punched hole).
    int check_fd = open("/tmp/3", O_RDONLY);
    ASSERT_GE(check_fd, 0);
    off_t first_hole = lseek(check_fd, 0, SEEK_HOLE);
    off_t first_data = lseek(check_fd, 0, SEEK_DATA);
    close(check_fd);

    EXPECT_EQ(first_hole, 0) << "Hole should begin at offset 0 after first page read";
    EXPECT_EQ(first_data, 4096) << "Non-hole data should start at 4096 after punching first page";

    ASSERT_FALSE(backing.Close());
  });
}

// Verifies that unaligned writes and reads correctly punch holes at aligned boundaries.
// Punch should only occur when we've fully read past 4096-byte boundaries.
TEST_F(DiskBackedQueueTest, PunchHoleUnalignedReadsAndWrites) {
  pp_->at(0)->Await([]() {
    DiskBackedQueue backing(4);
    ASSERT_FALSE(backing.Init());

    // Write 10000 bytes (not a multiple of 4096).
    // This is 2 full pages (8192 bytes) + 1808 partial bytes.
    std::string data(10000, 'z');
    {
      util::fb2::Done done;
      DiskBackedQueue::Chunk chunk;
      chunk.data.assign(reinterpret_cast<uint8_t*>(data.data()),
                        reinterpret_cast<uint8_t*>(data.data()) + data.size());
      backing.PushAsync(std::move(chunk), [&done](std::error_code ec) {
        ASSERT_FALSE(ec);
        done.Notify();
      });
      done.Wait();
    }

    // Read 3000 bytes (unaligned, less than 1 page).
    // next_read_offset_ will be 3000, but aligned_end = (3000/4096)*4096 = 0.
    // So no punch should happen yet.
    std::string results;
    {
      std::string buf(3000, '\0');
      auto out = io::MutableBytes(reinterpret_cast<uint8_t*>(buf.data()), buf.size());
      util::fb2::Done done;
      backing.PopAsync(out, [&done, &results, &buf](io::Result<size_t> res) {
        ASSERT_TRUE(res);
        results.append(buf.data(), *res);
        done.Notify();
      });
      done.Wait();
    }

    // Check that no hole exists yet (first 3000 bytes read but not 4096-aligned).
    int check_fd = open("/tmp/4", O_RDONLY);
    ASSERT_GE(check_fd, 0);
    off_t hole_at_start = lseek(check_fd, 0, SEEK_HOLE);
    // SEEK_HOLE from offset 0 should jump to EOF if no hole exists at start.
    EXPECT_GT(hole_at_start, 0) << "No hole should exist yet after reading 3000 bytes";
    close(check_fd);

    // Read another 2000 bytes (total read = 5000 bytes).
    // next_read_offset_ will be 5000, aligned_end = (5000/4096)*4096 = 4096.
    // Now the first page (0-4095) should be punched.
    {
      std::string buf(2000, '\0');
      auto out = io::MutableBytes(reinterpret_cast<uint8_t*>(buf.data()), buf.size());
      util::fb2::Done done;
      backing.PopAsync(out, [&done, &results, &buf](io::Result<size_t> res) {
        ASSERT_TRUE(res);
        results.append(buf.data(), *res);
        done.Notify();
      });
      done.Wait();
    }

    // Verify first page is now a hole.
    check_fd = open("/tmp/4", O_RDONLY);
    ASSERT_GE(check_fd, 0);
    off_t first_hole = lseek(check_fd, 0, SEEK_HOLE);
    off_t first_data = lseek(check_fd, 0, SEEK_DATA);
    EXPECT_EQ(first_hole, 0) << "Hole should start at offset 0 after reading past 4096 bytes";
    EXPECT_EQ(first_data, 4096) << "Data should start at 4096 (second page)";

    // Read another 3500 bytes (total read = 8500 bytes).
    // next_read_offset_ will be 8500, aligned_end = (8500/4096)*4096 = 8192.
    // Now the first two pages (0-8191) should be punched.
    {
      std::string buf(3500, '\0');
      auto out = io::MutableBytes(reinterpret_cast<uint8_t*>(buf.data()), buf.size());
      util::fb2::Done done;
      backing.PopAsync(out, [&done, &results, &buf](io::Result<size_t> res) {
        ASSERT_TRUE(res);
        results.append(buf.data(), *res);
        done.Notify();
      });
      done.Wait();
    }

    // Verify first two pages are holes.
    first_hole = lseek(check_fd, 0, SEEK_HOLE);
    first_data = lseek(check_fd, 0, SEEK_DATA);
    close(check_fd);
    EXPECT_EQ(first_hole, 0) << "Hole should start at offset 0";
    EXPECT_EQ(first_data, 8192) << "Data should start at 8192 (third page)";

    // Read remaining data and verify results match.
    while (!backing.Empty()) {
      std::string buf(4096, '\0');
      auto out = io::MutableBytes(reinterpret_cast<uint8_t*>(buf.data()), buf.size());
      util::fb2::Done done;
      backing.PopAsync(out, [&done, &results, &buf](io::Result<size_t> res) {
        ASSERT_TRUE(res);
        results.append(buf.data(), *res);
        done.Notify();
      });
      done.Wait();
    }
    EXPECT_EQ(results, data);

    ASSERT_FALSE(backing.Close());
  });
}

TEST_F(DiskBackedQueueTest, AsyncReadWrite) {
  pp_->at(0)->Await([]() {
    DiskBackedQueue backing(5 /* id */);
    EXPECT_FALSE(backing.Init());

    std::string commands;
    for (size_t i = 0; i < 100; ++i) {
      auto cmd = absl::StrCat("SET FOO", i, " BAR");
      commands += cmd;
    }

    // Async write all commands
    util::fb2::Fiber write_fiber = util::fb2::Fiber("writer", [&]() {
      for (size_t i = 0; i < 100; ++i) {
        auto cmd = absl::StrCat("SET FOO", i, " BAR");
        DiskBackedQueue::Chunk chunk;
        chunk.data.assign(reinterpret_cast<const uint8_t*>(cmd.data()),
                          reinterpret_cast<const uint8_t*>(cmd.data()) + cmd.size());

        util::fb2::Done done;
        backing.PushAsync(std::move(chunk), [&done](std::error_code ec) {
          EXPECT_FALSE(ec);
          done.Notify();
        });
        done.Wait();
      }
    });

    write_fiber.Join();

    // Async read all results
    std::string results;
    util::fb2::Fiber read_fiber = util::fb2::Fiber("reader", [&]() {
      while (!backing.Empty()) {
        std::string buf(1024, 'c');
        auto bytes = io::MutableBytes(reinterpret_cast<uint8_t*>(buf.data()), buf.size());

        util::fb2::Done done;
        backing.PopAsync(bytes, [&done, &results, &buf](io::Result<size_t> res) {
          EXPECT_TRUE(res);
          results.append(buf.data(), *res);
          done.Notify();
        });
        done.Wait();
      }
    });

    read_fiber.Join();

    EXPECT_EQ(results.size(), commands.size());
    EXPECT_EQ(results, commands);

    EXPECT_FALSE(backing.Close());
  });
}

TEST_F(DiskBackedQueueTest, AsyncPunchHole) {
  pp_->at(0)->Await([]() {
    DiskBackedQueue backing(6);
    ASSERT_FALSE(backing.Init());

    // Write 3 pages (12288 bytes) asynchronously
    std::string data(12288, 'x');

    util::fb2::Done write_done;
    DiskBackedQueue::Chunk chunk;
    chunk.data.assign(reinterpret_cast<uint8_t*>(data.data()),
                      reinterpret_cast<uint8_t*>(data.data()) + data.size());
    backing.PushAsync(std::move(chunk), [&write_done](std::error_code ec) {
      ASSERT_FALSE(ec);
      write_done.Notify();
    });
    write_done.Wait();

    // Async read all data back in 4096-byte chunks
    std::string results;
    while (!backing.Empty()) {
      std::string buf(4096, '\0');
      auto out = io::MutableBytes(reinterpret_cast<uint8_t*>(buf.data()), buf.size());

      util::fb2::Done read_done;
      backing.PopAsync(out, [&read_done, &results, &buf](io::Result<size_t> res) {
        ASSERT_TRUE(res);
        results.append(buf.data(), *res);
        read_done.Notify();
      });
      read_done.Wait();
    }
    EXPECT_EQ(results, data);

    // Verify punch hole freed space
    int check_fd = open("/tmp/6", O_RDONLY);
    ASSERT_GE(check_fd, 0);
    off_t hole_start = lseek(check_fd, 0, SEEK_HOLE);
    close(check_fd);
    EXPECT_EQ(hole_start, 0) << "Expected hole at start of file - async punch did not free space";

    ASSERT_FALSE(backing.Close());
  });
}

}  // namespace
}  // namespace dfly

#endif  // __linux__
