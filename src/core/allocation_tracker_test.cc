#include <absl/strings/match.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "base/gtest.h"
#include "base/logging.h"

#define INJECT_ALLOCATION_TRACKER
#include "core/allocation_tracker.h"

namespace dfly {
namespace {
using namespace std;
using namespace testing;

class LogSink : public google::LogSink {
 public:
  void send(google::LogSeverity severity, const char* full_filename, const char* base_filename,
            int line, const struct tm* tm_time, const char* message, size_t message_len) override {
    logs_.push_back(string(message, message_len));
  }

  const vector<string>& GetLogs() const {
    return logs_;
  }

  void Clear() {
    logs_.clear();
  }

 private:
  vector<string> logs_;
};

class AllocationTrackerTest : public Test {
 protected:
  AllocationTrackerTest() {
    google::AddLogSink(&log_sink_);
  }

  ~AllocationTrackerTest() {
    google::RemoveLogSink(&log_sink_);
    AllocationTracker::Get().Clear();
  }

  vector<string> GetLogsDelta() {
    auto logs = log_sink_.GetLogs();
    log_sink_.Clear();
    return logs;
  }

  void Allocate(size_t s) {
    CHECK(buffer_.empty());
    buffer_.resize(s);  // allocate 1mb before setting up tracking
  }

  void Deallocate() {
    buffer_.clear();
    // Force deallocation
    buffer_.shrink_to_fit();
  }

 private:
  LogSink log_sink_;
  string buffer_;
};

TEST_F(AllocationTrackerTest, UnusedTracker) {
  Allocate(1'000'000);  // allocate 1mb before setting up tracking
  EXPECT_THAT(GetLogsDelta(), Not(Contains(HasSubstr("Allocating"))));
  Deallocate();
}

TEST_F(AllocationTrackerTest, UsedTracker) {
  AllocationTracker::Get().Add(
      {.lower_bound = 1'000'000, .upper_bound = 2'000'000, .sample_odds = 1.0});
  Allocate(1'000'000);  // allocate 1mb before setting up tracking
  EXPECT_THAT(GetLogsDelta(), Contains(HasSubstr("Allocating")));
  EXPECT_THAT(GetLogsDelta(), Not(Contains(HasSubstr("Deallocating"))));
  Deallocate();
  EXPECT_THAT(GetLogsDelta(), Contains(HasSubstr("Deallocating")));

  // Allocate below threshold
  Allocate(100'000);
  EXPECT_THAT(GetLogsDelta(), Not(Contains(HasSubstr("Allocating"))));
  Deallocate();
  EXPECT_THAT(GetLogsDelta(), Not(Contains(HasSubstr("Deallocating"))));

  // Allocate above threshold
  Allocate(10'000'000);
  EXPECT_THAT(GetLogsDelta(), Not(Contains(HasSubstr("Allocating"))));
  Deallocate();
  EXPECT_THAT(GetLogsDelta(), Not(Contains(HasSubstr("Deallocating"))));

  // Remove allocator - stops logging
  EXPECT_TRUE(AllocationTracker::Get().Remove(1'000'000, 2'000'000));
  Allocate(1'000'000);  // allocate 1mb before setting up tracking
  EXPECT_THAT(GetLogsDelta(), Not(Contains(HasSubstr("Allocating"))));
  Deallocate();
  EXPECT_THAT(GetLogsDelta(), Not(Contains(HasSubstr("Deallocating"))));
}

TEST_F(AllocationTrackerTest, MultipleRanges) {
  AllocationTracker::Get().Add(
      {.lower_bound = 1'000'000, .upper_bound = 2'000'000, .sample_odds = 1.0});
  AllocationTracker::Get().Add(
      {.lower_bound = 100'000'000, .upper_bound = 200'000'000, .sample_odds = 1.0});

  // Below all ranges
  Allocate(100'000);
  EXPECT_THAT(GetLogsDelta(), Not(Contains(HasSubstr("Allocating"))));
  Deallocate();

  // Between ranges
  Allocate(10'000'000);
  EXPECT_THAT(GetLogsDelta(), Not(Contains(HasSubstr("Allocating"))));
  Deallocate();

  // Above all ranges
  Allocate(500'000'000);
  EXPECT_THAT(GetLogsDelta(), Not(Contains(HasSubstr("Allocating"))));
  Deallocate();

  // First range
  Allocate(1'000'000);
  EXPECT_THAT(GetLogsDelta(), Contains(HasSubstr("Allocating")));
  Deallocate();

  // Second range
  Allocate(100'000'000);
  EXPECT_THAT(GetLogsDelta(), Contains(HasSubstr("Allocating")));
  Deallocate();
}

TEST_F(AllocationTrackerTest, Sampling) {
  // Statistically, 80% of logs should be logged
  AllocationTracker::Get().Add(
      {.lower_bound = 1'000'000, .upper_bound = 2'000'000, .sample_odds = 0.8});

  const int kIterations = 10'000;
  for (int i = 0; i < kIterations; ++i) {
    Allocate(1'000'000);
    Deallocate();
  }

  int allocations = 0;
  int deallocations = 0;
  for (const string& s : GetLogsDelta()) {
    if (absl::StrContains(s, "Allocating")) {
      ++allocations;
    }
    if (absl::StrContains(s, "Deallocating")) {
      ++deallocations;
    }
  }

  EXPECT_GE(allocations, kIterations * 0.7);
  EXPECT_LE(allocations, kIterations * 0.9);
  EXPECT_EQ(deallocations, 0);  // we only track deletions when sample_odds == 1.0
}

}  // namespace
}  // namespace dfly
