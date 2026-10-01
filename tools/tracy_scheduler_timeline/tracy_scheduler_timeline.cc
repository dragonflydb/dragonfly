// Exports persisted Tracy fiber ContextSwitchData and CPU zones without constructing Worker data.
// This intentionally supports only the v0.14.1 on-disk layout.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <optional>
#include <queue>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "TracyEvent.hpp"
#include "TracyFileHeader.hpp"
#include "TracyFileRead.hpp"

namespace {

constexpr int kExpectedVersion = tracy::FileVersion(0, 14, 1);
constexpr int8_t kFiberReason = tracy::ContextSwitchData::Fiber;

struct Interval {
  int64_t start_ns;
  int64_t end_ns;
  uint64_t owner_thread_id;
  uint64_t fiber_thread_id;
};

static_assert(std::is_trivially_copyable_v<Interval>);

struct Zone {
  int64_t start_ns;
  int64_t end_ns;
  uint64_t fiber_thread_id;
  uint32_t extra;
  int16_t source_location;
  uint16_t level;
};

static_assert(std::is_trivially_copyable_v<Zone>);
static_assert(sizeof(Zone) == sizeof(Interval));

struct Segment {
  Interval interval;
  int64_t start_ns;
  int64_t end_ns;
  uint32_t extra;
  int16_t source_location;
  uint16_t zone_level;
};

static_assert(std::is_trivially_copyable_v<Segment>);

struct TimeWindow {
  int64_t start_ns;
  int64_t end_ns;

  bool Overlaps(int64_t start, int64_t end) const {
    return (end > start_ns) && (start < end_ns);
  }
};

bool IntervalLess(const Interval& lhs, const Interval& rhs) {
  if (lhs.start_ns != rhs.start_ns) {
    return lhs.start_ns < rhs.start_ns;
  }
  if (lhs.end_ns != rhs.end_ns) {
    return lhs.end_ns < rhs.end_ns;
  }
  if (lhs.owner_thread_id != rhs.owner_thread_id) {
    return lhs.owner_thread_id < rhs.owner_thread_id;
  }
  return lhs.fiber_thread_id < rhs.fiber_thread_id;
}

bool ZoneLess(const Zone& lhs, const Zone& rhs) {
  if (lhs.fiber_thread_id != rhs.fiber_thread_id) {
    return lhs.fiber_thread_id < rhs.fiber_thread_id;
  }
  if (lhs.level != rhs.level) {
    return lhs.level < rhs.level;
  }
  if (lhs.start_ns != rhs.start_ns) {
    return lhs.start_ns < rhs.start_ns;
  }
  return lhs.end_ns < rhs.end_ns;
}

bool SegmentLess(const Segment& lhs, const Segment& rhs) {
  if (lhs.start_ns != rhs.start_ns) {
    return lhs.start_ns < rhs.start_ns;
  }
  if (lhs.end_ns != rhs.end_ns) {
    return lhs.end_ns < rhs.end_ns;
  }
  if (lhs.interval.owner_thread_id != rhs.interval.owner_thread_id) {
    return lhs.interval.owner_thread_id < rhs.interval.owner_thread_id;
  }
  if (lhs.interval.fiber_thread_id != rhs.interval.fiber_thread_id) {
    return lhs.interval.fiber_thread_id < rhs.interval.fiber_thread_id;
  }
  if (lhs.zone_level != rhs.zone_level) {
    return lhs.zone_level < rhs.zone_level;
  }
  if (lhs.source_location != rhs.source_location) {
    return lhs.source_location < rhs.source_location;
  }
  return lhs.extra < rhs.extra;
}

std::string FormatDuration(int64_t duration_ns) {
  std::ostringstream stream;
  if (duration_ns < 1'000) {
    stream << duration_ns << " ns";
  } else if (duration_ns < 1'000'000) {
    stream << std::fixed << std::setprecision(3) << duration_ns / 1'000.0 << " us";
  } else {
    stream << std::fixed << std::setprecision(3) << duration_ns / 1'000'000.0 << " ms";
  }
  return stream.str();
}

std::string FormatTimestamp(int64_t time_ns) {
  std::ostringstream stream;
  const int64_t seconds = time_ns / 1'000'000'000;
  const int64_t nanoseconds = time_ns % 1'000'000'000;
  stream << '+' << seconds << "s " << std::setw(3) << std::setfill('0') << nanoseconds / 1'000'000
         << ',' << std::setw(3) << std::setfill('0') << (nanoseconds / 1'000) % 1'000 << ','
         << std::setw(3) << std::setfill('0') << nanoseconds % 1'000 << "ns";
  return stream.str();
}

std::string CsvField(std::string_view value) {
  if (value.find_first_of(",\"\n\r") == std::string_view::npos) {
    return std::string(value);
  }
  std::string escaped{"\""};
  for (const char character : value) {
    if (character == '\"') {
      escaped += "\"\"";
    } else {
      escaped += character;
    }
  }
  return escaped + '\"';
}

class ProgressReporter {
 public:
  explicit ProgressReporter(bool enabled) : enabled_(enabled) {
  }

  void Update(std::string_view phase, uint64_t completed, uint64_t total, bool force = false) {
    if (!enabled_) {
      return;
    }
    const auto now = std::chrono::steady_clock::now();
    if (!force && now < next_update_) {
      return;
    }
    next_update_ = now + std::chrono::milliseconds(250);
    const uint64_t percent = total == 0 ? 100 : (completed * 100) / total;
    std::cerr << '\r' << phase << ": " << percent << "% (" << completed << " / " << total << ')'
              << std::flush;
  }

  void Complete(std::string_view phase, uint64_t completed, uint64_t total) {
    Update(phase, completed, total, true);
    if (enabled_) {
      std::cerr << '\n';
    }
  }

  void Message(std::string_view message) {
    if (enabled_) {
      std::cerr << "\r" << message << std::string(24, ' ') << '\n' << std::flush;
    }
  }

 private:
  bool enabled_ = false;
  std::chrono::steady_clock::time_point next_update_{};
};

class CaptureReader {
 public:
  CaptureReader(const char* filename, ProgressReporter* progress)
      : file_(tracy::FileRead::Open(filename)), progress_(progress) {
    if (file_ == nullptr) {
      throw std::runtime_error("cannot open capture");
    }
  }

  ~CaptureReader() {
    delete file_;
  }

  void ExportFiberIntervals(std::ofstream* interval_spool,
                            const std::filesystem::path& zone_candidate_spool_path,
                            std::ofstream* zone_spool, bool include_subzones,
                            const std::optional<TimeWindow>& time_window) {
    progress_->Message("read capture metadata");
    ReadAndCheckHeader();
    SkipCapturePrefix();
    {
      std::ofstream zone_candidates(zone_candidate_spool_path, std::ios::binary);
      if (!zone_candidates) {
        throw std::runtime_error("cannot create zone candidate spool");
      }
      ReadThreads(&zone_candidates, include_subzones);
    }
    FilterZoneSpool(zone_candidate_spool_path, zone_spool, time_window);
    progress_->Message("skip GPU, plots, memory, and frame images");
    SkipGpuAndOtherDataBeforeContextSwitches();
    progress_->Message("read persisted fiber schedules");
    ReadFiberContextSwitches(interval_spool, time_window);
  }

  const std::unordered_map<uint64_t, std::string>& thread_names() const {
    return thread_names_;
  }
  const std::unordered_map<int16_t, std::string>& zone_names() const {
    return zone_names_;
  }
  const std::vector<std::string>& zone_metadata() const {
    return zone_metadata_;
  }
  const std::optional<int64_t>& earliest_interval_start() const {
    return earliest_interval_start_;
  }
  const std::optional<int64_t>& latest_interval_end() const {
    return latest_interval_end_;
  }

 private:
  template <typename T> void Read(T* value) {
    file_->Read(*value);
  }

  void Skip(uint64_t bytes) {
    while (bytes != 0) {
      const auto chunk = std::min<uint64_t>(bytes, std::numeric_limits<size_t>::max());
      file_->Skip(static_cast<size_t>(chunk));
      bytes -= chunk;
    }
  }

  uint64_t ReadCount() {
    uint64_t count{};
    Read(&count);
    return count;
  }

  void SkipBytesFor(uint64_t count, uint64_t item_size) {
    if ((item_size != 0) && (count > (std::numeric_limits<uint64_t>::max() / item_size))) {
      throw std::runtime_error("capture count overflows byte size");
    }
    Skip(count * item_size);
  }

  void ReadAndCheckHeader() {
    uint8_t header[8]{};
    file_->Read(header, sizeof(header));
    static constexpr uint8_t kMagic[] = {'t', 'r', 'a', 'c', 'y'};
    if (!std::equal(std::begin(kMagic), std::end(kMagic), std::begin(header))) {
      throw std::runtime_error("not a Tracy saved capture");
    }
    const int version = tracy::FileVersion(header[5], header[6], header[7]);
    if (version != kExpectedVersion) {
      throw std::runtime_error("only Tracy v0.14.1 captures are supported");
    }
  }

  void SkipString() {
    const uint64_t size = ReadCount();
    Skip(size);
  }

  void SkipTopology() {
    for (uint64_t packages = ReadCount(); packages-- != 0;) {
      Skip(sizeof(uint32_t));
      for (uint64_t dies = ReadCount(); dies-- != 0;) {
        Skip(sizeof(uint32_t));
        for (uint64_t cores = ReadCount(); cores-- != 0;) {
          Skip(sizeof(uint32_t));
          SkipBytesFor(ReadCount(), sizeof(uint32_t));
        }
      }
    }
  }

  void SkipFrames() {
    for (uint64_t frames = ReadCount(); frames-- != 0;) {
      uint8_t continuous{};
      Skip(sizeof(uint64_t));
      Read(&continuous);
      const uint64_t events = ReadCount();
      SkipBytesFor(events, sizeof(int64_t) * (continuous == 0 ? 2 : 1) + sizeof(int32_t));
    }
  }

  void SkipSections() {
    for (uint64_t sections = ReadCount(); sections-- != 0;) {
      Skip(sizeof(uint16_t));
      SkipBytesFor(ReadCount(), sizeof(tracy::SectionItem));
    }
    SkipBytesFor(ReadCount(), sizeof(uint16_t) + sizeof(tracy::StringIdx));
  }

  void ReadThreadNames() {
    for (uint64_t strings = ReadCount(); strings-- != 0;) {
      uint64_t pointer{};
      Read(&pointer);
      const uint64_t size = ReadCount();
      std::string value(size, '\0');
      if (size != 0) {
        file_->Read(value.data(), size);
      }
      strings_by_pointer_.emplace(pointer, value);
      string_data_.push_back(std::move(value));
    }
    for (uint64_t strings = ReadCount(); strings-- != 0;) {
      uint64_t id{};
      uint64_t pointer{};
      Read(&id);
      Read(&pointer);
      const auto found = strings_by_pointer_.find(pointer);
      if (found != strings_by_pointer_.end()) {
        strings_by_id_.emplace(id, found->second);
      }
    }
    for (uint64_t names = ReadCount(); names-- != 0;) {
      uint64_t id{};
      uint64_t pointer{};
      Read(&id);
      Read(&pointer);
      const auto found = strings_by_pointer_.find(pointer);
      if (found != strings_by_pointer_.end()) {
        thread_names_.emplace(id, found->second);
      }
    }
    SkipBytesFor(ReadCount(), sizeof(uint64_t) * 3);  // external names
    SkipBytesFor(ReadCount(), sizeof(uint64_t));      // Local thread-compression table.
    SkipBytesFor(ReadCount(), sizeof(uint64_t));      // External thread-compression table.
  }

  void ReadSourceLocations() {
    for (uint64_t locations = ReadCount(); locations-- != 0;) {
      uint64_t pointer{};
      tracy::SourceLocationBase location{};
      Read(&pointer);
      Read(&location);
      source_locations_.emplace(pointer, location);
    }
    const uint64_t expansion_count = ReadCount();
    source_location_expand_.resize(expansion_count);
    if (expansion_count != 0) {
      file_->Read(source_location_expand_.data(), expansion_count * sizeof(uint64_t));
    }
    const uint64_t payload_count = ReadCount();
    source_location_payload_.resize(payload_count);
    if (payload_count != 0) {
      file_->Read(source_location_payload_.data(),
                  payload_count * sizeof(tracy::SourceLocationBase));
    }
    SkipBytesFor(ReadCount(), sizeof(int16_t) + sizeof(uint64_t));
    SkipBytesFor(ReadCount(), sizeof(int16_t) + sizeof(uint64_t));
  }

  void ReadLocksMessagesAndZoneExtras() {
    for (uint64_t locks = ReadCount(); locks-- != 0;) {
      Skip(sizeof(uint32_t) + sizeof(tracy::StringIdx) + sizeof(int16_t));
      Skip(sizeof(uint8_t));
      Skip(sizeof(bool) + sizeof(int64_t) * 2);
      SkipBytesFor(ReadCount(), sizeof(uint64_t));
      const uint64_t events = ReadCount();
      SkipBytesFor(events, sizeof(int64_t) + sizeof(int16_t) + sizeof(uint8_t) * 2);
    }
    SkipBytesFor(ReadCount(), sizeof(uint64_t) + sizeof(int64_t) + sizeof(tracy::StringRef) +
                                  sizeof(uint32_t) + sizeof(tracy::Int24) +
                                  sizeof(tracy::MessageSourceType) +
                                  sizeof(tracy::MessageSeverity));
    const uint64_t zone_extra_count = ReadCount();
    zone_extras_.resize(zone_extra_count);
    if (zone_extra_count != 0) {
      file_->Read(zone_extras_.data(), zone_extra_count * sizeof(tracy::ZoneExtra));
    }
    zone_metadata_.reserve(zone_extras_.size());
    for (const tracy::ZoneExtra& extra : zone_extras_) {
      zone_metadata_.push_back(ResolveString(extra.text));
    }
  }

  std::string ResolveString(const tracy::StringRef& reference) const {
    if (reference.isidx != 0) {
      if (reference.str < string_data_.size()) {
        return string_data_[reference.str];
      }
      return {};
    }
    if (reference.active == 0) {
      return {};
    }
    const auto found = strings_by_id_.find(reference.str);
    return found == strings_by_id_.end() ? std::string{} : found->second;
  }

  std::string ResolveString(const tracy::StringIdx& reference) const {
    if (!reference.Active() || (reference.Idx() >= string_data_.size())) {
      return {};
    }
    return string_data_[reference.Idx()];
  }

  std::string ResolveZoneName(int16_t source_location) const {
    const tracy::SourceLocationBase* location = nullptr;
    if (source_location < 0) {
      const size_t index = static_cast<size_t>(-source_location - 1);
      if (index < source_location_payload_.size()) {
        location = &source_location_payload_[index];
      }
    } else if ((source_location != std::numeric_limits<int16_t>::max()) &&
               (static_cast<size_t>(source_location) < source_location_expand_.size())) {
      const auto found = source_locations_.find(source_location_expand_[source_location]);
      if (found != source_locations_.end()) {
        location = &found->second;
      }
    }
    if (location == nullptr) {
      return {};
    }
    return location->name.active != 0 ? ResolveString(location->name)
                                      : ResolveString(location->function);
  }

  void ReadZoneTimeline(uint32_t zone_count, uint64_t fiber_thread_id, std::ofstream* zone_spool,
                        int64_t* reference_time, uint16_t level, bool include_subzones) {
    for (uint32_t zone_index = 0; zone_index < zone_count; ++zone_index) {
      int16_t source_location{};
      int64_t start_delta{};
      uint32_t extra{};
      uint32_t child_count{};
      Read(&source_location);
      Read(&start_delta);
      Read(&extra);
      Read(&child_count);
      *reference_time += start_delta;
      const int64_t start_ns = *reference_time;
      const bool write_zone = include_subzones || (level == 1);
      const std::streampos end_offset = zone_spool->tellp();
      if (write_zone) {
        const Zone placeholder{start_ns, 0, fiber_thread_id, extra, source_location, level};
        zone_names_.try_emplace(source_location, ResolveZoneName(source_location));
        zone_spool->write(reinterpret_cast<const char*>(&placeholder), sizeof(placeholder));
        if (!*zone_spool) {
          throw std::runtime_error("cannot write zone spool");
        }
      }
      ReadZoneTimeline(child_count, fiber_thread_id, zone_spool, reference_time,
                       static_cast<uint16_t>(level + 1), include_subzones);
      int64_t end_delta{};
      Read(&end_delta);
      *reference_time += end_delta;
      if (write_zone) {
        const std::streampos resume_offset = zone_spool->tellp();
        zone_spool->seekp(end_offset + static_cast<std::streamoff>(sizeof(int64_t)));
        zone_spool->write(reinterpret_cast<const char*>(&*reference_time), sizeof(*reference_time));
        zone_spool->seekp(resume_offset);
        if (!*zone_spool) {
          throw std::runtime_error("cannot patch zone spool");
        }
      }
    }
  }

  void ReadRootZoneTimeline(uint64_t fiber_thread_id, std::ofstream* zone_spool,
                            int64_t* reference_time, bool include_subzones) {
    uint32_t root_count{};
    Read(&root_count);
    ReadZoneTimeline(root_count, fiber_thread_id, zone_spool, reference_time, 1, include_subzones);
  }

  void FilterZoneSpool(const std::filesystem::path& zone_candidate_spool_path,
                       std::ofstream* zone_spool, const std::optional<TimeWindow>& time_window) {
    std::ifstream zone_candidates(zone_candidate_spool_path, std::ios::binary);
    if (!zone_candidates) {
      throw std::runtime_error("cannot open zone candidate spool");
    }
    Zone zone{};
    while (zone_candidates.read(reinterpret_cast<char*>(&zone), sizeof(zone))) {
      if (time_window && !time_window->Overlaps(zone.start_ns, zone.end_ns)) {
        continue;
      }
      zone_spool->write(reinterpret_cast<const char*>(&zone), sizeof(zone));
      if (!*zone_spool) {
        throw std::runtime_error("cannot write zone spool");
      }
    }
    if (!zone_candidates.eof()) {
      throw std::runtime_error("cannot read zone candidate spool");
    }
  }

  void ReadThreads(std::ofstream* zone_spool, bool include_subzones) {
    const uint64_t total_zone_count = ReadCount();
    Skip(sizeof(uint64_t));  // zone child-vector count
    const uint64_t thread_count = ReadCount();
    uint64_t skipped_zone_count = 0;
    for (uint64_t threads = thread_count; threads-- != 0;) {
      uint64_t thread_id{};
      Read(&thread_id);
      const uint64_t thread_zone_count = ReadCount();
      Skip(sizeof(uint64_t));
      Skip(sizeof(uint8_t));
      Skip(sizeof(int32_t));
      int64_t reference_time{};
      ReadRootZoneTimeline(thread_id, zone_spool, &reference_time, include_subzones);
      skipped_zone_count += thread_zone_count;
      progress_->Update("decode zones", skipped_zone_count, total_zone_count);
      SkipBytesFor(ReadCount(), sizeof(uint64_t));
      SkipBytesFor(ReadCount(), sizeof(int64_t) + sizeof(tracy::Int24));
      SkipBytesFor(ReadCount(), sizeof(int64_t) + sizeof(tracy::Int24));
    }
    progress_->Complete("decode zones", skipped_zone_count, total_zone_count);
  }

  void SkipGpuTimeline() {
    struct TimelineFrame {
      uint64_t remaining;
      bool needs_end;
    };
    const uint64_t root_count = ReadCount();
    std::vector<TimelineFrame> frames{{root_count, false}};
    while (!frames.empty()) {
      auto& frame = frames.back();
      if (frame.remaining == 0) {
        const bool needs_end = frame.needs_end;
        frames.pop_back();
        if (needs_end) {
          Skip(sizeof(int64_t) * 2 + sizeof(uint16_t));
        }
        continue;
      }
      --frame.remaining;
      Skip(sizeof(int64_t) * 2 + sizeof(int16_t) + sizeof(tracy::Int24) + sizeof(uint16_t));
      const uint64_t child_count = ReadCount();
      if (child_count == 0) {
        Skip(sizeof(int64_t) * 2 + sizeof(uint16_t));
      } else {
        frames.push_back({child_count, true});
      }
    }
  }

  void SkipGpuAndOtherDataBeforeContextSwitches() {
    Skip(sizeof(uint64_t) * 2);  // GPU zones and child-vector count
    for (uint64_t contexts = ReadCount(); contexts-- != 0;) {
      Skip(sizeof(uint64_t) + sizeof(uint8_t) + sizeof(uint64_t) + sizeof(float) +
           sizeof(tracy::GpuContextType) + sizeof(tracy::StringIdx) + sizeof(uint64_t));
      SkipBytesFor(ReadCount(), sizeof(int64_t) + sizeof(tracy::StringIdx));
      for (uint64_t thread_data = ReadCount(); thread_data-- != 0;) {
        Skip(sizeof(uint64_t));
        SkipGpuTimeline();
      }
      for (uint64_t notes = ReadCount(); notes-- != 0;) {
        Skip(sizeof(uint16_t));
        SkipBytesFor(ReadCount(), sizeof(int64_t) + sizeof(double));
      }
    }

    for (uint64_t plots = ReadCount(); plots-- != 0;) {
      Skip(sizeof(tracy::PlotType) + sizeof(tracy::PlotValueFormatting) + sizeof(uint8_t) * 2 +
           sizeof(uint32_t) + sizeof(uint64_t) + sizeof(double) * 3);
      SkipBytesFor(ReadCount(), sizeof(int64_t) + sizeof(double));
    }

    uint64_t memory_group_count{};
    uint64_t memory_event_count{};
    Read(&memory_group_count);
    Read(&memory_event_count);
    for (uint64_t memory_group = 0; memory_group < memory_group_count; ++memory_group) {
      Skip(sizeof(uint64_t));
      const uint64_t entries = ReadCount();
      Skip(sizeof(uint64_t) * 2);
      SkipBytesFor(entries, sizeof(uint64_t) * 2 + sizeof(tracy::Int24) * 2 + sizeof(int64_t) * 2 +
                                sizeof(uint16_t) * 2);
      Skip(sizeof(uint64_t) * 4);
      progress_->Update("skip memory events", memory_group + 1, memory_group_count);
    }
    progress_->Complete("skip memory events", memory_group_count, memory_group_count);

    for (uint64_t callstacks = ReadCount(); callstacks-- != 0;) {
      uint16_t frames{};
      Read(&frames);
      SkipBytesFor(frames, sizeof(tracy::CallstackFrameId));
    }
    for (uint64_t frames = ReadCount(); frames-- != 0;) {
      uint8_t frame_count{};
      Skip(sizeof(tracy::CallstackFrameId));
      Read(&frame_count);
      Skip(sizeof(tracy::StringIdx));
      SkipBytesFor(frame_count, sizeof(tracy::CallstackFrame));
    }
    SkipBytesFor(ReadCount(), sizeof(tracy::StringRef));

    uint32_t dictionary_size{};
    Read(&dictionary_size);
    Skip(dictionary_size);
    for (uint64_t images = ReadCount(); images-- != 0;) {
      uint16_t width{};
      uint16_t height{};
      Read(&width);
      Read(&height);
      Skip(sizeof(uint8_t));
      Skip(static_cast<uint64_t>(width) * height / 2);
    }
  }

  void SkipCapturePrefix() {
    Skip(sizeof(int64_t) * 4 + sizeof(double) + sizeof(uint64_t) + sizeof(uint8_t) +
         sizeof(uint32_t) + 12 + sizeof(uint8_t));
    SkipString();
    SkipString();
    Skip(sizeof(uint64_t) * 2);
    SkipString();
    SkipTopology();
    Skip(sizeof(tracy::CrashEvent));
    SkipFrames();
    SkipSections();
    ReadThreadNames();
    ReadSourceLocations();
    ReadLocksMessagesAndZoneExtras();
  }

  void ReadFiberContextSwitches(std::ofstream* spool,
                                const std::optional<TimeWindow>& time_window) {
    const uint64_t group_count = ReadCount();
    for (uint64_t group = 0; group < group_count; ++group) {
      uint64_t fiber_thread_id{};
      Read(&fiber_thread_id);
      const uint64_t records = ReadCount();
      int64_t reference_time{};
      for (uint64_t record = 0; record < records; ++record) {
        int64_t wakeup_delta{};
        int64_t start_delta{};
        int64_t end_delta{};
        int8_t reason{};
        uint64_t owner_thread_id{};
        Read(&wakeup_delta);
        Read(&start_delta);
        Read(&end_delta);
        Skip(sizeof(uint8_t));
        Read(&reason);
        Skip(sizeof(int8_t));
        Read(&owner_thread_id);
        Skip(sizeof(uint8_t));
        reference_time += wakeup_delta;
        reference_time += start_delta;
        const int64_t start_ns = reference_time;
        reference_time += end_delta;
        const int64_t end_ns = reference_time;
        if ((reason == kFiberReason) && (end_ns >= start_ns)) {
          if (!earliest_interval_start_ || (start_ns < *earliest_interval_start_)) {
            earliest_interval_start_ = start_ns;
          }
          if (!latest_interval_end_ || (end_ns > *latest_interval_end_)) {
            latest_interval_end_ = end_ns;
          }
          if (time_window && !time_window->Overlaps(start_ns, end_ns)) {
            continue;
          }
          const Interval interval{start_ns, end_ns, owner_thread_id, fiber_thread_id};
          spool->write(reinterpret_cast<const char*>(&interval), sizeof(interval));
          if (!*spool) {
            throw std::runtime_error("cannot write interval spool");
          }
        }
      }
      progress_->Update("read fiber intervals", group + 1, group_count);
    }
    progress_->Complete("read fiber intervals", group_count, group_count);
  }

  tracy::FileRead* file_;
  ProgressReporter* progress_;
  std::unordered_map<uint64_t, std::string> strings_by_pointer_;
  std::vector<std::string> string_data_;
  std::unordered_map<uint64_t, std::string> strings_by_id_;
  std::unordered_map<uint64_t, std::string> thread_names_;
  std::unordered_map<int16_t, std::string> zone_names_;
  std::vector<tracy::ZoneExtra> zone_extras_;
  std::vector<std::string> zone_metadata_;
  std::unordered_map<uint64_t, tracy::SourceLocationBase> source_locations_;
  std::vector<uint64_t> source_location_expand_;
  std::vector<tracy::SourceLocationBase> source_location_payload_;
  std::optional<int64_t> earliest_interval_start_;
  std::optional<int64_t> latest_interval_end_;
};

struct ZoneRange {
  uint64_t offset{};
  uint64_t count{};
  uint64_t next{};
};

class ZoneReader {
 public:
  explicit ZoneReader(const std::filesystem::path& spool_path)
      : spool_(spool_path, std::ios::binary) {
    if (!spool_) {
      throw std::runtime_error("cannot open zone spool");
    }
    const uint64_t zone_count = std::filesystem::file_size(spool_path) / sizeof(Zone);
    for (uint64_t index = 0; index < zone_count; ++index) {
      Zone zone{};
      spool_.read(reinterpret_cast<char*>(&zone), sizeof(zone));
      if (!spool_) {
        throw std::runtime_error("cannot read zone spool");
      }
      auto [range, inserted] =
          ranges_.try_emplace(zone.fiber_thread_id, ZoneRange{index, 0, index});
      if (!inserted && ((range->second.offset + range->second.count) != index)) {
        throw std::runtime_error("zone spool is not grouped by thread");
      }
      ++range->second.count;
    }
  }

  std::vector<Zone> Overlapping(uint64_t fiber_thread_id, int64_t start_ns, int64_t end_ns) {
    const auto found = ranges_.find(fiber_thread_id);
    if (found == ranges_.end()) {
      return {};
    }
    auto& range = found->second;
    auto& active = active_zones_[fiber_thread_id];
    std::erase_if(active, [start_ns](const Zone& zone) { return zone.end_ns <= start_ns; });
    while (range.next < (range.offset + range.count)) {
      const Zone zone = ReadAt(range.next);
      if (zone.start_ns >= end_ns) {
        break;
      }
      ++range.next;
      if (zone.end_ns > start_ns) {
        active.push_back(zone);
      }
    }
    std::vector<Zone> zones;
    zones.reserve(active.size());
    for (const Zone& zone : active) {
      if ((zone.start_ns < end_ns) && (zone.end_ns > start_ns)) {
        zones.push_back(zone);
      }
    }
    std::sort(zones.begin(), zones.end(), ZoneLess);
    return zones;
  }

 private:
  Zone ReadAt(uint64_t index) {
    const auto offset = static_cast<std::streamoff>(index * sizeof(Zone));
    spool_.clear();
    spool_.seekg(offset);
    Zone zone{};
    spool_.read(reinterpret_cast<char*>(&zone), sizeof(zone));
    if (!spool_) {
      throw std::runtime_error("cannot read zone spool");
    }
    return zone;
  }

  std::ifstream spool_;
  std::unordered_map<uint64_t, ZoneRange> ranges_;
  std::unordered_map<uint64_t, std::vector<Zone>> active_zones_;
};

void PrintUsage(std::ostream& stream, const char* program) {
  stream
      << "Export a bounded-memory scheduler timeline from a Tracy v0.14.1 capture.\n\n"
      << "Usage:\n"
      << "  " << program << " <capture.tracy> <timeline.csv> [options]\n\n"
      << "Required arguments:\n"
      << "  capture.tracy              Saved Tracy v0.14.1 capture.\n"
      << "  timeline.csv               Destination for the global long-format timeline.\n\n"
      << "Time window options (capture-relative nanoseconds):\n"
      << "  --start-ns N --end-ns N    Include intervals overlapping [start, end).\n"
      << "  --start-ns N --duration-ns N\n"
      << "                            Include intervals overlapping [start, start + duration).\n"
      << "                            An overlapping interval keeps its full duration.\n\n"
      << "                            The capture is decoded sequentially in full; out-of-window\n"
      << "                            records are discarded before sorting and merging.\n\n"
      << "Memory and temporary storage:\n"
      << "  --memory-records N         Intervals per in-memory sort chunk (default: 1000000).\n"
      << "                            Each record is 32 bytes before sort/vector overhead.\n"
      << "  --work-dir DIR             Directory for the binary spool and sorted chunks.\n"
      << "                            Default: <timeline.csv>.tracy-scheduler-work.\n"
      << "                            Removed after a successful export; use a fast local disk.\n\n"
      << "Other options:\n"
      << "  --include-subzones         Include nested CPU zones; rows may overlap by zone level.\n"
      << "  --progress                 Print decode, sort, and merge progress to stderr.\n"
      << "  -h, --help                 Show this help text.\n\n"
      << "Output columns:\n"
      << "  start_time,end_time,duration,proactor,fiber,zone_level,zone,zone_metadata\n\n"
      << "The exporter reads persisted Fiber ContextSwitchData only. It does not load or\n"
      << "reconstruct fibers without saved context switch records. By default the zone column\n"
      << "contains only outermost CPU zones. --include-subzones adds every active nested zone;\n"
      << "zone_level is 1 for roots, depth + 1 for children, and 0 for scheduler gaps.\n"
      << "zone_metadata contains the persisted ZoneText or ZoneTextF text for its zone.\n";
}

}  // namespace

int main(int argc, char* argv[]) {
  if ((argc == 2) && (std::string_view(argv[1]) == "--help" || std::string_view(argv[1]) == "-h")) {
    PrintUsage(std::cout, argv[0]);
    return 0;
  }
  if (argc < 3) {
    PrintUsage(std::cerr, argv[0]);
    return 2;
  }
  std::filesystem::path capture = argv[1];
  std::filesystem::path output = argv[2];
  std::filesystem::path work_dir = output.string() + ".tracy-scheduler-work";
  uint64_t memory_records = 1'000'000;
  std::optional<int64_t> start_ns;
  std::optional<int64_t> end_ns;
  std::optional<int64_t> duration_ns;
  bool progress_enabled = false;
  bool include_subzones = false;

  for (int index = 3; index < argc;) {
    const std::string option = argv[index++];
    if (option == "--progress") {
      progress_enabled = true;
      continue;
    }
    if (option == "--include-subzones") {
      include_subzones = true;
      continue;
    }
    if (index >= argc) {
      PrintUsage(std::cerr, argv[0]);
      return 2;
    }
    const char* value = argv[index++];
    if (option == "--work-dir") {
      work_dir = value;
    } else if (option == "--memory-records") {
      try {
        memory_records = std::stoull(value);
      } catch (const std::exception&) {
        std::cerr << "invalid --memory-records value\n";
        return 2;
      }
      if (memory_records == 0) {
        std::cerr << "--memory-records must be positive\n";
        return 2;
      }
    } else if (option == "--start-ns" || option == "--end-ns" || option == "--duration-ns") {
      try {
        const int64_t time_value = std::stoll(value);
        if (time_value < 0) {
          throw std::out_of_range("negative time");
        }
        if (option == "--start-ns") {
          start_ns = time_value;
        } else if (option == "--end-ns") {
          end_ns = time_value;
        } else {
          duration_ns = time_value;
        }
      } catch (const std::exception&) {
        std::cerr << "invalid " << option << " value: " << value << '\n';
        return 2;
      }
    } else {
      PrintUsage(std::cerr, argv[0]);
      return 2;
    }
  }

  if (duration_ns && !start_ns) {
    std::cerr << "--duration-ns requires --start-ns\n";
    return 2;
  }
  if (end_ns && duration_ns) {
    std::cerr << "use either --end-ns or --duration-ns, not both\n";
    return 2;
  }
  if ((start_ns && !end_ns && !duration_ns) || (!start_ns && (end_ns || duration_ns))) {
    std::cerr << "a time window requires --start-ns and either --end-ns or --duration-ns\n";
    return 2;
  }
  if (duration_ns && *duration_ns == 0) {
    std::cerr << "--duration-ns must be positive\n";
    return 2;
  }
  if (duration_ns) {
    if (*start_ns > std::numeric_limits<int64_t>::max() - *duration_ns) {
      std::cerr << "time window overflows int64 nanoseconds\n";
      return 2;
    }
    end_ns = *start_ns + *duration_ns;
  }
  if (start_ns && end_ns && *end_ns <= *start_ns) {
    std::cerr << "--end-ns must be greater than --start-ns\n";
    return 2;
  }
  std::optional<TimeWindow> time_window;
  if (start_ns) {
    time_window = TimeWindow{*start_ns, *end_ns};
  }

  try {
    if (std::filesystem::exists(work_dir)) {
      throw std::runtime_error("work directory already exists: " + work_dir.string());
    }
    std::filesystem::create_directories(work_dir);
    ProgressReporter progress(progress_enabled);
    const auto spool_path = work_dir / "intervals.bin";
    const auto zone_candidate_spool_path = work_dir / "zone-candidates.bin";
    const auto zone_spool_path = work_dir / "zones.bin";
    std::unordered_map<uint64_t, std::string> thread_names;
    std::unordered_map<int16_t, std::string> zone_names;
    std::vector<std::string> zone_metadata;
    std::optional<int64_t> earliest_interval_start;
    std::optional<int64_t> latest_interval_end;
    {
      std::ofstream spool(spool_path, std::ios::binary);
      std::ofstream zone_spool(zone_spool_path, std::ios::binary);
      if (!spool || !zone_spool) {
        throw std::runtime_error("cannot create interval spool");
      }
      CaptureReader reader(capture.c_str(), &progress);
      reader.ExportFiberIntervals(&spool, zone_candidate_spool_path, &zone_spool, include_subzones,
                                  time_window);
      thread_names = reader.thread_names();
      zone_names = reader.zone_names();
      zone_metadata = reader.zone_metadata();
      earliest_interval_start = reader.earliest_interval_start();
      latest_interval_end = reader.latest_interval_end();
    }

    std::ifstream spool(spool_path, std::ios::binary);
    const uint64_t interval_count = std::filesystem::file_size(spool_path) / sizeof(Interval);
    std::vector<std::filesystem::path> runs;
    uint64_t sorted_interval_count = 0;
    while (spool) {
      std::vector<Interval> chunk;
      chunk.reserve(static_cast<size_t>(memory_records));
      Interval interval{};
      while ((chunk.size() < memory_records) &&
             spool.read(reinterpret_cast<char*>(&interval), sizeof(interval))) {
        chunk.push_back(interval);
      }
      if (chunk.empty()) {
        break;
      }
      sorted_interval_count += chunk.size();
      std::sort(chunk.begin(), chunk.end(), IntervalLess);
      const auto run_path = work_dir / ("run-" + std::to_string(runs.size()) + ".bin");
      std::ofstream run(run_path, std::ios::binary);
      run.write(reinterpret_cast<const char*>(chunk.data()),
                static_cast<std::streamsize>(chunk.size() * sizeof(Interval)));
      if (!run) {
        throw std::runtime_error("cannot write sorted run");
      }
      runs.push_back(run_path);
      progress.Update("sort intervals", sorted_interval_count, interval_count);
    }
    progress.Complete("sort intervals", sorted_interval_count, interval_count);

    ZoneReader zones(zone_spool_path);

    std::filesystem::create_directories(output.parent_path().empty() ? "." : output.parent_path());
    std::ofstream csv(output);
    if (!csv) {
      throw std::runtime_error("cannot create output CSV");
    }
    csv << "start_time,end_time,duration,proactor,fiber,zone_level,zone,zone_metadata\n";

    struct RunItem {
      Interval interval;
      size_t run_index;
    };
    auto later = [](const RunItem& lhs, const RunItem& rhs) {
      return IntervalLess(rhs.interval, lhs.interval);
    };
    std::priority_queue<RunItem, std::vector<RunItem>, decltype(later)> pending_intervals(later);
    std::vector<std::ifstream> inputs(runs.size());
    for (size_t index = 0; index < runs.size(); ++index) {
      inputs[index].open(runs[index], std::ios::binary);
      Interval interval{};
      if (inputs[index].read(reinterpret_cast<char*>(&interval), sizeof(interval))) {
        pending_intervals.push({interval, index});
      }
    }
    uint64_t merged_interval_count = 0;
    uint64_t written_interval_count = 0;
    auto segment_later = [](const Segment& lhs, const Segment& rhs) {
      return SegmentLess(rhs, lhs);
    };
    std::priority_queue<Segment, std::vector<Segment>, decltype(segment_later)> pending_segments(
        segment_later);
    const auto write_segment = [&csv, &thread_names, &zone_metadata,
                                &zone_names](const Segment& segment) {
      const auto proactor = thread_names.find(segment.interval.owner_thread_id);
      const auto fiber = thread_names.find(segment.interval.fiber_thread_id);
      std::string_view zone_name;
      std::string_view metadata;
      if (segment.zone_level != 0) {
        const auto name = zone_names.find(segment.source_location);
        zone_name = name == zone_names.end() ? std::string_view{} : name->second;
        metadata = segment.extra < zone_metadata.size() ? zone_metadata[segment.extra]
                                                        : std::string_view{};
      }
      csv << CsvField(FormatTimestamp(segment.start_ns)) << ','
          << CsvField(FormatTimestamp(segment.end_ns)) << ','
          << CsvField(FormatDuration(segment.end_ns - segment.start_ns)) << ','
          << CsvField(proactor == thread_names.end() ? "unnamed proactor" : proactor->second) << ','
          << CsvField(fiber == thread_names.end() ? "unnamed fiber" : fiber->second) << ','
          << segment.zone_level << ',' << CsvField(zone_name) << ',' << CsvField(metadata) << '\n';
    };
    const auto queue_segment =
        [&pending_segments](const Interval& interval, int64_t segment_start, int64_t segment_end,
                            uint16_t zone_level, int16_t source_location = 0,
                            uint32_t extra = std::numeric_limits<uint32_t>::max()) {
          pending_segments.push(
              {interval, segment_start, segment_end, extra, source_location, zone_level});
        };
    const auto flush_segments = [&pending_segments,
                                 &write_segment](std::optional<int64_t> next_start) {
      while (!pending_segments.empty() &&
             (!next_start || (pending_segments.top().start_ns < *next_start))) {
        write_segment(pending_segments.top());
        pending_segments.pop();
      }
    };
    while (!pending_intervals.empty()) {
      const RunItem item = pending_intervals.top();
      pending_intervals.pop();
      ++merged_interval_count;
      const auto& interval = item.interval;
      if (!start_ns || (interval.end_ns > *start_ns && interval.start_ns < *end_ns)) {
        ++written_interval_count;
        if (interval.start_ns == interval.end_ns) {
          queue_segment(interval, interval.start_ns, interval.end_ns, 0);
        } else {
          const std::vector<Zone> overlapping =
              zones.Overlapping(interval.fiber_thread_id, interval.start_ns, interval.end_ns);
          if (!include_subzones) {
            int64_t segment_start = interval.start_ns;
            for (const Zone& zone : overlapping) {
              const int64_t zone_start = std::max(interval.start_ns, zone.start_ns);
              const int64_t zone_end = std::min(interval.end_ns, zone.end_ns);
              if (segment_start < zone_start) {
                queue_segment(interval, segment_start, zone_start, 0);
              }
              if (zone_start < zone_end) {
                queue_segment(interval, zone_start, zone_end, zone.level, zone.source_location,
                              zone.extra);
              }
              segment_start = std::max(segment_start, zone_end);
            }
            if (segment_start < interval.end_ns) {
              queue_segment(interval, segment_start, interval.end_ns, 0);
            }
          } else {
            std::vector<int64_t> boundaries{interval.start_ns, interval.end_ns};
            for (const Zone& zone : overlapping) {
              boundaries.push_back(std::max(interval.start_ns, zone.start_ns));
              boundaries.push_back(std::min(interval.end_ns, zone.end_ns));
            }
            std::sort(boundaries.begin(), boundaries.end());
            boundaries.erase(std::unique(boundaries.begin(), boundaries.end()), boundaries.end());
            for (size_t boundary_index = 1; boundary_index < boundaries.size(); ++boundary_index) {
              const int64_t segment_start = boundaries[boundary_index - 1];
              const int64_t segment_end = boundaries[boundary_index];
              bool wrote_zone = false;
              for (const Zone& zone : overlapping) {
                if ((zone.start_ns > segment_start) || (zone.end_ns < segment_end)) {
                  continue;
                }
                queue_segment(interval, segment_start, segment_end, zone.level,
                              zone.source_location, zone.extra);
                wrote_zone = true;
              }
              if (!wrote_zone) {
                queue_segment(interval, segment_start, segment_end, 0);
              }
            }
          }
        }
      }
      Interval next{};
      if (inputs[item.run_index].read(reinterpret_cast<char*>(&next), sizeof(next))) {
        pending_intervals.push({next, item.run_index});
      }
      flush_segments(pending_intervals.empty()
                         ? std::nullopt
                         : std::optional<int64_t>{pending_intervals.top().interval.start_ns});
      progress.Update("merge CSV", merged_interval_count, interval_count);
    }
    flush_segments(std::nullopt);
    progress.Complete("merge CSV", merged_interval_count, interval_count);
    if (progress_enabled) {
      std::cerr << "wrote scheduler intervals: " << written_interval_count << '\n';
    }
    if (!csv) {
      throw std::runtime_error("cannot write output CSV");
    }
    csv.close();
    if (start_ns && written_interval_count == 0) {
      std::filesystem::remove(output);
      if (earliest_interval_start && latest_interval_end) {
        throw std::runtime_error(
            "no scheduler activity in requested time window:\n"
            "  requested: " +
            FormatTimestamp(*start_ns) + " to " + FormatTimestamp(*end_ns) +
            "\n"
            "  recorded:  " +
            FormatTimestamp(*earliest_interval_start) + " to " +
            FormatTimestamp(*latest_interval_end));
      }
      throw std::runtime_error("capture contains no saved scheduler intervals");
    }
    std::filesystem::remove_all(work_dir);
  } catch (const std::exception& error) {
    std::cerr << "\033[31merror:\033[0m " << error.what() << '\n';
    return 1;
  }
  return 0;
}
