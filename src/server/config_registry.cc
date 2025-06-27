// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/config_registry.h"

#include <absl/flags/reflection.h>
#include <absl/strings/str_replace.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <sstream>

#include "base/logging.h"
#include "core/glob_matcher.h"
#include "server/common.h"
#include "server/server_family.h"

namespace dfly {

// Forward declaration of global variable
extern std::string g_config_file_path;

namespace {
using namespace std;

string NormalizeConfigName(string_view name) {
  return absl::StrReplaceAll(name, {{"-", "_"}});
}
}  // namespace

// Returns true if the value was updated.
auto ConfigRegistry::Set(string_view config_name, string_view value) -> SetResult {
  string name = NormalizeConfigName(config_name);

  util::fb2::LockGuard lk(mu_);
  auto it = registry_.find(name);
  if (it == registry_.end())
    return SetResult::UNKNOWN;
  if (!it->second.is_mutable)
    return SetResult::READONLY;

  auto cb = it->second.cb;

  absl::CommandLineFlag* flag = absl::FindCommandLineFlag(name);
  CHECK(flag) << config_name;
  if (string error; !flag->ParseFrom(value, &error)) {
    LOG(WARNING) << error;
    return SetResult::INVALID;
  }

  bool success = !cb || cb(*flag);
  if (success) {
    // Mark this config as set by user for rewrite tracking
    set_by_user_.insert(name);
  }
  return success ? SetResult::OK : SetResult::INVALID;
}

optional<string> ConfigRegistry::Get(string_view config_name) {
  string name = NormalizeConfigName(config_name);

  // Special handling for config_file parameter
  if (name == "config_file") {
    return g_config_file_path;
  }

  {
    util::fb2::LockGuard lk(mu_);
    if (!registry_.contains(name))
      return nullopt;
  }

  absl::CommandLineFlag* flag = absl::FindCommandLineFlag(name);
  CHECK(flag);
  return flag->CurrentValue();
}

void ConfigRegistry::Reset() {
  util::fb2::LockGuard lk(mu_);
  registry_.clear();
}

vector<string> ConfigRegistry::List(string_view glob) const {
  string normalized_glob = NormalizeConfigName(glob);
  GlobMatcher matcher(normalized_glob, false /* case insensitive*/);

  vector<string> res;
  util::fb2::LockGuard lk(mu_);

  for (const auto& [name, _] : registry_) {
    if (matcher.Matches(name))
      res.push_back(name);
  }
  return res;
}

void ConfigRegistry::RegisterInternal(string_view config_name, bool is_mutable, WriteCb cb) {
  string name = NormalizeConfigName(config_name);

  // Special handling for config_file parameter
  if (name == "config_file") {
    util::fb2::LockGuard lk(mu_);
    auto [it, inserted] = registry_.emplace(name, Entry{std::move(cb), is_mutable});
    CHECK(inserted) << "Duplicate config name: " << name;
    return;
  }

  absl::CommandLineFlag* flag = absl::FindCommandLineFlag(name);
  CHECK(flag) << "Unknown config name: " << name;

  util::fb2::LockGuard lk(mu_);
  auto [it, inserted] = registry_.emplace(name, Entry{std::move(cb), is_mutable});
  CHECK(inserted) << "Duplicate config name: " << name;
}

ConfigRegistry config_registry;

// Atomic file write helper function
bool AtomicWriteFile(const std::string& filepath, const std::string& content) {
  // mkstemp requires a template ending with 6 'X' characters
  std::string tmp_template = filepath + ".tmpXXXXXX";
  std::vector<char> tmp_path(tmp_template.begin(), tmp_template.end());
  tmp_path.push_back('\0');  // Ensure null-terminated
  int fd = -1;
  int dir_fd = -1;
  bool success = false;

  // Create temporary file using mkstemp, which modifies tmp_path in place
  fd = mkstemp(tmp_path.data());
  if (fd == -1) {
    LOG(WARNING) << "Could not create tmp config file: " << strerror(errno);
    return false;
  }

  // Write content to temporary file
  size_t offset = 0;
  while (offset < content.length()) {
    ssize_t written_bytes = write(fd, content.c_str() + offset, content.length() - offset);
    if (written_bytes <= 0) {
      if (errno == EINTR)
        continue;  // Retry on interrupt
      LOG(WARNING) << "Failed after writing " << offset
                   << " bytes to tmp config file: " << strerror(errno);
      goto cleanup;
    }
    offset += written_bytes;
  }

  // Sync to disk
  if (fsync(fd) == -1) {
    LOG(WARNING) << "Could not sync tmp config file to disk: " << strerror(errno);
    goto cleanup;
  }

  // Set file permissions (0644)
  if (fchmod(fd, 0644) == -1) {
    LOG(WARNING) << "Could not chmod config file: " << strerror(errno);
    goto cleanup;
  }

  // Close file descriptor before rename
  close(fd);
  fd = -1;

  // Atomic rename: move the temp file to the target path
  if (rename(tmp_path.data(), filepath.c_str()) == -1) {
    LOG(WARNING) << "Could not rename tmp config file: " << strerror(errno);
    goto cleanup;
  }

  // Sync directory to ensure rename is persisted
  dir_fd = open(std::filesystem::path(filepath).parent_path().c_str(), O_RDONLY);
  if (dir_fd != -1) {
    fsync(dir_fd);
    close(dir_fd);
    dir_fd = -1;
  }

  success = true;

cleanup:
  if (fd != -1) {
    close(fd);
  }
  if (dir_fd != -1) {
    close(dir_fd);
  }
  // Remove temp file if operation failed
  if (!success && std::filesystem::exists(tmp_path.data())) {
    unlink(tmp_path.data());
  }
  return success;
}

// Rewrite configuration file with current values
bool ConfigRegistry::Rewrite() const {
  if (g_config_file_path.empty()) {
    return false;
  }

  std::filesystem::path config_path(g_config_file_path);
  if (!std::filesystem::exists(config_path)) {
    return false;
  }

  // Read existing config file
  std::ifstream config_file(config_path);
  if (!config_file.is_open()) {
    return false;
  }

  std::vector<std::string> lines;
  std::string line;
  bool has_rewrite_section = false;
  std::map<std::string, std::string> existing_rewrite_configs;
  std::map<std::string, size_t> original_config_positions;  // Track positions of original configs

  // Parse existing config file and collect existing rewrite configs and original config positions
  size_t line_number = 0;
  while (std::getline(config_file, line)) {
    lines.push_back(line);

    // Check if we're in the rewrite section
    if (line == "# Generated by CONFIG REWRITE") {
      has_rewrite_section = true;
      line_number++;
      continue;
    }

    // Collect existing rewrite configs
    if (has_rewrite_section && !line.empty() && line[0] != '#') {
      if (line.find("--") == 0) {
        size_t equal_pos = line.find('=');
        if (equal_pos != std::string::npos) {
          std::string config_name = line.substr(2, equal_pos - 2);
          std::string config_value = line.substr(equal_pos + 1);
          existing_rewrite_configs[config_name] = config_value;
        }
      }
      line_number++;
      continue;
    }

    // End of rewrite section
    if (has_rewrite_section && line.empty()) {
      has_rewrite_section = false;
    }

    // Track original config positions (outside rewrite section)
    if (!has_rewrite_section && line.find("--") == 0) {
      size_t equal_pos = line.find('=');
      if (equal_pos != std::string::npos) {
        std::string config_name = line.substr(2, equal_pos - 2);
        original_config_positions[config_name] = line_number;
      }
    }

    line_number++;
  }
  config_file.close();

  // Collect configs that were modified via CONFIG SET
  std::set<std::string> current_rewrite_configs;
  {
    util::fb2::LockGuard lk(mu_);
    for (const auto& name : set_by_user_) {
      absl::CommandLineFlag* flag = absl::FindCommandLineFlag(name);
      if (!flag)
        continue;
      current_rewrite_configs.insert(name);
    }
  }

  // Update configs in their original positions if they exist
  for (const auto& name : current_rewrite_configs) {
    auto it = original_config_positions.find(name);
    if (it != original_config_positions.end()) {
      // Config exists in original file, update it in place
      absl::CommandLineFlag* flag = absl::FindCommandLineFlag(name);
      if (flag) {
        std::string current_value = flag->CurrentValue();
        size_t line_pos = it->second;
        if (line_pos < lines.size()) {
          lines[line_pos] = "--" + name + "=" + current_value;
        }
      }
    }
  }

  // Remove existing rewrite section
  while (!lines.empty() && lines.back().empty()) {
    lines.pop_back();
  }

  // Remove existing rewrite section lines
  while (!lines.empty() && lines.back() != "# Generated by CONFIG REWRITE") {
    if (lines.back().find("--") == 0) {
      lines.pop_back();
    } else {
      break;
    }
  }

  if (!lines.empty() && lines.back() == "# Generated by CONFIG REWRITE") {
    lines.pop_back();
  }

  // Add rewrite section for configs that don't exist in original file
  std::map<std::string, std::string> final_rewrite_configs;

  // First, add all existing rewrite configs (preserve them)
  for (const auto& [name, value] : existing_rewrite_configs) {
    // Only add to rewrite section if it doesn't exist in original file
    if (original_config_positions.find(name) == original_config_positions.end()) {
      final_rewrite_configs[name] = value;
    }
  }

  // Then, add new configs that don't exist in original file and were modified via CONFIG SET
  for (const auto& name : current_rewrite_configs) {
    // Only add to rewrite section if it doesn't exist in original file
    if (original_config_positions.find(name) == original_config_positions.end()) {
      absl::CommandLineFlag* flag = absl::FindCommandLineFlag(name);
      if (flag) {
        std::string current_value = flag->CurrentValue();
        final_rewrite_configs[name] =
            current_value;  // This will override existing value if present
      }
    }
  }

  // Add rewrite section for configs not in original file
  if (!final_rewrite_configs.empty()) {
    lines.push_back("");
    lines.push_back("# Generated by CONFIG REWRITE");
    for (const auto& [name, value] : final_rewrite_configs) {
      lines.push_back("--" + name + "=" + value);
    }
  }

  // Build content string
  std::string content;
  for (const auto& line : lines) {
    content += line + "\n";
  }

  // Write back to file using atomic operation
  return AtomicWriteFile(config_path.string(), content);
}

}  // namespace dfly
