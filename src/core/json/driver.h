// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <string>
#include <vector>

namespace dfly {
namespace json {

class Lexer;
class location;  // from jsonpath_grammar.hh

enum class SegmentType {
  IDENTIFIER = 1,  // $.identifier
  INDEX = 2,       // $.array[0]
  WILDCARD = 3,    // $.array[*] or $.*
};

class PathSegment {
 public:
  PathSegment(SegmentType type, std::string identifier = std::string())
      : type_(type), identifier_(std::move(identifier)) {
  }

  SegmentType type() const {
    return type_;
  }

  const std::string& identifier() const {
    return identifier_;
  }

 private:
  SegmentType type_;
  std::string identifier_;
  int index_;
};

class Path {
 public:
  void AddSegment(PathSegment segment) {
    segments_.push_back(std::move(segment));
  }

  size_t size() const {
    return segments_.size();
  }

  const PathSegment& operator[](size_t i) const {
    return segments_[i];
  }

  void Clear() {
    segments_.clear();
  }

 private:
  std::vector<PathSegment> segments_;
};

class Driver {
 public:
  Driver();
  virtual ~Driver();

  Lexer* lexer() {
    return lexer_.get();
  }

  void SetInput(std::string str);
  void ResetScanner();
  virtual void Error(const location& l, const std::string& msg) = 0;

  void AddIdentifier(const std::string& identifier) {
    path_.AddSegment(PathSegment(SegmentType::IDENTIFIER, identifier));
  }

  Path TakePath() {
    return std::move(path_);
  }

 private:
  Path path_;
  std::string cur_str_;
  std::unique_ptr<Lexer> lexer_;
};

}  // namespace json
}  // namespace dfly
