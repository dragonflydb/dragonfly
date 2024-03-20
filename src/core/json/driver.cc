// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "src/core/json/driver.h"

#include <absl/strings/str_cat.h>

#include "base/logging.h"
#include "src/core/json/lexer_impl.h"
#include "src/core/overloaded.h"

using namespace std;

namespace dfly::json {

namespace {

class SingleValueImpl : public AggFunction {
  Result GetResultImpl() const final {
    return val_;
  }

 protected:
  void Init(const JsonType& src) {
    if (src.is_double()) {
      val_.emplace<double>(src.as_double());
    } else {
      val_.emplace<int64_t>(src.as<int64_t>());
    }
  }

  void Init(const flexbuffers::Reference src) {
    if (src.IsFloat()) {
      val_.emplace<double>(src.AsDouble());
    } else {
      val_.emplace<int64_t>(src.AsInt64());
    }
  }

  Result val_;
};

class MaxImpl : public SingleValueImpl {
  bool ApplyImpl(const JsonType& src) final {
    if (!src.is_number()) {
      return false;
    }

    visit(Overloaded{
              [&](monostate) { Init(src); },
              [&](double d) { val_ = max(d, src.as_double()); },
              [&](int64_t i) {
                if (src.is_double())
                  val_ = max(double(i), src.as_double());
                else
                  val_ = max(i, src.as<int64_t>());
              },
          },
          val_);

    return true;
  }

  bool ApplyImpl(flexbuffers::Reference src) final {
    if (!src.IsNumeric()) {
      return false;
    }

    visit(Overloaded{
              [&](monostate) { Init(src); },
              [&](double d) { val_ = max(d, src.AsDouble()); },
              [&](int64_t i) {
                if (src.IsFloat())
                  val_ = max(double(i), src.AsDouble());
                else
                  val_ = max(i, src.AsInt64());
              },
          },
          val_);
    return true;
  }
};

class MinImpl : public SingleValueImpl {
 private:
  bool ApplyImpl(const JsonType& src) final {
    if (!src.is_number()) {
      return false;
    }

    visit(Overloaded{
              [&](monostate) { Init(src); },
              [&](double d) { val_ = min(d, src.as_double()); },
              [&](int64_t i) {
                if (src.is_double())
                  val_ = min(double(i), src.as_double());
                else
                  val_ = min(i, src.as<int64_t>());
              },
          },
          val_);

    return true;
  }

  bool ApplyImpl(flexbuffers::Reference src) final {
    if (!src.IsNumeric()) {
      return false;
    }

    visit(Overloaded{
              [&](monostate) { Init(src); },
              [&](double d) { val_ = min(d, src.AsDouble()); },
              [&](int64_t i) {
                if (src.IsFloat())
                  val_ = min(double(i), src.AsDouble());
                else
                  val_ = min(i, src.AsInt64());
              },
          },
          val_);
    return true;
  }
};

class AvgImpl : public AggFunction {
 private:
  bool ApplyImpl(const JsonType& src) final {
    if (!src.is_number()) {
      return false;
    }
    sum_ += src.as_double();
    count_++;

    return true;
  }

  bool ApplyImpl(flexbuffers::Reference src) final {
    if (!src.IsNumeric()) {
      return false;
    }
    sum_ += src.AsDouble();
    count_++;

    return true;
  }

  Result GetResultImpl() const final {
    DCHECK_GT(count_, 0u);  // AggFunction guarantees that
    return Result(double(sum_ / count_));
  }

  double sum_ = 0;
  uint64_t count_ = 0;
};

}  // namespace

Driver::Driver() : lexer_(make_unique<Lexer>()) {
}

Driver::~Driver() {
}

void Driver::SetInput(string str) {
  cur_str_ = std::move(str);
  lexer_->in(cur_str_);
  path_.clear();
}

void Driver::ResetScanner() {
  lexer_ = make_unique<Lexer>();
}

void Driver::AddFunction(string_view fname) {
  if (!path_.empty()) {
    throw Parser::syntax_error(lexer_->location(),
                               "function can be only at the beginning of the path");
  }

  shared_ptr<AggFunction> func;
  if (fname == "max") {
    func = make_shared<MaxImpl>();
  } else if (fname == "min") {
    func = make_shared<MinImpl>();
  } else if (fname == "avg") {
    func = make_shared<AvgImpl>();
  } else {
    throw Parser::syntax_error(lexer_->location(), absl::StrCat("Unknown function: ", fname));
  }
  path_.emplace_back(std::move(func));
}

}  // namespace dfly::json
