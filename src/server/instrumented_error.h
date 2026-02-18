#pragma once

#include <memory>
#include <source_location>
#include <sstream>
#include <string>
#include <system_error>

namespace dfly {

inline constexpr std::string_view short_file_name(std::source_location loc) {
  std::string_view path = loc.file_name();
  size_t pos = path.find_last_of("/\\");
  if (pos != std::string_view::npos)
    return path.substr(pos + 1);

  return path;
}

struct InstrumentedError {
  InstrumentedError(std::error_code ec, std::source_location loc = std::source_location::current())
      : ec_{ec}, loc_{loc} {
  }
  InstrumentedError(std::string msg, std::source_location loc = std::source_location::current())
      : msg_{msg}, loc_{loc} {
  }

  InstrumentedError(InstrumentedError trace, std::source_location loc)
      : loc_{loc}, trace_{std::make_unique<InstrumentedError>(std::move(trace))} {
  }

  operator bool() {
    return ec_ || !msg_.empty() || trace_;
  }

  InstrumentedError Trace(std::source_location loc = std::source_location::current()) {
    if (!*this)
      return std::move(*this);

    return InstrumentedError{std::move(*this), loc};
  }

  template <typename... Ts> InstrumentedError&& Context(Ts... ts) {
    if (!*this)
      return std::move(*this);

    std::stringstream ss;
    ((ss << ts << ' '), ...);
    msg_ = ss.str();
    return std::move(*this);
  }

  void Print(std::ostream& os, int depth = 0) const {
    os << std::string(depth, ' ');
    os << "[" << short_file_name(loc_) << ":" << loc_.line() << "]";

    if (!msg_.empty())
      os << ' ' << msg_;

    if (ec_)
      os << ' ' << ec_ << " (" << ec_.message() << ")";

    if (trace_) {
      os << " caused by: \n";
      trace_->Print(os, depth + 1);
    }
  }

  const InstrumentedError& Cause() const {
    if (trace_)
      return trace_->Cause();
    return *this;
  }

  friend std::ostream& operator<<(std::ostream& os, const InstrumentedError& err);

  std::error_code ec_;
  std::string msg_;
  std::source_location loc_;
  std::unique_ptr<InstrumentedError> trace_;
};

inline std::ostream& operator<<(std::ostream& os, const InstrumentedError& err) {
  err.Print(os);
  return os;
}
}  // namespace dfly
