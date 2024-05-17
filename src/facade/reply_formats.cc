#include "facade/reply_formats.h"

#include <absl/strings/str_join.h>

#include "absl/strings/str_cat.h"
#include "base/logging.h"
#include "facade/reply_capture.h"

namespace facade {

namespace {

using namespace std;

// Escape a string so that it is legal to print it in JSON text.
std::string JsonEscape(string_view input) {
  auto hex_digit = [](unsigned c) -> char {
    DCHECK_LT(c, 0xFu);
    return c < 10 ? c + '0' : c - 10 + 'a';
  };

  string out;
  out.reserve(input.size() + 2);
  out.push_back('\"');

  auto p = input.begin();
  auto e = input.end();

  while (p < e) {
    uint8_t c = *p;
    if (c == '\\' || c == '\"') {
      out.push_back('\\');
      out.push_back(*p++);
    } else if (c <= 0x1f) {
      switch (c) {
        case '\b':
          out.append("\\b");
          p++;
          break;
        case '\f':
          out.append("\\f");
          p++;
          break;
        case '\n':
          out.append("\\n");
          p++;
          break;
        case '\r':
          out.append("\\r");
          p++;
          break;
        case '\t':
          out.append("\\t");
          p++;
          break;
        default:
          // this condition captures non readable chars with value < 32,
          // so size = 1 byte (e.g control chars).
          out.append("\\u00");
          out.push_back(hex_digit((c & 0xf0) >> 4));
          out.push_back(hex_digit(c & 0xf));
          p++;
      }
    } else {
      out.push_back(*p++);
    }
  }

  out.push_back('\"');
  return out;
}

struct CaptureVisitor {
  CaptureVisitor() {
    str = R"({"result":)";
  }

  void operator()(monostate) {
  }

  void operator()(long v) {
    absl::StrAppend(&str, v);
  }

  void operator()(double v) {
    absl::StrAppend(&str, v);
  }

  void operator()(const CapturingReplyBuilder::SimpleString& ss) {
    absl::StrAppend(&str, "\"", ss, "\"");
  }

  void operator()(const CapturingReplyBuilder::BulkString& bs) {
    absl::StrAppend(&str, JsonEscape(bs));
  }

  void operator()(CapturingReplyBuilder::Null) {
    absl::StrAppend(&str, "null");
  }

  void operator()(CapturingReplyBuilder::Error err) {
    str = absl::StrCat(R"({"error": ")", err.first, "\"");
  }

  void operator()(facade::OpStatus status) {
    absl::StrAppend(&str, "\"", facade::StatusToMsg(status), "\"");
  }

  void operator()(const CapturingReplyBuilder::StrArrPayload& sa) {
    absl::StrAppend(&str, "[");
    for (const auto& val : sa.arr) {
      absl::StrAppend(&str, JsonEscape(val), ",");
    }
    if (sa.arr.size())
      str.pop_back();
    absl::StrAppend(&str, "]");
  }

  void operator()(const unique_ptr<CapturingReplyBuilder::CollectionPayload>& cp) {
    if (!cp) {
      absl::StrAppend(&str, "null");
      return;
    }
    if (cp->len == 0 && cp->type == facade::RedisReplyBuilder::ARRAY) {
      absl::StrAppend(&str, "[]");
      return;
    }

    absl::StrAppend(&str, "[");
    for (auto& pl : cp->arr) {
      visit(*this, std::move(pl));
    }
  }

  void operator()(const facade::SinkReplyBuilder::MGetResponse& resp) {
    absl::StrAppend(&str, "[");
    for (const auto& val : resp.resp_arr) {
      if (val) {
        absl::StrAppend(&str, JsonEscape(val->value), ",");
      } else {
        absl::StrAppend(&str, "null,");
      }
    }

    if (resp.resp_arr.size())
      str.pop_back();
    absl::StrAppend(&str, "]");
  }

  void operator()(const CapturingReplyBuilder::ScoredArray& sarr) {
    absl::StrAppend(&str, "[");
    for (const auto& [key, score] : sarr.arr) {
      absl::StrAppend(&str, "{", JsonEscape(key), ":", score, "},");
    }
    if (sarr.arr.size() > 0) {
      str.pop_back();
    }
    absl::StrAppend(&str, "]");
  }

  string Take() {
    absl::StrAppend(&str, "}\r\n");
    return std::move(str);
  }

  string str;
};

}  // namespace

std::string FormatToJson(CapturingReplyBuilder::Payload&& payload) {
  CaptureVisitor visitor{};
  std::visit(visitor, payload);
  return visitor.Take();
}

};  // namespace facade
