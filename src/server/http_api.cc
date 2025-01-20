// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/http_api.h"

#include "base/logging.h"
#include "core/flatbuffers.h"
#include "facade/conn_context.h"
#include "facade/reply_builder.h"
#include "server/main_service.h"
#include "util/http/http_common.h"

namespace dfly {
using namespace util;
using namespace std;
namespace h2 = boost::beast::http;
using facade::CapturingReplyBuilder;

namespace {

bool IsVectorOfStrings(flexbuffers::Reference req) {
  if (!req.IsVector()) {
    return false;
  }

  auto vec = req.AsVector();
  if (vec.size() == 0) {
    return false;
  }

  for (size_t i = 0; i < vec.size(); ++i) {
    if (!vec[i].IsString()) {
      return false;
    }
  }
  return true;
}

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

  void operator()(unique_ptr<CapturingReplyBuilder::CollectionPayload> cp) {
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

  string str;
};

}  // namespace

void HttpAPI(const http::QueryArgs& args, HttpRequest&& req, Service* service,
             HttpContext* http_cntx) {
  auto& body = req.body();

  flexbuffers::Builder fbb;
  flatbuffers::Parser parser;
  flexbuffers::Reference doc;
  bool success = parser.ParseFlexBuffer(body.c_str(), nullptr, &fbb);
  if (success) {
    fbb.Finish();
    doc = flexbuffers::GetRoot(fbb.GetBuffer());
    if (!IsVectorOfStrings(doc)) {
      success = false;
    }
  }

  // TODO: to add a content-type/json check.
  if (!success) {
    VLOG(1) << "Invalid body " << body;
    auto response = http::MakeStringResponse(h2::status::bad_request);
    http::SetMime(http::kTextMime, &response);
    response.body() = "Failed to parse json\r\n";
    http_cntx->Invoke(std::move(response));
    return;
  }

  vector<string> cmd_args;
  flexbuffers::Vector vec = doc.AsVector();
  for (size_t i = 0; i < vec.size(); ++i) {
    cmd_args.push_back(vec[i].AsString().c_str());
  }
  vector<string_view> cmd_slices(cmd_args.size());
  for (size_t i = 0; i < cmd_args.size(); ++i) {
    cmd_slices[i] = cmd_args[i];
  }

  facade::ConnectionContext* context = (facade::ConnectionContext*)http_cntx->user_data();
  DCHECK(context);

  facade::CapturingReplyBuilder reply_builder;

  // TODO: to finish this.
  service->DispatchCommand(absl::MakeSpan(cmd_slices), &reply_builder, context);
  facade::CapturingReplyBuilder::Payload payload = reply_builder.Take();

  auto response = http::MakeStringResponse();
  http::SetMime(http::kJsonMime, &response);

  CaptureVisitor visitor;
  std::visit(visitor, std::move(payload));
  visitor.str.append("}\r\n");
  response.body() = visitor.str;
  http_cntx->Invoke(std::move(response));
}

}  // namespace dfly
