// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/http_api.h"

#include "base/logging.h"
#include "core/flatbuffers.h"
#include "facade/conn_context.h"
#include "facade/reply_builder.h"
#include "facade/reply_formats.h"
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
  vector<facade::MutableSlice> cmd_slices(cmd_args.size());
  for (size_t i = 0; i < cmd_args.size(); ++i) {
    cmd_slices[i] = absl::MakeSpan(cmd_args[i]);
  }

  facade::ConnectionContext* context = (facade::ConnectionContext*)http_cntx->user_data();
  DCHECK(context);

  facade::CapturingReplyBuilder reply_builder;
  auto* prev = context->Inject(&reply_builder);
  // TODO: to finish this.
  service->DispatchCommand(absl::MakeSpan(cmd_slices), context);

  context->Inject(prev);
  auto response = http::MakeStringResponse();
  http::SetMime(http::kJsonMime, &response);

  response.body() = facade::FormatToJson(reply_builder.Take());
  http_cntx->Invoke(std::move(response));
}

}  // namespace dfly
