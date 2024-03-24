// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "util/http/http_handler.h"

namespace dfly {
class Service;
using HttpRequest = util::HttpListenerBase::RequestType;

void HttpAPI(const util::http::QueryArgs& args, HttpRequest&& req, Service* service,
             util::HttpContext* send);

}  // namespace dfly
