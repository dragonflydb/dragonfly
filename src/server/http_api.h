// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "util/http/http_handler.h"

namespace dfly {
class Service;
using HttpRequest = util::HttpListenerBase::RequestType;

/**
 * @brief The main handler function for dispatching commands via HTTP.
 *
 * @param args - query arguments. currently not used.
 * @param req  - full http request including the body that should consist of a json array
 *               representing a Dragonfly command. aka `["set", "foo", "bar"]`
 * @param service - a pointer to dfly::Service* object.
 * @param http_cntxt - a pointer to the http context object which provide dragonfly context
 *                     information via user_data() and allows to reply with HTTP responses.
 */
void HttpAPI(const util::http::QueryArgs& args, HttpRequest&& req, Service* service,
             util::HttpContext* http_cntxt);

}  // namespace dfly
