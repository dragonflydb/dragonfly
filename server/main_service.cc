// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "server/main_service.h"

#include <boost/fiber/operations.hpp>
#include <filesystem>

#include "base/logging.h"
#include "util/uring/uring_fiber_algo.h"
#include "util/varz.h"

DEFINE_uint32(port, 6380, "Redis port");


namespace dfly {

using namespace std;
using namespace util;
using base::VarzValue;

namespace fibers = ::boost::fibers;
namespace this_fiber = ::boost::this_fiber;

namespace {

DEFINE_VARZ(VarzMapAverage, request_latency_usec);

std::optional<VarzFunction> engine_varz;

}  // namespace

Service::Service(ProactorPool* pp)
    : pp_(*pp) {
  CHECK(pp);
  engine_varz.emplace("engine", [this] { return GetVarzStats(); });
}

Service::~Service() {
  engine_varz.reset();
}

void Service::Init(util::AcceptServer* acceptor) {
  request_latency_usec.Init(&pp_);
}

void Service::Shutdown() {
  request_latency_usec.Shutdown();
}

void Service::RegisterHttp(HttpListenerBase* listener) {
  CHECK_NOTNULL(listener);
}

VarzValue::Map Service::GetVarzStats() {
  VarzValue::Map res;

  res.emplace_back("keys", VarzValue::FromInt(0));

  return res;
}

}  // namespace dfly
