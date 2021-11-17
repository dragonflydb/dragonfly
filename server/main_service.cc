// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "server/main_service.h"

#include <boost/fiber/operations.hpp>
#include <filesystem>
#include <xxhash.h>

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

inline ShardId Shard(string_view sv, ShardId shard_num) {
  XXH64_hash_t hash = XXH64(sv.data(), sv.size(), 24061983);
  return hash % shard_num;
}

}  // namespace

Service::Service(ProactorPool* pp)
    : shard_set_(pp), pp_(*pp) {
  CHECK(pp);
  engine_varz.emplace("engine", [this] { return GetVarzStats(); });
}

Service::~Service() {
}

void Service::Init(util::AcceptServer* acceptor) {
  uint32_t shard_num = pp_.size() > 1 ? pp_.size() - 1 : pp_.size();
  shard_set_.Init(shard_num);

  pp_.AwaitOnAll([&](uint32_t index, ProactorBase* pb) {
    if (index < shard_count()) {
      shard_set_.InitThreadLocal(index);
    }
  });

  request_latency_usec.Init(&pp_);
}

void Service::Shutdown() {
  engine_varz.reset();
  request_latency_usec.Shutdown();

  shard_set_.RunBriefInParallel([&](EngineShard*) { EngineShard::DestroyThreadLocal(); });
}

void Service::RegisterHttp(HttpListenerBase* listener) {
  CHECK_NOTNULL(listener);
}

void Service::Set(std::string_view key, std::string_view val) {
  ShardId sid = Shard(key, shard_count());
  shard_set_.Await(sid, [&] {
    EngineShard* es = EngineShard::tlocal();
    auto [it, res] = es->db_slice.AddOrFind(0, key);
    it->second = val;
  });
}

VarzValue::Map Service::GetVarzStats() {
  VarzValue::Map res;

  atomic_ulong num_keys{0};
  shard_set_.RunBriefInParallel([&](EngineShard* es) {
    num_keys += es->db_slice.DbSize(0);
  });
  res.emplace_back("keys", VarzValue::FromInt(num_keys.load()));

  return res;
}

}  // namespace dfly
