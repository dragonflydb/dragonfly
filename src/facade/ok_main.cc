// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "base/init.h"
#include "facade/conn_context.h"
#include "facade/dragonfly_listener.h"
#include "facade/service_interface.h"
#include "util/accept_server.h"
#include "util/fibers/pool.h"

ABSL_FLAG(uint32_t, port, 6379, "server port");

using namespace util;
using namespace std;
using absl::GetFlag;

namespace facade {

namespace {

thread_local ConnectionStats tl_stats;

class OkService : public ServiceInterface {
 public:
  void DispatchCommand(CmdArgList args, ConnectionContext* cntx) final {
    (*cntx)->SendOk();
  }

  void DispatchManyCommands(absl::Span<CmdArgList> args_lists, ConnectionContext* cntx) final {
    for (auto args : args_lists)
      DispatchCommand(args, cntx);
  }

  void DispatchMC(const MemcacheParser::Command& cmd, std::string_view value,
                  ConnectionContext* cntx) final {
    cntx->reply_builder()->SendError("");
  }

  ConnectionContext* CreateContext(util::FiberSocketBase* peer, Connection* owner) final {
    return new ConnectionContext{peer, owner};
  }

  ConnectionStats* GetThreadLocalConnectionStats() final {
    return &tl_stats;
  }
};

void RunEngine(ProactorPool* pool, AcceptServer* acceptor) {
  OkService service;

  acceptor->AddListener(GetFlag(FLAGS_port), new Listener{Protocol::REDIS, &service});

  acceptor->Run();
  acceptor->Wait();
}

}  // namespace

}  // namespace facade

int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);

  CHECK_GT(GetFlag(FLAGS_port), 0u);

  unique_ptr<util::ProactorPool> pp(fb2::Pool::IOUring(1024));
  pp->Run();

  AcceptServer acceptor(pp.get());
  facade::RunEngine(pp.get(), &acceptor);

  pp->Stop();

  return 0;
}
