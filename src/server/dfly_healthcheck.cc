// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

// Minimal health-check helper for shell-less containers.
//
// Usage: dragonfly-healthcheck [--port=PORT] [--host=HOST]
//
// Connects to the Dragonfly port, issues a Redis PING, and exits with:
//   0  — server responded +PONG (healthy)
//   1  — any other response or connection failure (unhealthy)
//
// Used as Docker HEALTHCHECK CMD so it must have no shell dependency.

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <memory>
#include <string>

namespace {

constexpr int kDefaultPort = 6379;
constexpr const char* kDefaultHost = "127.0.0.1";
constexpr const char* kPingCmd = "PING\r\n";
constexpr const char* kPongReply = "+PONG";

struct FdGuard {
  int fd;
  explicit FdGuard(int fd) : fd(fd) {
  }
  ~FdGuard() {
    if (fd >= 0)
      close(fd);
  }
  FdGuard(const FdGuard&) = delete;
  FdGuard& operator=(const FdGuard&) = delete;
};

// Parse --flag=value or --flag value from argv. Returns empty string if not found.
std::string ParseFlag(int argc, char** argv, const char* flag) {
  std::string prefix = std::string("--") + flag + "=";
  std::string bare = std::string("--") + flag;
  for (int i = 1; i < argc; ++i) {
    std::string arg(argv[i]);
    if (arg.rfind(prefix, 0) == 0)
      return arg.substr(prefix.size());
    if (arg == bare && i + 1 < argc)
      return argv[i + 1];
  }
  return {};
}

}  // namespace

int main(int argc, char** argv) {
  std::string host_str = ParseFlag(argc, argv, "host");
  std::string port_str = ParseFlag(argc, argv, "port");

  const char* host = host_str.empty() ? kDefaultHost : host_str.c_str();
  int port = kDefaultPort;
  if (!port_str.empty()) {
    if (sscanf(port_str.c_str(), "%d", &port) != 1 || port <= 0 || port > 65535)
      return 1;
  }

  // Resolve host.
  struct addrinfo hints {};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* raw_res = nullptr;
  if (getaddrinfo(host, nullptr, &hints, &raw_res) != 0 || raw_res == nullptr) {
    fprintf(stderr, "dragonfly-healthcheck: cannot resolve host '%s'\n", host);
    return 1;
  }
  std::unique_ptr<addrinfo, decltype(&freeaddrinfo)> res(raw_res, freeaddrinfo);

  FdGuard guard(socket(res->ai_family, SOCK_STREAM, 0));
  if (guard.fd < 0)
    return 1;

  // Set port in the resolved address.
  if (res->ai_family == AF_INET) {
    reinterpret_cast<struct sockaddr_in*>(res->ai_addr)->sin_port = htons(port);
  } else if (res->ai_family == AF_INET6) {
    reinterpret_cast<struct sockaddr_in6*>(res->ai_addr)->sin6_port = htons(port);
  } else {
    return 1;
  }

  if (connect(guard.fd, res->ai_addr, res->ai_addrlen) != 0)
    return 1;
  res.reset();

  // Send Redis inline PING.
  const int cmdlen = strlen(kPingCmd);
  if (write(guard.fd, kPingCmd, cmdlen) != cmdlen)
    return 1;

  // Read response — expect "+PONG\r\n".
  char resp[32] = {};
  ssize_t n = read(guard.fd, resp, sizeof(resp) - 1);
  if (n <= 0)
    return 1;

  return strncmp(resp, kPongReply, strlen(kPongReply)) == 0 ? 0 : 1;
}
