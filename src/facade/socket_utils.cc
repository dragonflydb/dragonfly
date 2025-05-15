// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "socket_utils.h"

#ifdef __linux__
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include "absl/strings/str_cat.h"
#include "io/proc_reader.h"

#endif

namespace {

int get_socket_family(int fd) {
  struct sockaddr_storage ss;
  socklen_t len = sizeof(ss);

  if (getsockname(fd, (struct sockaddr*)&ss, &len) == -1) {
    return -1;  // Indicate an error
  }

  return ss.ss_family;
}

}  // namespace

namespace dfly {

// Returns information about the TCP socket state by its descriptor
std::string GetSocketInfo(int socket_fd) {
  if (socket_fd < 0)
    return "invalid socket";

#ifdef __linux__
  struct stat sock_stat;
  if (fstat(socket_fd, &sock_stat) != 0) {
    return "could not stat socket";
  }

  io::Result<io::TcpInfo> tcp_info;
  int family = get_socket_family(socket_fd);
  if (family == AF_INET) {
    tcp_info = io::ReadTcpInfo(sock_stat.st_ino);
  } else if (family == AF_INET6) {
    tcp_info = io::ReadTcp6Info(sock_stat.st_ino);
  } else {
    return "unsupported socket family";
  }

  if (!tcp_info) {
    return "socket not found in /proc/net/tcp or /proc/net/tcp6";
  }

  std::string state_str = io::TcpStateToString(tcp_info->state);

  if (tcp_info->is_ipv6) {
    char local_ip[INET6_ADDRSTRLEN], remote_ip[INET6_ADDRSTRLEN];
    inet_ntop(AF_INET6, &tcp_info->local_addr6, local_ip, sizeof(local_ip));
    inet_ntop(AF_INET6, &tcp_info->remote_addr6, remote_ip, sizeof(remote_ip));
    return absl::StrCat("State: ", state_str, ", Local: [", local_ip, "]:", tcp_info->local_port,
                        ", Remote: [", remote_ip, "]:", tcp_info->remote_port,
                        ", Inode: ", tcp_info->inode);
  } else {
    char local_ip[INET_ADDRSTRLEN], remote_ip[INET_ADDRSTRLEN];
    struct in_addr addr;
    addr.s_addr = htonl(tcp_info->local_addr);
    inet_ntop(AF_INET, &addr, local_ip, sizeof(local_ip));
    addr.s_addr = htonl(tcp_info->remote_addr);
    inet_ntop(AF_INET, &addr, remote_ip, sizeof(remote_ip));
    return absl::StrCat("State: ", state_str, ", Local: ", local_ip, ":", tcp_info->local_port,
                        ", Remote: ", remote_ip, ":", tcp_info->remote_port,
                        ", Inode: ", tcp_info->inode);
  }
#else
  return "socket info not available on this platform";
#endif
}

}  // namespace dfly
