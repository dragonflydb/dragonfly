// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "socket_utils.h"

#include <arpa/inet.h>
#include <sys/socket.h>

#ifdef __linux__
#include <linux/sockios.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/ioctl.h>
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

std::string GetSocketQueuesInfo(int socket_fd) {
  if (socket_fd < 0)
    return "invalid socket";

#ifdef __linux__
  // outq: bytes not yet acked by the peer, notsent: bytes not yet sent,
  // inq: bytes received but not yet read by the application.
  int outq = -1, notsent = -1, inq = -1;
  ioctl(socket_fd, SIOCOUTQ, &outq);
  ioctl(socket_fd, SIOCOUTQNSD, &notsent);
  ioctl(socket_fd, SIOCINQ, &inq);

  pollfd pfd{.fd = socket_fd, .events = POLLIN | POLLOUT, .revents = 0};
  int poll_res = poll(&pfd, 1, 0);

  std::string res = absl::StrCat("outq: ", outq, ", notsent: ", notsent, ", inq: ", inq,
                                 ", readable: ", poll_res > 0 && (pfd.revents & POLLIN) ? 1 : 0,
                                 ", writable: ", poll_res > 0 && (pfd.revents & POLLOUT) ? 1 : 0,
                                 ", revents: ", poll_res > 0 ? pfd.revents : 0);

  struct tcp_info info;
  socklen_t info_len = sizeof(info);
  if (getsockopt(socket_fd, IPPROTO_TCP, TCP_INFO, &info, &info_len) == 0) {
    absl::StrAppend(&res, ", tcp_state: ", info.tcpi_state, ", unacked: ", info.tcpi_unacked,
                    ", retransmits: ", info.tcpi_retransmits, ", probes: ", info.tcpi_probes,
                    ", backoff: ", info.tcpi_backoff, ", snd_cwnd: ", info.tcpi_snd_cwnd,
                    ", last_data_sent_ms: ", info.tcpi_last_data_sent,
                    ", last_data_recv_ms: ", info.tcpi_last_data_recv,
                    ", last_ack_recv_ms: ", info.tcpi_last_ack_recv);
  }
  return res;
#else
  return "socket queues info not available on this platform";
#endif
}

}  // namespace dfly
