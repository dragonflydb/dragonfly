// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <string>

#include "util/fiber_socket_base.h"

namespace facade {

// Minimal FiberSocketBase subclass that captures writes to a string buffer.
// Used in tests as a replacement for io::StringSink when constructing reply builders.
class StringSocket : public util::FiberSocketBase {
 public:
  StringSocket() : FiberSocketBase(nullptr) {
  }

  const std::string& str() const {
    return str_;
  }

  void Clear() {
    str_.clear();
  }

  // io::Sink
  io::Result<size_t> WriteSome(const iovec* v, uint32_t len) override {
    size_t total = 0;
    for (uint32_t i = 0; i < len; ++i) {
      str_.append(reinterpret_cast<const char*>(v[i].iov_base), v[i].iov_len);
      total += v[i].iov_len;
    }
    return total;
  }

  // io::AsyncSink
  void AsyncWriteSome(const iovec* v, uint32_t len, io::AsyncProgressCb cb) override {
    auto res = WriteSome(v, len);
    cb(res);
  }

  // io::AsyncSource
  void AsyncReadSome(const iovec* v, uint32_t len, io::AsyncProgressCb cb) override {
    cb(nonstd::make_unexpected(std::make_error_code(std::errc::not_supported)));
  }

  error_code Shutdown(int) override {
    return {};
  }

  AcceptResult Accept() override {
    return nonstd::make_unexpected(std::make_error_code(std::errc::not_supported));
  }

  error_code Connect(const endpoint_type&, std::function<void(int)>) override {
    return std::make_error_code(std::errc::not_supported);
  }

  error_code Close() override {
    return {};
  }

  bool IsOpen() const override {
    return true;
  }

  io::Result<size_t> RecvMsg(const msghdr&, int) override {
    return nonstd::make_unexpected(std::make_error_code(std::errc::not_supported));
  }

  io::Result<size_t> Recv(const io::MutableBytes&, int) override {
    return nonstd::make_unexpected(std::make_error_code(std::errc::not_supported));
  }

  void set_timeout(uint32_t) override {
  }

  uint32_t timeout() const override {
    return UINT32_MAX;
  }

  endpoint_type LocalEndpoint() const override {
    return {};
  }

  endpoint_type RemoteEndpoint() const override {
    return {};
  }

  void RegisterOnErrorCb(std::function<void(uint32_t)>) override {
  }

  void CancelOnErrorCb() override {
  }

  void RegisterOnRecv(OnRecvCb) override {
  }

  void ResetOnRecvHook() override {
  }

  bool IsUDS() const override {
    return false;
  }

  native_handle_type native_handle() const override {
    return -1;
  }

  error_code Create(unsigned short) override {
    return std::make_error_code(std::errc::not_supported);
  }

  error_code Bind(const struct sockaddr*, unsigned) override {
    return std::make_error_code(std::errc::not_supported);
  }

  error_code Listen(unsigned) override {
    return std::make_error_code(std::errc::not_supported);
  }

  error_code Listen(uint16_t, unsigned) override {
    return std::make_error_code(std::errc::not_supported);
  }

  error_code ListenUDS(const char*, mode_t, unsigned) override {
    return std::make_error_code(std::errc::not_supported);
  }

  io::Result<size_t> TrySend(io::Bytes) override {
    return nonstd::make_unexpected(std::make_error_code(std::errc::not_supported));
  }

  io::Result<size_t> TrySend(const iovec*, uint32_t) override {
    return nonstd::make_unexpected(std::make_error_code(std::errc::not_supported));
  }

  io::Result<size_t> TryRecv(io::MutableBytes) override {
    return nonstd::make_unexpected(std::make_error_code(std::errc::not_supported));
  }

 private:
  std::string str_;
};

}  // namespace facade
