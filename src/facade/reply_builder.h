// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include <absl/container/flat_hash_map.h>

#include <optional>
#include <string_view>

#include "facade/op_status.h"
#include "io/io.h"

namespace facade {

class SinkReplyBuilder {
 public:
  SinkReplyBuilder(const SinkReplyBuilder&) = delete;
  void operator=(const SinkReplyBuilder&) = delete;

  SinkReplyBuilder(::io::Sink* sink);

  virtual ~SinkReplyBuilder() {
  }

  // In order to reduce interrupt rate we allow coalescing responses together using
  // Batch mode. It is controlled by Connection state machine because it makes sense only
  // when pipelined requests are arriving.
  void SetBatchMode(bool batch) {
    should_batch_ = batch;
  }

  // Used for QUIT - > should move to conn_context?
  void CloseConnection();

  std::error_code GetError() const {
    return ec_;
  }

  size_t io_write_cnt() const {
    return io_write_cnt_;
  }

  size_t io_write_bytes() const {
    return io_write_bytes_;
  }

  void reset_io_stats() {
    io_write_cnt_ = 0;
    io_write_bytes_ = 0;
    err_count_.clear();
  }

  const absl::flat_hash_map<std::string, uint64_t>& err_count() const {
    return err_count_;
  }

  //! Sends a string as is without any formatting. raw should be encoded according to the protocol.
  void SendRaw(std::string_view str);
  void SendRawVec(absl::Span<const std::string_view> msg_vec);

  // Common for both MC and Redis.
  virtual void SendError(std::string_view str, std::string_view type = std::string_view{}) = 0;

  virtual void SendSimpleString(std::string_view str) = 0;

  void SendOk() {
    SendSimpleString("OK");
  }

  struct ResponseValue {
    std::string_view key;
    std::string value;
    uint64_t mc_ver = 0;  // 0 means we do not output it (i.e has not been requested).
    uint32_t mc_flag = 0;
  };

  using OptResp = std::optional<ResponseValue>;

  virtual void SendMGetResponse(const OptResp* resp, uint32_t count) = 0;
  virtual void SendLong(long val) = 0;

  // Reply for set commands.
  virtual void SendStored() = 0;
  virtual void SendSetSkipped() = 0;

 protected:
  void Send(const iovec* v, uint32_t len);

  std::string batch_;
  ::io::Sink* sink_;
  std::error_code ec_;

  size_t io_write_cnt_ = 0;
  size_t io_write_bytes_ = 0;
  absl::flat_hash_map<std::string, uint64_t> err_count_;

  bool should_batch_ = false;
};

class MCReplyBuilder : public SinkReplyBuilder {
 public:
  MCReplyBuilder(::io::Sink* stream);

  void SendError(std::string_view str, std::string_view type = std::string_view{}) final;

  // void SendGetReply(std::string_view key, uint32_t flags, std::string_view value) final;
  void SendMGetResponse(const OptResp* resp, uint32_t count) final;

  void SendStored() final;
  void SendLong(long val) final;
  void SendSetSkipped() final;

  void SendClientError(std::string_view str);
  void SendNotFound();
  void SendSimpleString(std::string_view str) final;
};

class RedisReplyBuilder : public SinkReplyBuilder {
 public:
  RedisReplyBuilder(::io::Sink* stream);

  void SetResp3(bool is_resp3);

  void SendError(std::string_view str, std::string_view type = std::string_view{}) override;
  void SendMGetResponse(const OptResp* resp, uint32_t count) override;
  void SendSimpleString(std::string_view str) override;
  void SendStored() override;
  void SendLong(long val) override;
  void SendSetSkipped() override;

  void SendError(OpStatus status);

  virtual void SendSimpleStrArr(const std::string_view* arr, uint32_t count);
  // Send *-1
  virtual void SendNullArray();
  // Send *0
  virtual void SendEmptyArray();

  virtual void SendStringArr(absl::Span<const std::string_view> arr);
  virtual void SendStringArr(absl::Span<const std::string> arr);
  virtual void SendStringArraysAsMap(absl::Span<const std::string_view> arr);
  virtual void SendStringArraysAsMap(absl::Span<const std::string> arr);
  virtual void SendStringArrayAsSet(absl::Span<const std::string_view> arr);

  virtual void SendNull();

  virtual void SendScoredArray(const std::vector<std::pair<std::string, double>>& arr,
                               bool with_scores);

  virtual void SendDouble(double val);

  virtual void SendBulkString(std::string_view str);

  virtual void StartArray(unsigned len);
  virtual void StartMap(unsigned num_pairs);
  virtual void StartSet(unsigned num_elements);

  static char* FormatDouble(double val, char* dest, unsigned dest_len);

  // You normally should not call this - maps the status
  // into the string that would be sent
  static std::string_view StatusToMsg(OpStatus status);

 private:
  enum Resp3Type {
    ARRAY,
    SET,
    MAP,
  };

  using StrPtr = std::variant<const std::string_view*, const std::string*>;
  void SendStringCollection(StrPtr str_ptr, uint32_t len, Resp3Type type);

  bool is_resp3_ = false;
  const char* NullString();
};

class ReqSerializer {
 public:
  explicit ReqSerializer(::io::Sink* stream) : sink_(stream) {
  }

  void SendCommand(std::string_view str);

  std::error_code ec() const {
    return ec_;
  }

 private:
  ::io::Sink* sink_;
  std::error_code ec_;
};

}  // namespace facade
