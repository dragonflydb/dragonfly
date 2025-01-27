// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/reply_builder.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>

#include <random>

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/error.h"
#include "facade/facade_test.h"
#include "facade/redis_parser.h"
#include "facade/reply_capture.h"

using namespace testing;
using namespace std;

namespace facade {

namespace {

const std::string_view kErrorStrPreFix = "-ERR ";
constexpr std::string_view kCRLF = "\r\n";
constexpr char kErrorStartChar = '-';
constexpr char kStringStartChar = '+';
constexpr std::string_view kOKMessage = "+OK\r\n";
constexpr char kArrayStart = '*';
constexpr char kBulkString = '$';
constexpr char kIntStart = ':';
const std::string_view kIntStartString = ":";
const std::string_view kNullBulkString = "$-1\r\n";
const std::string_view kBulkStringStart = "$";
const std::string_view kStringStart = "+";
const std::string_view kErrorStart = "-";
const std::string_view kArrayStartString = "*";
constexpr std::size_t kMinPayloadLen = 3;  // the begin type char and "\r\n" at the end

std::string BuildExpectedErrorString(std::string_view msg) {
  if (msg.at(0) == kErrorStartChar) {
    return absl::StrCat(msg, kCRLF);
  } else {
    return absl::StrCat(kErrorStrPreFix, msg, kCRLF);
  }
}

std::string_view GetErrorType(std::string_view err) {
  return err == kSyntaxErr ? kSyntaxErrType : err;
}

}  // namespace

class RedisReplyBuilderTest : public testing::Test {
 public:
  struct ParsingResults {
    RedisParser::Result result = RedisParser::OK;
    RespExpr::Vec args;
    std::uint32_t consumed = 0;

    bool Verify(std::uint32_t expected) const {
      return consumed == expected && result == RedisParser::OK;
    }

    bool IsError() const {
      return result != RedisParser::OK || (args.size() == 1 && args[0].type == RespExpr::ERROR);
    }

    bool IsOk() const {
      return IsString();
    }

    bool IsNull() const {
      return result == RedisParser::OK && args.size() == 1 && args.at(0).type == RespExpr::NIL;
    }

    bool IsString() const {
      return args.size() == 1 && result == RedisParser::OK && args[0].type == RespExpr::STRING;
    }
  };

  void SetUp() {
    sink_.Clear();
    builder_.reset(new RedisReplyBuilder(&sink_));
    ResetStats();
  }

  static void SetUpTestSuite() {
    tl_facade_stats = new FacadeStats;
  }

 protected:
  std::vector<std::string_view> RawTokenizedMessage() const {
    CHECK(!str().empty());
    return absl::StrSplit(str(), kCRLF);
  }

  std::string_view str() const {
    return sink_.str();
  }

  std::string TakePayload() {
    std::string ret = sink_.str();
    sink_.Clear();
    return ret;
  }

  std::size_t SinkSize() const {
    return str().size();
  }

  unsigned GetError(string_view err) const {
    const auto& map = SinkReplyBuilder::GetThreadLocalStats().err_count;
    auto it = map.find(err);
    return it == map.end() ? 0 : it->second;
  }

  static bool NoErrors() {
    return tl_facade_stats->reply_stats.err_count.empty();
  }

  static const ReplyStats& GetReplyStats() {
    return tl_facade_stats->reply_stats;
  }

  // Breaks the string we have in sink into tokens.
  // In  RESP each token is build up from series of bytes follow by "\r\n"
  // This function don't try to parse the message, only to break the strings based
  // on the delimiter "\r\n". It is up to the test to verify these tokens
  std::vector<std::string_view> TokenizeMessage() const;

  // Call the redis parser with the data in the sink
  ParsingResults Parse();

  io::StringSink sink_;
  std::unique_ptr<RedisReplyBuilder> builder_;
  std::unique_ptr<std::uint8_t[]> parser_buffer_;
};

std::vector<std::string_view> RedisReplyBuilderTest::TokenizeMessage() const {
  std::vector<std::string_view> message_tokens = RawTokenizedMessage();
  CHECK(message_tokens.back().empty());  // we're expecting to last to be empty as it only has \r\n
  message_tokens.pop_back();             // remove this empty entry
  std::string_view data = str();
  switch (data[0]) {
    case kArrayStart:
      // in the case of array. we cannot tell the expected tokens number without doing parsing for
      // sub elements
      break;
    case kBulkString:
      if (data == kNullBulkString) {
        CHECK(message_tokens.size() == 1)
            << "NULL bulk string should only have one token, got " << message_tokens.size();
      } else {
        CHECK(message_tokens.size() == 2)
            << "bulk string should only have two tokens, got " << message_tokens.size();
      }
      break;
    case kErrorStartChar:
    case kStringStartChar:
    case kIntStart:
      // for errors and string and ints we don't really need to split as there must be only one
      // entry for \r\n
      CHECK(message_tokens.size() == 1)
          << "string/error message must have only one token got " << message_tokens.size();
      break;
    default:
      LOG(FATAL) << "invalid start char [" << data[0] << "]";
      break;
  }
  return message_tokens;
}

std::ostream& operator<<(std::ostream& os, const RedisReplyBuilderTest::ParsingResults& res) {
  os << "result{consumed bytes:" << res.consumed << ", status: " << res.result << " result count "
     << res.args.size() << ", first entry result: ";
  if (!res.args.empty()) {
    if (res.args.size() > 1) {
      os << "ARRAY: ";
    }

    for (const auto& e : res.args) {
      os << e << "\n";
    }
  } else {
    os << "NILL";
  }
  return os << "}";
}

RedisReplyBuilderTest::ParsingResults RedisReplyBuilderTest::Parse() {
  ParsingResults result;
  parser_buffer_.reset(new uint8_t[SinkSize()]);
  auto* ptr = parser_buffer_.get();
  memcpy(ptr, str().data(), SinkSize());
  RedisParser parser(UINT32_MAX, false);  // client side
  result.result =
      parser.Parse(RedisParser::Buffer{ptr, SinkSize()}, &result.consumed, &result.args);
  return result;
}

///////////////////////////////////////////////////////////////////////////////

TEST_F(RedisReplyBuilderTest, MessageSend) {
  // Test each message that is "sent" to the sink
  builder_->SinkReplyBuilder::SendOk();
  ASSERT_EQ(TakePayload(), kOKMessage);
  builder_->StartArray(10);

  std::string_view hello_msg = "hello";
  builder_->SendBulkString(hello_msg);
  std::string expected_bulk_string = absl::StrCat(
      "*10\r\n", kBulkStringStart, std::to_string(hello_msg.size()), kCRLF, hello_msg, kCRLF);
  ASSERT_EQ(TakePayload(), expected_bulk_string);
}

TEST_F(RedisReplyBuilderTest, SimpleError) {
  // test with simple error case. This means that we must comply to
  // https://redis.io/docs/reference/protocol-spec/#resp-errors
  std::string_view error = "my error";
  std::string_view empty_type;

  builder_->SendError(error, empty_type);
  // must start with "-" and ends with "\r\n"
  // ASSERT_EQ(sink_.str().at(0), kErrorStartChar);
  ASSERT_TRUE(absl::StartsWith(str(), kErrorStart));
  ASSERT_TRUE(absl::EndsWith(str(), kCRLF));
  ASSERT_EQ(GetError(error), 1);
  ASSERT_EQ(str(), BuildExpectedErrorString(error))
      << " error different from expected - '" << str() << "'";
  auto parsing = Parse();
  ASSERT_TRUE(parsing.Verify(SinkSize()));
  ASSERT_TRUE(parsing.IsError()) << " result: " << parsing;
  EXPECT_THAT(parsing.args, ElementsAre(ErrArg(absl::StrCat("ERR ", error))));

  sink_.Clear();
  builder_->SendError(OpStatus::OK);  // in this case we should not have an error string
  ASSERT_TRUE(absl::StartsWith(str(), kStringStart));
  ASSERT_EQ(str(), kOKMessage);

  ASSERT_TRUE(absl::EndsWith(str(), kCRLF));
  ASSERT_EQ(GetError(error), 1);

  parsing = Parse();
  ASSERT_TRUE(parsing.Verify(SinkSize()));
  ASSERT_TRUE(parsing.IsOk()) << " result: " << parsing;
  EXPECT_THAT(parsing.args, ElementsAre("OK"));
}

TEST_F(RedisReplyBuilderTest, ErrorBuiltInMessage) {
  OpStatus error_codes[] = {
      OpStatus::KEY_NOTFOUND,  OpStatus::OUT_OF_RANGE,  OpStatus::WRONG_TYPE,
      OpStatus::OUT_OF_MEMORY, OpStatus::INVALID_FLOAT, OpStatus::INVALID_INT,
      OpStatus::SYNTAX_ERR,    OpStatus::BUSY_GROUP,    OpStatus::INVALID_NUMERIC_RESULT};
  for (const auto& err : error_codes) {
    const std::string_view error_name = StatusToMsg(err);
    const std::string_view error_type = GetErrorType(error_name);

    sink_.Clear();
    builder_->SendError(err);
    ASSERT_TRUE(absl::StartsWith(str(), kErrorStart)) << " invalid start char for " << err;
    ASSERT_TRUE(absl::EndsWith(str(), kCRLF)) << " failed to find correct termination at " << err;
    ASSERT_EQ(GetError(error_type), 1) << " number of error count is invalid for " << err;
    ASSERT_EQ(str(), BuildExpectedErrorString(error_name))
        << " error different from expected - '" << str() << "'";

    auto parsing_output = Parse();
    ASSERT_TRUE(parsing_output.Verify(SinkSize()))
        << " verify for the result is invalid for " << err;
    ASSERT_TRUE(parsing_output.IsError()) << " expecting error for " << err;
  }
}

TEST_F(RedisReplyBuilderTest, ErrorReplyBuiltInMessage) {
  ErrorReply err{OpStatus::OUT_OF_RANGE};
  builder_->SendError(err);
  ASSERT_TRUE(absl::StartsWith(str(), kErrorStart));
  ASSERT_TRUE(absl::EndsWith(str(), kCRLF));
  ASSERT_EQ(GetError(kIndexOutOfRange), 1);
  ASSERT_EQ(str(), BuildExpectedErrorString(kIndexOutOfRange));

  auto parsing_output = Parse();
  ASSERT_TRUE(parsing_output.Verify(SinkSize()));
  ASSERT_TRUE(parsing_output.IsError());
  sink_.Clear();

  err = ErrorReply{"e1", "e2"};
  builder_->SendError(err);
  ASSERT_TRUE(absl::StartsWith(str(), kErrorStart));
  ASSERT_TRUE(absl::EndsWith(str(), kCRLF));
  ASSERT_EQ(GetError("e2"), 1);
  ASSERT_EQ(str(), BuildExpectedErrorString("e1"));

  parsing_output = Parse();
  ASSERT_TRUE(parsing_output.Verify(SinkSize()));
  ASSERT_TRUE(parsing_output.IsError());
}

TEST_F(RedisReplyBuilderTest, ErrorNoneBuiltInMessage) {
  // All these op codes creating the same error message
  OpStatus none_unique_codes[] = {OpStatus::SKIPPED, OpStatus::KEY_EXISTS, OpStatus::INVALID_VALUE,
                                  OpStatus::TIMED_OUT, OpStatus::STREAM_ID_SMALL};
  uint64_t error_count = 0;
  for (const auto& err : none_unique_codes) {
    const std::string_view error_name = StatusToMsg(err);
    const std::string_view error_type = GetErrorType(error_name);

    sink_.Clear();
    builder_->SendError(err);
    ASSERT_TRUE(absl::StartsWith(str(), kErrorStart)) << " invalid start char for " << err;
    ASSERT_TRUE(absl::EndsWith(str(), kCRLF));
    auto current_error_count = GetError(error_type);
    error_count++;
    ASSERT_EQ(current_error_count, error_count) << " number of error count is invalid for " << err;
    auto parsing_output = Parse();
    ASSERT_TRUE(parsing_output.Verify(SinkSize()))
        << " verify for the result is invalid for " << err;

    ASSERT_TRUE(parsing_output.IsError()) << " expecting error for " << err;
  }
}

TEST_F(RedisReplyBuilderTest, StringMessage) {
  // This would test a message that contain a string in it
  // For string this is simple, any string message should start with + and ends with \r\n
  // there can never be more than single \r\n in it as well as no special chars
  const std::string_view payloads[] = {
      "this is a string message", "$$$$$", "12334", "1v%6&*", "@@@", "----", "!!!"};
  for (auto payload : payloads) {
    const std::size_t expected_len = payload.size() + kCRLF.size() + 1;  // include '+' at the start
    sink_.Clear();
    builder_->SendSimpleString(payload);
    ASSERT_EQ(SinkSize(), expected_len);
    ASSERT_TRUE(absl::StartsWith(str(), kStringStart));
    ASSERT_TRUE(absl::EndsWith(str(), kCRLF));
    // auto message_payload = SimpleStringPayload();
    //  ASSERT_EQ(message_payload, payload);
    ASSERT_TRUE(absl::StartsWith(str(), kStringStart));
    ASSERT_TRUE(absl::EndsWith(str(), kCRLF));
    auto data = str();
    data.remove_suffix(kCRLF.size());
    ASSERT_TRUE(absl::EndsWith(data, payload));
  }
}

TEST_F(RedisReplyBuilderTest, EmptyArray) {
  // This test would build an array and try sending it over the "wire"
  // The array starts with the '*', then the number of elements in the array
  // then "\r\n", then each element inside is encoded accordingly
  // an empty array has this "*0\r\n" form
  const std::string_view empty_array = "*0\r\n";
  const std::string_view null_array = "*-1\r\n";
  builder_->StartArray(0);
  ASSERT_EQ(str(), empty_array);

  sink_.Clear();
  builder_->SendNullArray();
  ASSERT_EQ(null_array, str());

  sink_.Clear();
  builder_->SendEmptyArray();
  ASSERT_EQ(str(), empty_array);
}

TEST_F(RedisReplyBuilderTest, StrArray) {
  std::vector<std::string_view> string_vector{"hello", "world", "111", "@3#$^&*~"};
  builder_->StartArray(string_vector.size());
  std::size_t expected_size = kCRLF.size() + 2;
  for (auto s : string_vector) {
    builder_->SendSimpleString(s);
    expected_size += s.size() + kCRLF.size() + 1;
    ASSERT_TRUE(NoErrors());
  }
  ASSERT_EQ(SinkSize(), expected_size);
  // ASSERT_EQ(kArrayStart, str().at(0));
  ASSERT_TRUE(absl::StartsWith(str(), absl::StrCat(kArrayStartString, 4)));
  auto parsing_output = Parse();
  ASSERT_TRUE(parsing_output.Verify(SinkSize()))
      << " invalid parsing for the array message by the parser: " << parsing_output;

  ASSERT_EQ(string_vector.size(), parsing_output.args.size());
  ASSERT_THAT(parsing_output.args,
              ElementsAre(string_vector[0], string_vector[1], string_vector[2], string_vector[3]));

  std::vector<std::string_view> message_tokens = TokenizeMessage();
  ASSERT_THAT(message_tokens, ElementsAre("*4", absl::StrCat(kStringStart, string_vector[0]),
                                          absl::StrCat(kStringStart, string_vector[1]),
                                          absl::StrCat(kStringStart, string_vector[2]),
                                          absl::StrCat(kStringStart, string_vector[3])));
}

TEST_F(RedisReplyBuilderTest, SendSimpleStrArr) {
  // This would send array of strings, but with different API than TestStrArray test
  const std::string_view kArrayMessage[] = {
      // random values
      "+++", "---", "$$$", "~~~~", "@@@", "^^^", "1234", "foo"};
  const std::size_t kArrayLen = sizeof(kArrayMessage) / sizeof(kArrayMessage[0]);
  builder_->SendSimpleStrArr(kArrayMessage);
  ASSERT_TRUE(NoErrors());
  // Tokenize the message and verify content
  std::vector<std::string_view> message_tokens = TokenizeMessage();
  ASSERT_THAT(message_tokens, ElementsAre(absl::StrCat(kArrayStartString, kArrayLen),
                                          absl::StrCat(kStringStart, kArrayMessage[0]),
                                          absl::StrCat(kStringStart, kArrayMessage[1]),
                                          absl::StrCat(kStringStart, kArrayMessage[2]),
                                          absl::StrCat(kStringStart, kArrayMessage[3]),
                                          absl::StrCat(kStringStart, kArrayMessage[4]),
                                          absl::StrCat(kStringStart, kArrayMessage[5]),
                                          absl::StrCat(kStringStart, kArrayMessage[6]),
                                          absl::StrCat(kStringStart, kArrayMessage[7])));

  auto parsed_message = Parse();
  ASSERT_THAT(parsed_message.args,
              ElementsAre(kArrayMessage[0], kArrayMessage[1], kArrayMessage[2], kArrayMessage[3],
                          kArrayMessage[4], kArrayMessage[5], kArrayMessage[6], kArrayMessage[7]));
}

TEST_F(RedisReplyBuilderTest, SendStringViewArr) {
  // This would send array of strings, but with different API than TestStrArray test
  const std::vector<std::string_view> kArrayMessage{
      // random values
      "(((", "}}}", "&&&&", "####", "___", "+++", "0.1234", "bar"};
  builder_->SendBulkStrArr(kArrayMessage);
  ASSERT_TRUE(NoErrors());
  // verify content
  std::vector<std::string_view> message_tokens = TokenizeMessage();
  // the form of this is *<array size>\r\n$<string1 size>\r\n<string1>..$<stringN
  // size>\r\n<stringN>\r\n
  ASSERT_THAT(
      message_tokens,
      ElementsAre(absl::StrCat(kArrayStartString, kArrayMessage.size()),  // array size
                                                                          // size + string 0..N
                  absl::StrCat(kBulkStringStart, kArrayMessage[0].size()), kArrayMessage[0],
                  absl::StrCat(kBulkStringStart, kArrayMessage[1].size()), kArrayMessage[1],
                  absl::StrCat(kBulkStringStart, kArrayMessage[2].size()), kArrayMessage[2],
                  absl::StrCat(kBulkStringStart, kArrayMessage[3].size()), kArrayMessage[3],
                  absl::StrCat(kBulkStringStart, kArrayMessage[4].size()), kArrayMessage[4],
                  absl::StrCat(kBulkStringStart, kArrayMessage[5].size()), kArrayMessage[5],
                  absl::StrCat(kBulkStringStart, kArrayMessage[6].size()), kArrayMessage[6],
                  absl::StrCat(kBulkStringStart, kArrayMessage[7].size()), kArrayMessage[7]));

  // Check the parsed message
  auto parsed_message = Parse();
  ASSERT_THAT(parsed_message.args,
              ElementsAre(kArrayMessage[0], kArrayMessage[1], kArrayMessage[2], kArrayMessage[3],
                          kArrayMessage[4], kArrayMessage[5], kArrayMessage[6], kArrayMessage[7]));
}

TEST_F(RedisReplyBuilderTest, SendBulkStringArr) {
  // This would send array of strings, but with different API than TestStrArray test
  const std::vector<std::string> kArrayMessage{
      // Test this one with large values
      std::string(1024, '.'), std::string(2048, ','), std::string(4096, ' ')};
  builder_->SendBulkStrArr(kArrayMessage);
  ASSERT_TRUE(NoErrors());
  std::vector<std::string_view> message_tokens = TokenizeMessage();
  // the form of this is *<array size>\r\n$<string1 size>\r\n<string1>..$<stringN
  // size>\r\n<stringN>\r\n
  ASSERT_THAT(
      message_tokens,
      ElementsAre(absl::StrCat(kArrayStartString, kArrayMessage.size()),  // array size
                                                                          // size + string 0..N
                  absl::StrCat(kBulkStringStart, kArrayMessage[0].size()), kArrayMessage[0],
                  absl::StrCat(kBulkStringStart, kArrayMessage[1].size()), kArrayMessage[1],
                  absl::StrCat(kBulkStringStart, kArrayMessage[2].size()), kArrayMessage[2]));
  // Check the parsed message
  auto parsed_message = Parse();
  ASSERT_TRUE(parsed_message.Verify(SinkSize()))
      << "message was not successfully parsed: " << parsed_message;
  ASSERT_THAT(parsed_message.args,
              ElementsAre(kArrayMessage[0], kArrayMessage[1], kArrayMessage[2]));
}

TEST_F(RedisReplyBuilderTest, NullBulkString) {
  // null bulk string == "$-1\r\n" i.e. '$' + -1 + \r + \n
  builder_->SendNull();
  ASSERT_TRUE(NoErrors());
  ASSERT_EQ(str(), kNullBulkString);
  auto parsing_output = Parse();
  ASSERT_TRUE(parsing_output.Verify(SinkSize()));
  ASSERT_TRUE(parsing_output.IsNull());
  ASSERT_THAT(parsing_output.args, ElementsAre(ArgType(RespExpr::NIL)));
}

TEST_F(RedisReplyBuilderTest, EmptyBulkString) {
  // empty bulk string is in the form of "$0\r\n\r\n", i.e. length 0 after $ follow by \r\n*2
  const std::string_view kEmptyBulkString = "$0\r\n\r\n";
  builder_->SendBulkString(std::string_view{});
  ASSERT_TRUE(NoErrors());
  ASSERT_EQ(str(), kEmptyBulkString);
  auto parsing_output = Parse();
  ASSERT_TRUE(parsing_output.Verify(SinkSize()));
  ASSERT_TRUE(parsing_output.IsString());
  ASSERT_THAT(parsing_output.args, ElementsAre(std::string_view{}));
}

TEST_F(RedisReplyBuilderTest, NoAsciiBulkString) {
  // Bulk string may contain none ascii chars
  const char random_bytes[] = {0x12, 0x25, 0x37};
  std::size_t data_size = sizeof(random_bytes) / sizeof(random_bytes[0]);
  std::string_view none_ascii_payload{random_bytes, data_size};
  builder_->SendBulkString(none_ascii_payload);
  ASSERT_TRUE(NoErrors());
  const std::string expected_payload =
      absl::StrCat(kBulkStringStart, data_size, kCRLF, none_ascii_payload, kCRLF);
  ASSERT_EQ(str(), expected_payload);
  std::vector<std::string_view> message_tokens = TokenizeMessage();
  ASSERT_EQ(message_tokens.size(), 2);  // length and payload
  ASSERT_THAT(message_tokens,
              ElementsAre(absl::StrCat(kBulkStringStart, data_size), none_ascii_payload));
  auto parsing_output = Parse();
  ASSERT_TRUE(parsing_output.IsString());
  ASSERT_THAT(parsing_output.args, ElementsAre(none_ascii_payload));
}

TEST_F(RedisReplyBuilderTest, BulkStringWithCRLF) {
  // Verify bulk string that contains the \r\n as payload
  std::string_view crlf_chars{"\r\n"};
  builder_->SendBulkString(crlf_chars);
  ASSERT_TRUE(NoErrors());
  // the expected message in this case is $2\r\n\r\n\r\n
  std::string expected_message =
      absl::StrCat(kBulkStringStart, crlf_chars.size(), kCRLF, crlf_chars, kCRLF);
  ASSERT_EQ(str(), expected_message);
  auto parsing_output = Parse();
  ASSERT_TRUE(parsing_output.IsString());
  ASSERT_THAT(parsing_output.args, ElementsAre(crlf_chars));
}

TEST_F(RedisReplyBuilderTest, BulkStringWithStartBulkString) {
  // check a bulk string that contains $<number> as payload
  std::string message = absl::StrCat(kBulkStringStart, "10");
  std::string expected_message =
      absl::StrCat(kBulkStringStart, message.size(), kCRLF, message, kCRLF);
  builder_->SendBulkString(message);
  ASSERT_TRUE(NoErrors());
  ASSERT_EQ(str(), expected_message);

  auto parsing_output = Parse();
  ASSERT_TRUE(parsing_output.IsString());
  ASSERT_THAT(parsing_output.args, ElementsAre(message));
}

TEST_F(RedisReplyBuilderTest, BulkStringWithStarString) {
  std::string message = absl::StrCat(kStringStart, "a string message");
  std::string expected_message =
      absl::StrCat(kBulkStringStart, message.size(), kCRLF, message, kCRLF);
  builder_->SendBulkString(message);
  ASSERT_EQ(str(), expected_message);
  auto parsing_output = Parse();
  ASSERT_TRUE(parsing_output.IsString());
  ASSERT_THAT(parsing_output.args, ElementsAre(message));
}

TEST_F(RedisReplyBuilderTest, BulkStringWithErrorString) {
  std::string message = absl::StrCat(kErrorStrPreFix, kSyntaxErrType);
  std::string expected_message =
      absl::StrCat(kBulkStringStart, message.size(), kCRLF, message, kCRLF);
  builder_->SendBulkString(message);
  ASSERT_TRUE(NoErrors());
  ASSERT_EQ(str(), expected_message);
  auto parsing_output = Parse();
  ASSERT_TRUE(parsing_output.IsString());
  ASSERT_THAT(parsing_output.args, ElementsAre(message));
}

TEST_F(RedisReplyBuilderTest, Int) {
  // message in the form of ":0\r\n" and ":1000\r\n"
  // this message just starts with ':' and ends with \r\n
  // and the payload must be successfully parsed into int type
  const long kPayloadInt = 12345;
  const std::string expected_output = absl::StrCat(kIntStartString, kPayloadInt, kCRLF);
  builder_->SendLong(kPayloadInt);
  ASSERT_EQ(str(), expected_output);
  long value = 0;
  std::string_view expected_payload = str().substr(1, SinkSize() - kMinPayloadLen);
  ASSERT_TRUE(absl::SimpleAtoi(expected_payload, &value));
  ASSERT_EQ(value, kPayloadInt);
  auto parsing_output = Parse();
  ASSERT_THAT(parsing_output.args, ElementsAre(IntArg(kPayloadInt)));
}

TEST_F(RedisReplyBuilderTest, Double) {
  // There is no direct support for double types in RESP
  // to send this, it is sent as bulk string
  const std::string_view kPayloadStr = "23.456";
  double double_value = 0;
  CHECK(absl::SimpleAtod(kPayloadStr, &double_value));
  const std::string expected_payload =
      absl::StrCat(kBulkStringStart, kPayloadStr.size(), kCRLF, kPayloadStr, kCRLF);
  builder_->SendDouble(double_value);
  ASSERT_TRUE(NoErrors());
  std::vector<std::string_view> message_tokens = TokenizeMessage();
  ASSERT_EQ(str(), expected_payload);
  ASSERT_THAT(message_tokens,
              ElementsAre(absl::StrCat(kBulkStringStart, kPayloadStr.size()), kPayloadStr));
  auto parsing_output = Parse();
  ASSERT_TRUE(parsing_output.IsString());
  ASSERT_THAT(parsing_output.args, ElementsAre(kPayloadStr));
}

TEST_F(RedisReplyBuilderTest, MixedTypeArray) {
  // For arrays, we can send an array that contains more than a single type (string/bulk
  // string/simple string/null..) In this test we are verifying that this is actually working. note
  // that this is not part of class RedisReplyBuilder API
  // The entries are:
  // array start
  // bulk string
  // int
  // int
  // simple string
  // simple string
  // empty bulk string
  // double (bulk string)
  std::string long_string(1024, '-');
  const unsigned int kArraySize = 9;
  const char random_bytes[] = {0x12, 0x15, 0x2F};
  const std::string_view kFirstBulkString{random_bytes, 3};
  const long kFirstLongValue = 54321;
  const long kSecondLongValue = 87654321;
  const std::string_view kLongSimpleString{long_string};
  const std::string_view kPayloadDoubleStr = "9987654321.0123";
  double double_value = 0;
  CHECK(absl::SimpleAtod(kPayloadDoubleStr, &double_value));

  builder_->StartArray(kArraySize);
  builder_->SendBulkString(kFirstBulkString);
  builder_->SendLong(kFirstLongValue);
  builder_->SendLong(kSecondLongValue);
  builder_->SendSimpleString(kLongSimpleString);
  // builder_->SendNull();
  builder_->SendBulkString(std::string_view{});
  builder_->SendDouble(double_value);
  const std::string_view output_msg = str();
  ASSERT_FALSE(output_msg.empty());
  ASSERT_TRUE(NoErrors());
  std::vector<std::string_view> message_tokens = TokenizeMessage();
  ASSERT_THAT(
      message_tokens,
      ElementsAre(absl::StrCat(kArrayStartString, kArraySize),  // the length
                  absl::StrCat(kBulkStringStart, kFirstBulkString.size()), kFirstBulkString,
                  absl::StrCat(kIntStartString, kFirstLongValue),
                  absl::StrCat(kIntStartString, kSecondLongValue),
                  absl::StrCat(kStringStart, kLongSimpleString),  // ArgType(RespExpr::NIL),
                  absl::StrCat(kBulkStringStart, "0"), std::string_view{},
                  absl::StrCat(kBulkStringStart, kPayloadDoubleStr.size()), kPayloadDoubleStr));

  // // Now we need to parse it and make sure that its a valid message by the parser as well
  auto parsed_message = Parse();
  ASSERT_THAT(
      parsed_message.args,
      ElementsAre(ArgType(RespExpr::STRING), ArgType(RespExpr::INT64), ArgType(RespExpr::INT64),
                  ArgType(RespExpr::STRING), ArgType(RespExpr::STRING), ArgType(RespExpr::STRING)));
}

TEST_F(RedisReplyBuilderTest, BatchMode) {
  GTEST_SKIP() << "Some differences";

  // Test that when the batch mode is enabled, we are getting the same correct results
  builder_->SetBatchMode(true);
  // Some random values and sizes
  const std::vector<std::string> kInputArray{
      std::string(10, 'p'),  std::string(48, 'o'),  std::string(67, 'y'),
      std::string(167, 'e'), std::string(478, '*'), std::string(164, 't'),
  };
  builder_->StartArray(kInputArray.size());
  ASSERT_EQ(SinkSize(), 0);
  int count = 0;
  std::size_t total_bytes = 0;
  for (const auto& val : kInputArray) {
    builder_->SendBulkString(val);
    ASSERT_EQ(SinkSize(), 0) << " sink is not empty at iteration number " << count;
    ASSERT_EQ(GetReplyStats().io_write_bytes, 0);
    ASSERT_EQ(GetReplyStats().io_write_cnt, 0);
    total_bytes += val.size();
    ++count;
  }
  // in order to actually see the message, we need to disable the batching, then
  // write something
  builder_->SetBatchMode(false);
  builder_->SendBulkString(std::string_view{});
  ASSERT_EQ(GetReplyStats().io_write_cnt, 1);
  // We expecting to have more than the total bytes we count,
  // since we are not counting the \r\n and the type char as well
  // as length entries
  ASSERT_GT(GetReplyStats().io_write_bytes, total_bytes);
  std::vector<std::string_view> array_members = TokenizeMessage();
  ASSERT_THAT(array_members,
              ElementsAre(absl::StrCat(kArrayStartString, kInputArray.size()),
                          absl::StrCat(kBulkStringStart, kInputArray[0].size()), kInputArray[0],
                          absl::StrCat(kBulkStringStart, kInputArray[1].size()), kInputArray[1],
                          absl::StrCat(kBulkStringStart, kInputArray[2].size()), kInputArray[2],
                          absl::StrCat(kBulkStringStart, kInputArray[3].size()), kInputArray[3],
                          absl::StrCat(kBulkStringStart, kInputArray[4].size()), kInputArray[4],
                          absl::StrCat(kBulkStringStart, kInputArray[5].size()), kInputArray[5],
                          absl::StrCat(kBulkStringStart, "0"), std::string_view{}));
}

TEST_F(RedisReplyBuilderTest, Resp3Double) {
  builder_->SetRespVersion(RespVersion::kResp3);
  builder_->SendDouble(5.5);
  ASSERT_TRUE(NoErrors());
  ASSERT_EQ(str(), ",5.5\r\n");
}

TEST_F(RedisReplyBuilderTest, Resp3NullString) {
  builder_->SetRespVersion(RespVersion::kResp3);
  builder_->SendNull();
  ASSERT_TRUE(NoErrors());
  ASSERT_EQ(TakePayload(), "_\r\n");
}

TEST_F(RedisReplyBuilderTest, SendStringArrayAsMap) {
  const std::vector<std::string> map_array{"k1", "v1", "k2", "v2"};

  builder_->SetRespVersion(RespVersion::kResp2);
  builder_->SendBulkStrArr(map_array, builder_->MAP);
  ASSERT_TRUE(NoErrors());
  ASSERT_EQ(TakePayload(), "*4\r\n$2\r\nk1\r\n$2\r\nv1\r\n$2\r\nk2\r\n$2\r\nv2\r\n")
      << "SendStringArrayAsMap Resp2 Failed.";

  builder_->SetRespVersion(RespVersion::kResp3);
  builder_->SendBulkStrArr(map_array, builder_->MAP);
  ASSERT_TRUE(NoErrors());
  ASSERT_EQ(TakePayload(), "%2\r\n$2\r\nk1\r\n$2\r\nv1\r\n$2\r\nk2\r\n$2\r\nv2\r\n")
      << "SendStringArrayAsMap Resp3 Failed.";
}

TEST_F(RedisReplyBuilderTest, SendStringArrayAsSet) {
  const std::vector<std::string> set_array{"e1", "e2", "e3"};

  builder_->SetRespVersion(RespVersion::kResp2);
  builder_->SendBulkStrArr(set_array, builder_->SET);
  ASSERT_TRUE(NoErrors());
  ASSERT_EQ(TakePayload(), "*3\r\n$2\r\ne1\r\n$2\r\ne2\r\n$2\r\ne3\r\n")
      << "SendStringArrayAsSet Resp2 Failed.";

  builder_->SetRespVersion(RespVersion::kResp3);
  builder_->SendBulkStrArr(set_array, builder_->SET);
  ASSERT_TRUE(NoErrors());
  ASSERT_EQ(TakePayload(), "~3\r\n$2\r\ne1\r\n$2\r\ne2\r\n$2\r\ne3\r\n")
      << "SendStringArrayAsSet Resp3 Failed.";
}

TEST_F(RedisReplyBuilderTest, SendScoredArray) {
  const std::vector<std::pair<std::string, double>> scored_array{
      {"e1", 1.1}, {"e2", 2.2}, {"e3", 3.3}};

  builder_->SetRespVersion(RespVersion::kResp2);
  builder_->SendScoredArray(scored_array, false);
  ASSERT_TRUE(NoErrors());
  ASSERT_EQ(TakePayload(), "*3\r\n$2\r\ne1\r\n$2\r\ne2\r\n$2\r\ne3\r\n")
      << "Resp2 WITHOUT scores failed.";

  builder_->SetRespVersion(RespVersion::kResp3);
  builder_->SendScoredArray(scored_array, false);
  ASSERT_TRUE(NoErrors());
  ASSERT_EQ(TakePayload(), "*3\r\n$2\r\ne1\r\n$2\r\ne2\r\n$2\r\ne3\r\n")
      << "Resp3 WITHOUT scores failed.";

  builder_->SetRespVersion(RespVersion::kResp2);
  builder_->SendScoredArray(scored_array, true);
  ASSERT_TRUE(NoErrors());
  ASSERT_EQ(TakePayload(),
            "*6\r\n$2\r\ne1\r\n$3\r\n1.1\r\n$2\r\ne2\r\n$3\r\n2.2\r\n$2\r\ne3\r\n$3\r\n3.3\r\n")
      << "Resp3 WITHSCORES failed.";

  builder_->SetRespVersion(RespVersion::kResp3);
  builder_->SendScoredArray(scored_array, true);
  ASSERT_TRUE(NoErrors());
  ASSERT_EQ(TakePayload(),
            "*3\r\n*2\r\n$2\r\ne1\r\n,1.1\r\n*2\r\n$2\r\ne2\r\n,2.2\r\n*2\r\n$2\r\ne3\r\n,3.3\r\n")
      << "Resp3 WITHSCORES failed.";
}

TEST_F(RedisReplyBuilderTest, SendLabeledScoredArray) {
  const std::vector<std::pair<std::string, double>> scored_array{
      {"e1", 1.1}, {"e2", 2.2}, {"e3", 3.3}};

  builder_->SetRespVersion(RespVersion::kResp2);
  builder_->SendLabeledScoredArray("foobar", scored_array);
  ASSERT_TRUE(NoErrors());
  ASSERT_EQ(TakePayload(),
            "*2\r\n$6\r\nfoobar\r\n*3\r\n*2\r\n$2\r\ne1\r\n$3\r\n1.1\r\n*2\r\n$2\r\ne2\r\n$3\r\n2."
            "2\r\n*2\r\n$2\r\ne3\r\n$3\r\n3.3\r\n")
      << "Resp3 failed.\n";

  builder_->SetRespVersion(RespVersion::kResp3);
  builder_->SendLabeledScoredArray("foobar", scored_array);
  ASSERT_TRUE(NoErrors());
  ASSERT_EQ(TakePayload(),
            "*2\r\n$6\r\nfoobar\r\n*3\r\n*2\r\n$2\r\ne1\r\n,1.1\r\n*2\r\n$2\r\ne2\r\n,2.2\r\n*"
            "2\r\n$2\r\ne3\r\n,3.3\r\n")
      << "Resp3 failed.";
}

TEST_F(RedisReplyBuilderTest, BasicCapture) {
  GTEST_SKIP() << "Unmark when CaptuingReplyBuilder is updated";

  using namespace std;
  string_view kTestSws[] = {"a1"sv, "a2"sv, "a3"sv, "a4"sv};

  CapturingReplyBuilder crb{};
  using RRB = RedisReplyBuilder;

  auto big_arr_cb = [](RRB* r) {
    r->StartArray(4);
    {
      r->StartArray(2);
      r->SendLong(1);
      r->StartArray(2);
      {
        r->SendLong(2);
        r->SendLong(3);
      }
    }
    r->SendLong(4);
    {
      r->StartArray(2);
      {
        r->StartArray(2);
        r->SendLong(5);
        r->SendLong(6);
      }
      r->SendLong(7);
    }
    r->SendLong(8);
  };

  function<void(RRB*)> funcs[] = {
      [](RRB* r) { r->SendNull(); },
      [](RRB* r) { r->SendLong(1L); },
      [](RRB* r) { r->SendDouble(6.7); },
      [](RRB* r) { r->SendSimpleString("ok"); },
      [](RRB* r) { r->SendEmptyArray(); },
      [](RRB* r) { r->SendNullArray(); },
      [](RRB* r) { r->SendError("e1", "e2"); },
      [kTestSws](RRB* r) { r->SendSimpleStrArr(kTestSws); },
      [kTestSws](RRB* r) { r->SendBulkStrArr(kTestSws); },
      [kTestSws](RRB* r) { r->SendBulkStrArr(kTestSws, RRB::SET); },
      [kTestSws](RRB* r) { r->SendBulkStrArr(kTestSws, RRB::MAP); },
      [kTestSws](RRB* r) {
        r->StartArray(3);
        r->SendLong(1L);
        r->SendDouble(2.5);
        r->SendSimpleStrArr(kTestSws);
      },
      big_arr_cb,
  };

  crb.SetRespVersion(RespVersion::kResp3);
  builder_->SetRespVersion(RespVersion::kResp3);

  // Run generator functions on both a regular redis builder
  // and the capturing builder with its capture applied.
  for (auto& f : funcs) {
    f(builder_.get());
    auto expected = TakePayload();
    // f(&crb);
    // CapturingReplyBuilder::Apply(crb.Take(), builder_.get());
    auto actual = TakePayload();
    EXPECT_EQ(expected, actual);
  }

  builder_->SetRespVersion(RespVersion::kResp2);
}

TEST_F(RedisReplyBuilderTest, FormatDouble) {
  char buf[64];

  auto format = [&](double d) { return RedisReplyBuilder::FormatDouble(d, buf, sizeof(buf)); };

  EXPECT_STREQ("0.1", format(0.1));
  EXPECT_STREQ("0.2", format(0.2));
  EXPECT_STREQ("0.8", format(0.8));
  EXPECT_STREQ("1.1", format(1.1));
  EXPECT_STREQ("inf", format(INFINITY));
  EXPECT_STREQ("-inf", format(-INFINITY));
  EXPECT_STREQ("0", format(-0.0));
  EXPECT_STREQ("1e-7", format(0.0000001));
  EXPECT_STREQ("111111111111111110000", format(111111111111111111111.0));
  EXPECT_STREQ("1.1111111111111111e+21", format(1111111111111111111111.0));
  EXPECT_STREQ("1e-23", format(1e-23));
}

TEST_F(RedisReplyBuilderTest, VerbatimString) {
  // test resp3
  std::string str = "A simple string!";

  builder_->SetRespVersion(RespVersion::kResp3);
  builder_->SendVerbatimString(str, RedisReplyBuilder::VerbatimFormat::TXT);
  ASSERT_TRUE(NoErrors());
  ASSERT_EQ(TakePayload(), "=20\r\ntxt:A simple string!\r\n") << "Resp3 VerbatimString TXT failed.";

  builder_->SetRespVersion(RespVersion::kResp3);
  builder_->SendVerbatimString(str, RedisReplyBuilder::VerbatimFormat::MARKDOWN);
  ASSERT_TRUE(NoErrors());
  ASSERT_EQ(TakePayload(), "=20\r\nmkd:A simple string!\r\n") << "Resp3 VerbatimString TXT failed.";

  builder_->SetRespVersion(RespVersion::kResp2);
  builder_->SendVerbatimString(str);
  ASSERT_TRUE(NoErrors());
  ASSERT_EQ(TakePayload(), "$16\r\nA simple string!\r\n") << "Resp3 VerbatimString TXT failed.";
}

TEST_F(RedisReplyBuilderTest, Issue3449) {
  vector<string> records;
  for (unsigned i = 0; i < 10'000; ++i) {
    records.push_back(absl::StrCat(i));
  }
  builder_->SendBulkStrArr(records);
  ASSERT_TRUE(NoErrors());
  ParsingResults parse_result = Parse();
  ASSERT_FALSE(parse_result.IsError());
  EXPECT_EQ(10000, parse_result.args.size());
}

TEST_F(RedisReplyBuilderTest, Issue4424) {
  vector<string> records;
  for (unsigned i = 0; i < 800; ++i) {
    records.push_back(string(100, 'a'));
  }

  for (unsigned j = 0; j < 2; ++j) {
    builder_->SendBulkStrArr(records);
    ASSERT_TRUE(NoErrors());
    ParsingResults parse_result = Parse();
    ASSERT_FALSE(parse_result.IsError()) << int(parse_result.result);
    ASSERT_TRUE(parse_result.Verify(SinkSize()));
    EXPECT_EQ(800, parse_result.args.size());
    sink_.Clear();
  }
}

static void BM_FormatDouble(benchmark::State& state) {
  vector<double> values;
  char buf[64];

  uniform_real_distribution<double> unif(0, 1e9);
  default_random_engine re;
  for (unsigned i = 0; i < 100; i++) {
    values.push_back(unif(re));
  }

  while (state.KeepRunning()) {
    for (auto d : values) {
      RedisReplyBuilder::FormatDouble(d, buf, sizeof(buf));
    }
  }
}
BENCHMARK(BM_FormatDouble);

}  // namespace facade
