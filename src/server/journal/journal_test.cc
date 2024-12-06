#include <string>

#include "base/gtest.h"
#include "base/logging.h"
#include "server/journal/serializer.h"
#include "server/journal/types.h"
#include "server/serializer_commons.h"

using namespace testing;
using namespace std;
using namespace util;

namespace dfly {
namespace journal {
template <typename T> string ConCat(const T& list) {
  string res;
  for (auto arg : list) {
    res += string_view{arg.data(), arg.size()};
    res += ' ';
  }
  return res;
}

template <> string ConCat(const CmdArgList& list) {
  string res;
  for (auto arg : list) {
    res += facade::ToSV(arg);
    res += ' ';
  }
  return res;
}

struct EntryPayloadVisitor {
  void operator()(const Entry::Payload& p) {
    out->append(p.cmd).append(" ");
    *out += visit([this](const auto& args) { return ConCat(args); }, p.args);
  }

  string* out;
};

// Extract payload from entry in string form.
std::string ExtractPayload(ParsedEntry& entry) {
  std::string out = ConCat(entry.cmd.cmd_args);

  if (out.size() > 0)
    out.pop_back();

  return out;
}

std::string ExtractPayload(Entry& entry) {
  std::string out;
  EntryPayloadVisitor visitor{&out};
  visitor(entry.payload);

  if (out.size() > 0)
    out.pop_back();

  return out;
}

// Mock non-owned types with underlying storage.
using StoredSlices = vector<vector<string_view>>;
using StoredLists = vector<pair<vector<string>, CmdArgVec>>;

template <typename... Ss> ArgSlice StoreSlice(StoredSlices* vec, Ss... strings) {
  vec->emplace_back(initializer_list<string_view>{strings...});
  return ArgSlice{vec->back().data(), vec->back().size()};
}

template <typename... Ss> CmdArgList StoreList(StoredLists* vec, Ss... strings) {
  vector<string> stored_strings{strings...};
  CmdArgVec out;
  for (auto& s : stored_strings) {
    out.emplace_back(s.data(), s.size());
  }

  vec->emplace_back(std::move(stored_strings), std::move(out));
  auto& arg_vec = vec->back().second;
  return CmdArgList{arg_vec.data(), arg_vec.size()};
}

// Test serializing and de-serializing entries.
TEST(Journal, WriteRead) {
  StoredSlices slices{};
  StoredLists lists{};

  auto slice = [v = &slices](auto... ss) { return StoreSlice(v, ss...); };
  auto list = [v = &lists](auto... ss) { return StoreList(v, ss...); };
  using Payload = Entry::Payload;

  std::vector<Entry> test_entries = {
      {0, Op::COMMAND, 0, 2, nullopt, Payload("MSET", slice("A", "1", "B", "2"))},
      {0, Op::COMMAND, 0, 2, nullopt, Payload("MSET", slice("C", "3"))},
      {1, Op::COMMAND, 0, 2, nullopt, Payload("DEL", list("A", "B"))},
      {2, Op::COMMAND, 1, 1, nullopt, Payload("LPUSH", list("l", "v1", "v2"))},
      {3, Op::COMMAND, 0, 1, nullopt, Payload("MSET", slice("D", "4"))},
      {4, Op::COMMAND, 1, 1, nullopt, Payload("DEL", list("l1"))},
      {5, Op::COMMAND, 2, 1, nullopt, Payload("DEL", list("E", "2"))}};

  // Write all entries to a buffer.
  base::IoBuf buf;
  io::BufSink sink{&buf};

  JournalWriter writer{&sink};
  for (const auto& entry : test_entries) {
    writer.Write(entry);
  }

  // Read them back.
  io::BufSource source{&buf};
  JournalReader reader{&source, 0};

  for (unsigned i = 0; i < test_entries.size(); i++) {
    auto& expected = test_entries[i];

    auto res = reader.ReadEntry();
    ASSERT_TRUE(res.has_value());

    ASSERT_EQ(expected.opcode, res->opcode);
    ASSERT_EQ(expected.txid, res->txid);
    ASSERT_EQ(expected.dbid, res->dbid);
    ASSERT_EQ(ExtractPayload(expected), ExtractPayload(*res));
  }
}

}  // namespace journal
}  // namespace dfly
