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

struct EntryPayloadVisitor {
  void operator()(const string_view sv) {
    *out += sv;
    *out += ' ';
  }

  void operator()(const CmdArgList list) {
    for (auto arg : list) {
      *out += facade::ToSV(arg);
      *out += ' ';
    }
  }

  void operator()(const ArgSlice slice) {
    for (auto arg : slice) {
      *out += arg;
      *out += ' ';
    }
  }

  template <typename C> void operator()(const pair<string_view, C> p) {
    (*this)(p.first);
    (*this)(p.second);
  }

  void operator()(monostate) {
  }

  string* out;
};

// Extract payload from entry in string form.
std::string ExtractPayload(journal::ParsedEntry& entry) {
  std::string out;
  EntryPayloadVisitor visitor{&out};

  CmdArgList list{entry.cmd.cmd_args.data(), entry.cmd.cmd_args.size()};
  visitor(list);

  if (out.size() > 0 && out.back() == ' ')
    out.pop_back();

  return out;
}

std::string ExtractPayload(journal::Entry& entry) {
  std::string out;
  EntryPayloadVisitor visitor{&out};
  std::visit(visitor, entry.payload);

  if (out.size() > 0 && out.back() == ' ')
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

  std::vector<journal::Entry> test_entries = {
      {0, journal::Op::COMMAND, 0, 2, make_pair("MSET", slice("A", "1", "B", "2"))},
      {0, journal::Op::COMMAND, 0, 2, make_pair("MSET", slice("C", "3"))},
      {1, journal::Op::COMMAND, 0, 2, make_pair("DEL", list("A", "B"))},
      {2, journal::Op::COMMAND, 1, 1, make_pair("LPUSH", list("l", "v1", "v2"))},
      {3, journal::Op::COMMAND, 0, 1, make_pair("MSET", slice("D", "4"))},
      {4, journal::Op::COMMAND, 1, 1, make_pair("DEL", list("l1"))},
      {5, journal::Op::COMMAND, 2, 1, make_pair("DEL", list("E", "2"))},
      {6, journal::Op::MULTI_COMMAND, 2, 1, make_pair("SET", list("E", "2"))},
      {6, journal::Op::EXEC, 2, 1}};

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

}  // namespace dfly

// TODO: extend test.
