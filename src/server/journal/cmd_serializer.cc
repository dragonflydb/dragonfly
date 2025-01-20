// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/cmd_serializer.h"

#include "server/container_utils.h"
#include "server/journal/serializer.h"
#include "server/rdb_save.h"

namespace dfly {

namespace {
using namespace std;

class CommandAggregator {
 public:
  using WriteCmdCallback = std::function<void(absl::Span<const string_view>)>;

  CommandAggregator(string_view key, WriteCmdCallback cb, size_t max_agg_bytes)
      : key_(key), cb_(cb), max_aggragation_bytes_(max_agg_bytes) {
  }

  ~CommandAggregator() {
    CommitPending();
  }

  enum class CommitMode { kAuto, kNoCommit };

  // Returns whether CommitPending() was called
  bool AddArg(string arg, CommitMode commit_mode = CommitMode::kAuto) {
    agg_bytes_ += arg.size();
    members_.push_back(std::move(arg));

    if (commit_mode != CommitMode::kNoCommit && agg_bytes_ >= max_aggragation_bytes_) {
      CommitPending();
      return true;
    }

    return false;
  }

 private:
  void CommitPending() {
    if (members_.empty()) {
      return;
    }

    args_.clear();
    args_.reserve(members_.size() + 1);
    args_.push_back(key_);
    for (string_view member : members_) {
      args_.push_back(member);
    }
    cb_(args_);
    members_.clear();
  }

  string_view key_;
  WriteCmdCallback cb_;
  vector<string> members_;
  absl::InlinedVector<string_view, 5> args_;
  size_t agg_bytes_ = 0;
  size_t max_aggragation_bytes_;
};

}  // namespace

CmdSerializer::CmdSerializer(FlushSerialized cb, size_t max_serialization_buffer_size)
    : cb_(std::move(cb)), max_serialization_buffer_size_(max_serialization_buffer_size) {
}

size_t CmdSerializer::SerializeEntry(string_view key, const PrimeValue& pk, const PrimeValue& pv,
                                     uint64_t expire_ms) {
  // We send RESTORE commands for small objects, or objects we don't support breaking.
  bool use_restore_serialization = true;
  size_t commands = 1;
  if (max_serialization_buffer_size_ > 0 && pv.MallocUsed() > max_serialization_buffer_size_) {
    switch (pv.ObjType()) {
      case OBJ_SET:
        commands = SerializeSet(key, pv);
        use_restore_serialization = false;
        break;
      case OBJ_ZSET:
        commands = SerializeZSet(key, pv);
        use_restore_serialization = false;
        break;
      case OBJ_HASH:
        commands = SerializeHash(key, pv);
        use_restore_serialization = false;
        break;
      case OBJ_LIST:
        commands = SerializeList(key, pv);
        use_restore_serialization = false;
        break;
      case OBJ_STRING:
      case OBJ_STREAM:
      case OBJ_JSON:
      case OBJ_SBF:
      default:
        // These types are unsupported wrt splitting huge values to multiple commands, so we send
        // them as a RESTORE command.
        break;
    }
  }

  if (use_restore_serialization) {
    // RESTORE sets STICK and EXPIRE as part of the command.
    SerializeRestore(key, pk, pv, expire_ms);
  } else {
    SerializeStickIfNeeded(key, pk);
    SerializeExpireIfNeeded(key, expire_ms);
  }
  return commands;
}

void CmdSerializer::SerializeCommand(string_view cmd, absl::Span<const string_view> args) {
  journal::Entry entry(0,                     // txid
                       journal::Op::COMMAND,  // single command
                       0,                     // db index
                       1,                     // shard count
                       0,                     // slot-id, but it is ignored at this level
                       journal::Entry::Payload(cmd, ArgSlice(args)));

  // Serialize into a string
  io::StringSink cmd_sink;
  JournalWriter writer{&cmd_sink};
  writer.Write(entry);

  cb_(std::move(cmd_sink).str());
}

void CmdSerializer::SerializeStickIfNeeded(string_view key, const PrimeValue& pk) {
  if (!pk.IsSticky()) {
    return;
  }

  SerializeCommand("STICK", {key});
}

void CmdSerializer::SerializeExpireIfNeeded(string_view key, uint64_t expire_ms) {
  if (expire_ms == 0) {
    return;
  }

  SerializeCommand("PEXIRE", {key, absl::StrCat(expire_ms)});
}

size_t CmdSerializer::SerializeSet(string_view key, const PrimeValue& pv) {
  CommandAggregator aggregator(
      key, [&](absl::Span<const string_view> args) { SerializeCommand("SADD", args); },
      max_serialization_buffer_size_);

  size_t commands = 0;
  container_utils::IterateSet(pv, [&](container_utils::ContainerEntry ce) {
    commands += aggregator.AddArg(ce.ToString());
    return true;
  });
  return commands;
}

size_t CmdSerializer::SerializeZSet(string_view key, const PrimeValue& pv) {
  CommandAggregator aggregator(
      key, [&](absl::Span<const string_view> args) { SerializeCommand("ZADD", args); },
      max_serialization_buffer_size_);

  size_t commands = 0;
  container_utils::IterateSortedSet(
      pv.GetRobjWrapper(),
      [&](container_utils::ContainerEntry ce, double score) {
        aggregator.AddArg(absl::StrCat(score), CommandAggregator::CommitMode::kNoCommit);
        commands += aggregator.AddArg(ce.ToString());
        return true;
      },
      /*start=*/0, /*end=*/-1, /*reverse=*/false, /*use_score=*/true);
  return commands;
}

size_t CmdSerializer::SerializeHash(string_view key, const PrimeValue& pv) {
  CommandAggregator aggregator(
      key, [&](absl::Span<const string_view> args) { SerializeCommand("HSET", args); },
      max_serialization_buffer_size_);

  size_t commands = 0;
  container_utils::IterateMap(
      pv, [&](container_utils::ContainerEntry k, container_utils::ContainerEntry v) {
        aggregator.AddArg(k.ToString(), CommandAggregator::CommitMode::kNoCommit);
        commands += aggregator.AddArg(v.ToString());
        return true;
      });
  return commands;
}

size_t CmdSerializer::SerializeList(string_view key, const PrimeValue& pv) {
  CommandAggregator aggregator(
      key, [&](absl::Span<const string_view> args) { SerializeCommand("RPUSH", args); },
      max_serialization_buffer_size_);

  size_t commands = 0;
  container_utils::IterateList(pv, [&](container_utils::ContainerEntry ce) {
    commands += aggregator.AddArg(ce.ToString());
    return true;
  });
  return commands;
}

void CmdSerializer::SerializeRestore(string_view key, const PrimeValue& pk, const PrimeValue& pv,
                                     uint64_t expire_ms) {
  absl::InlinedVector<string_view, 5> args;
  args.push_back(key);

  string expire_str = absl::StrCat(expire_ms);
  args.push_back(expire_str);

  io::StringSink value_dump_sink;
  SerializerBase::DumpObject(pv, &value_dump_sink);
  args.push_back(value_dump_sink.str());

  args.push_back("ABSTTL");  // Means expire string is since epoch

  if (pk.IsSticky()) {
    args.push_back("STICK");
  }

  SerializeCommand("RESTORE", args);
}

}  // namespace dfly
