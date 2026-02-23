// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#ifndef WITH_COLLECTION_CMDS

#include "base/logging.h"
#include "server/hset_family.h"
#include "server/set_family.h"
#include "server/stream_family.h"
#include "server/zset_family.h"
namespace dfly {

using namespace std;

namespace {
void Fail() {
  CHECK(false) << "Compiled without command support";
}
}  // namespace

StreamMemTracker::StreamMemTracker() {
}

void StreamMemTracker::UpdateStreamSize(PrimeValue& pv) const {
}

StringMap* HSetFamily::ConvertToStrMap(uint8_t* lp) {
  Fail();
  return nullptr;
}

StringSet* SetFamily::ConvertToStrSet(const intset* is, size_t expected_len) {
  Fail();
  return nullptr;
}

uint32_t SetFamily::MaxIntsetEntries() {
  Fail();
  return 0;
}

LoadBlobResult SetFamily::LoadLPSetBlob(std::string_view blob, PrimeValue* pv) {
  Fail();
  return LoadBlobResult::kCorrupted;
}

LoadBlobResult SetFamily::LoadIntSetBlob(std::string_view blob, PrimeValue* pv) {
  Fail();
  return LoadBlobResult::kCorrupted;
}

LoadBlobResult HSetFamily::LoadZiplistBlob(std::string_view blob, PrimeValue* pv) {
  Fail();
  return LoadBlobResult::kCorrupted;
}

LoadBlobResult HSetFamily::LoadListpackBlob(std::string_view blob, PrimeValue* pv) {
  Fail();
  return LoadBlobResult::kCorrupted;
}

LoadBlobResult ZSetFamily::LoadZiplistBlob(std::string_view blob, PrimeValue* pv) {
  Fail();
  return LoadBlobResult::kCorrupted;
}

LoadBlobResult ZSetFamily::LoadListpackBlob(std::string_view blob, PrimeValue* pv) {
  Fail();
  return LoadBlobResult::kCorrupted;
}

OpResult<ZSetFamily::MScoreResponse> ZSetFamily::ZGetMembers(CmdArgList args, Transaction* tx,
                                                             SinkReplyBuilder* builder) {
  Fail();
  return {};
}

OpResult<ZSetFamily::AddResult> ZSetFamily::OpAdd(const OpArgs& op_args, const ZParams& zparams,
                                                  std::string_view key, ScoredMemberSpan members) {
  Fail();
  return {};
}

OpResult<double> ZSetFamily::OpScore(const OpArgs& op_args, std::string_view key,
                                     std::string_view member) {
  Fail();
  return 0;
}

void ZSetFamily::ZAddGeneric(std::string_view key, const ZParams& zparams, ScoredMemberSpan memb_sp,
                             CommandContext* cmd_cntx) {
  Fail();
}

OpResult<void> ZSetFamily::OpKeyExisted(const OpArgs& op_args, std::string_view key) {
  Fail();
  return {};
}

OpResult<std::vector<ZSetFamily::ScoredArray>> ZSetFamily::OpRanges(
    const std::vector<ZRangeSpec>& range_specs, const OpArgs& op_args, std::string_view key) {
  Fail();
  return {};
}

}  // namespace dfly

#endif
