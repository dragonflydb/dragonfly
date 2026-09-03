// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <array>
#include <cstdint>

// Keep IDs stable. Append new entries; never reuse or renumber an existing ID.
// X(id, symbolic_name, Tracy_display_name)
#define DFLY_TRACY_MANUAL_ZONE_LIST(X)                                               \
  X(1, kConnFlushReplies, "Conn.FlushReplies")                                       \
  X(2, kV1Backpressure, "V1.Backpressure")                                           \
  X(3, kV1Dispatch, "V1.Dispatch")                                                   \
  X(4, kParseYield, "ParseYield")                                                    \
  X(5, kV2ParseLoop, "V2.ParseLoop")                                                 \
  X(6, kV2Parse, "V2.Parse")                                                         \
  X(7, kMigrate, "Migrate")                                                          \
  X(8, kV2Control, "V2.Control")                                                     \
  X(9, kV1Recv, "V1.Recv")                                                           \
  X(10, kV1Parse, "V1.Parse")                                                        \
  X(11, kV1SquashPipeline, "V1.Squash.Pipeline")                                     \
  X(12, kV1SquashDispatch, "V1.Squash.Dispatch")                                     \
  X(13, kV1SquashReply, "V1.Squash.Reply")                                           \
  X(14, kV1SquashReplySend, "V1.Squash.Reply.Send")                                  \
  X(15, kV1SquashRelease, "V1.Squash.Release")                                       \
  X(16, kV1SquashReleaseCommand, "V1.Squash.Release.Command")                        \
  X(17, kV1SquashAdvanceAndDispatchStats, "V1.Squash.AdvanceAndDispatchStats")       \
  X(18, kV1SquashFlush, "V1.Squash.Flush")                                           \
  X(19, kV1CondWait, "V1.CondWait")                                                  \
  X(20, kV1BatchYield, "V1.BatchYield")                                              \
  X(21, kV1Squash, "V1.Squash")                                                      \
  X(22, kV1QuotaYield, "V1.QuotaYield")                                              \
  X(23, kV1Admin, "V1.Admin")                                                        \
  X(24, kConnMemoryRefresh, "Conn.Memory.Refresh")                                   \
  X(25, kConnMemoryComputeUsage, "Conn.Memory.ComputeUsage")                         \
  X(26, kConnMemoryApplyUsage, "Conn.Memory.ApplyUsage")                             \
  X(27, kV2SquashPipeline, "V2.Squash.Pipeline")                                     \
  X(28, kV2SquashDispatch, "V2.Squash.Dispatch")                                     \
  X(29, kV2SquashAdvanceAndDispatchStats, "V2.Squash.AdvanceAndDispatchStats")       \
  X(30, kV2ExecuteBatch, "V2.ExecuteBatch")                                          \
  X(31, kV2SendReply, "V2.SendReply")                                                \
  X(32, kV2Dispatch, "V2.Dispatch")                                                  \
  X(33, kV2ReplyBatch, "V2.ReplyBatch")                                              \
  X(34, kV2ReplySend, "V2.Reply.Send")                                               \
  X(35, kV2ReplySendOne, "V2.Reply.SendOne")                                         \
  X(36, kV2ReplyRelease, "V2.Reply.Release")                                         \
  X(37, kConnPipelineEnqueue, "Conn.Pipeline.Enqueue")                               \
  X(38, kConnPipelineEnqueueFinalize, "Conn.Pipeline.Enqueue.Finalize")              \
  X(39, kConnPipelineReleasePipelined, "Conn.Pipeline.ReleasePipelined")             \
  X(40, kConnPipelineReleaseParsed, "Conn.Pipeline.ReleaseParsed")                   \
  X(41, kV2ProactorParse, "V2.ProactorParse")                                        \
  X(42, kV2RunParsePath, "V2.RunParsePath")                                          \
  X(43, kV2ParsedQueueLength, "v2.parsed_q_len")                                     \
  X(44, kV2Backpressure, "V2.Backpressure")                                          \
  X(45, kV2ReadInput, "V2.ReadInput")                                                \
  X(46, kV2Flush, "V2.Flush")                                                        \
  X(47, kV2IdleWait, "V2.IdleWait")                                                  \
  X(48, kReplyBuilderFlushAggregator, "ReplyBuilder.Flush.Aggregator")               \
  X(49, kReplyBuilderFlushBufferSpace, "ReplyBuilder.Flush.BufferSpace")             \
  X(50, kReplyBuilderFlushIovLimit, "ReplyBuilder.Flush.IovLimit")                   \
  X(51, kReplyBuilderFlushDecodeReserve, "ReplyBuilder.Flush.DecodeReserve")         \
  X(52, kReplyBuilderFlushDecodeBufferSpace, "ReplyBuilder.Flush.DecodeBufferSpace") \
  X(53, kReplyBuilderFlush, "ReplyBuilder.Flush")                                    \
  X(54, kReplyBuilderSend, "ReplyBuilder.Send")                                      \
  X(55, kReplyBuilderFinishScope, "ReplyBuilder.FinishScope")                        \
  X(56, kReplyBuilderFlushScopeUnbatched, "ReplyBuilder.Flush.ScopeUnbatched")       \
  X(57, kReplyBuilderFlushScopeLargeRefs, "ReplyBuilder.Flush.ScopeLargeRefs")       \
  X(58, kReplyBuilderFlushScopeCopyNoSpace, "ReplyBuilder.Flush.ScopeCopyNoSpace")   \
  X(59, kReplyBuilderFinishScopeCopyRefs, "ReplyBuilder.FinishScope.CopyRefs")       \
  X(60, kDispatchCommand, "Dispatch.Command")                                        \
  X(61, kDispatchResolve, "Dispatch.Resolve")                                        \
  X(62, kDispatchUnknownCommand, "Dispatch.UnknownCommand")                          \
  X(63, kDispatchBlockingFlush, "Dispatch.BlockingFlush")                            \
  X(64, kDispatchPauseCheck, "Dispatch.PauseCheck")                                  \
  X(65, kDispatchVerify, "Dispatch.Verify")                                          \
  X(66, kDispatchVerifyFailure, "Dispatch.VerifyFailure")                            \
  X(67, kDispatchMultiQueue, "Dispatch.MultiQueue")                                  \
  X(68, kDispatchTransactionAndInvoke, "Dispatch.TransactionAndInvoke")              \
  X(69, kDispatchInvoke, "Dispatch.Invoke")                                          \
  X(70, kDispatchTransactionComplete, "Dispatch.TransactionComplete")                \
  X(71, kDispatchErrorClose, "Dispatch.ErrorClose")                                  \
  X(72, kInvokeCmdHandler, "InvokeCmd.Handler")                                      \
  X(73, kSquashDispatchBatch, "Squash.DispatchBatch")                                \
  X(74, kSquashDispatchTransactionSetup, "Squash.Dispatch.TransactionSetup")         \
  X(75, kSquashDispatchExecute, "Squash.Dispatch.Execute")                           \
  X(76, kSquashDispatchTransactionTeardown, "Squash.Dispatch.TransactionTeardown")   \
  X(77, kSquashDispatchCommand, "Squash.Dispatch.Command")                           \
  X(78, kSquashDispatchResolve, "Squash.Dispatch.Resolve")                           \
  X(79, kSquashDispatchPreDispatch, "Squash.Dispatch.PreDispatch")                   \
  X(80, kSquashDispatchVerify, "Squash.Dispatch.Verify")                             \
  X(81, kSquashDispatchThrottleSleep, "Squash.Dispatch.ThrottleSleep")               \
  X(82, kSquashDispatchUnlock, "Squash.Dispatch.Unlock")                             \
  X(83, kSquasherPrepareShard, "Squasher.PrepareShard")                              \
  X(84, kSquasherClassify, "Squasher.Classify")                                      \
  X(85, kSquasherStandalone, "Squasher.Standalone")                                  \
  X(86, kSquasherStandaloneTransaction, "Squasher.Standalone.Transaction")           \
  X(87, kSquasherStandaloneInvoke, "Squasher.Standalone.Invoke")                     \
  X(88, kSquasherStandaloneResolveReply, "Squasher.Standalone.ResolveReply")         \
  X(89, kSquasherHopWork, "Squasher.Hop.Work")                                       \
  X(90, kSquasherHopCommand, "Squasher.Hop.Command")                                 \
  X(91, kSquasherHopCommandTransaction, "Squasher.Hop.Command.Transaction")          \
  X(92, kSquasherHopCommandInvoke, "Squasher.Hop.Command.Invoke")                    \
  X(93, kSquasherHopCommandCaptureReply, "Squasher.Hop.Command.CaptureReply")        \
  X(94, kSquasherHopCommandAsyncReplyWait, "Squasher.Hop.Command.AsyncReplyWait")    \
  X(95, kSquasherExecute, "Squasher.Execute")                                        \
  X(96, kSquasherExecuteAtomicHops, "Squasher.Execute.AtomicHops")                   \
  X(97, kSquasherHopCallback, "Squasher.Hop.Callback")                               \
  X(98, kSquasherHopBusyYield, "Squasher.Hop.BusyYield")                             \
  X(99, kSquasherExecuteScheduleHops, "Squasher.Execute.ScheduleHops")               \
  X(100, kSquasherExecuteWaitForHops, "Squasher.Execute.WaitForHops")                \
  X(101, kSquasherExecuteMergeReplies, "Squasher.Execute.MergeReplies")              \
  X(102, kSquasherExecuteCleanup, "Squasher.Execute.Cleanup")                        \
  X(103, kSquasherRun, "Squasher.Run")                                               \
  X(104, kStringGetReply, "String.Get.Reply")                                        \
  X(105, kStringGetLookup, "String.Get.Lookup")                                      \
  X(106, kStringGetValue, "String.Get.Value")

#define DFLY_TRACY_MANUAL_ZONE_COUNT 106

namespace facade {

enum class TracyManualZone : uint8_t {
#define DFLY_TRACY_MANUAL_ZONE_ENUM(id, symbol, name) symbol = id,
  DFLY_TRACY_MANUAL_ZONE_LIST(DFLY_TRACY_MANUAL_ZONE_ENUM)
#undef DFLY_TRACY_MANUAL_ZONE_ENUM
};

inline constexpr std::array<const char*, DFLY_TRACY_MANUAL_ZONE_COUNT + 1> kTracyManualZoneNames = {
    "",
#define DFLY_TRACY_MANUAL_ZONE_NAME(id, symbol, name) name,
    DFLY_TRACY_MANUAL_ZONE_LIST(DFLY_TRACY_MANUAL_ZONE_NAME)
#undef DFLY_TRACY_MANUAL_ZONE_NAME
};

constexpr const char* TracyManualZoneName(TracyManualZone zone) {
  return kTracyManualZoneNames[static_cast<uint8_t>(zone)];
}

}  // namespace facade
