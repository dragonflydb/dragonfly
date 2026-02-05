# Visual Strategy Guide

## Current State (The Problem)

```
┌─────────────────────────────────────────────────────────────────┐
│                    facade/facade_types.h                        │
│                     (included by 67 files)                      │
├─────────────────────────────────────────────────────────────────┤
│  #include "common/arg_range.h"          ← 78 file impact       │
│    └── #include "absl/types/span.h"     ← 94 file impact  🔴   │
│    └── #include "base/iterator.h"       ← 79 file impact  🔴   │
│                                                                 │
│  #include "common/backed_args.h"        ← 69 file impact       │
│    └── #include "absl/container/inlined_vector.h"  🔴          │
│                                         ← 88 file impact       │
│                                                                 │
│  #include "facade/op_status.h"          ← 70 file impact       │
│                                                                 │
│  #include "strings/human_readable.h"    ← 68 file impact  🔴   │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│  Exports:                                                       │
│    • CmdArgList, CmdArgVec (simple typedefs)                   │
│    • ParsedArgs (complex class)                                │
│    • ErrorReply (complex struct)                               │
│    • MemoryBytesFlag (typedef from strings/human_readable.h)   │
└─────────────────────────────────────────────────────────────────┘
                              ↓
                    (67 files include this)
                              ↓
        ┌─────────────────────┴─────────────────────┐
        ↓                                           ↓
┌────────────────┐                         ┌────────────────┐
│ server/common.h│                         │  Other files   │
│  (46 files)    │                         │   (~21 files)  │
└────────────────┘                         └────────────────┘
   Only needs:                                Only needs:
   • CmdArgList ✅                            • CmdArgList ✅
   • CmdArgVec ✅                             • Protocol ✅
   • OpStatus ✅                              • ParsedArgs ❌
   
   But gets:                                 But gets:
   • 4 heavy headers 🔴                      • 4 heavy headers 🔴
```

## After Phase 1: Extract MemoryBytesFlag

```
┌──────────────────────────────────────────────────────────────────┐
│                    facade/facade_types.h                         │
│                     (included by 67 files)                       │
├──────────────────────────────────────────────────────────────────┤
│  #include "common/arg_range.h"          ← 78 file impact        │
│    └── #include "absl/types/span.h"     ← 94 file impact   🔴   │
│    └── #include "base/iterator.h"       ← 79 file impact   🔴   │
│                                                                  │
│  #include "common/backed_args.h"        ← 69 file impact        │
│    └── #include "absl/container/inlined_vector.h"   🔴          │
│                                                                  │
│  #include "facade/op_status.h"          ← 70 file impact        │
│                                                                  │
│  ❌ REMOVED: strings/human_readable.h                           │
│                                                                  │
├──────────────────────────────────────────────────────────────────┤
│  Exports:                                                        │
│    • CmdArgList, CmdArgVec (simple typedefs)                    │
│    • ParsedArgs (complex class)                                 │
│    • ErrorReply (complex struct)                                │
│    ❌ REMOVED: MemoryBytesFlag                                  │
└──────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│            ✨ NEW: facade/memory_bytes_flag.h                    │
│                    (included by 11 files only)                   │
├──────────────────────────────────────────────────────────────────┤
│  #include "strings/human_readable.h"                             │
│  using MemoryBytesFlag = strings::MemoryBytesFlag;              │
└──────────────────────────────────────────────────────────────────┘
        ↓ (only 11 files include this)
   ┌────────────────┐
   │ Files that     │
   │ actually use   │
   │ MemoryBytesFlag│
   └────────────────┘

Impact: ~56 files no longer include strings/human_readable.h ✅
```

## After Phase 2: Complete Split

```
┌──────────────────────────────────────────────────────────────────┐
│         ✨ NEW: common/arg_slice.h (lightweight!)                │
│                  Include this for ArgSlice only                  │
├──────────────────────────────────────────────────────────────────┤
│  #include <absl/types/span.h>                                    │
│  #include <string_view>                                          │
│                                                                  │
│  using ArgSlice = absl::Span<const std::string_view>;           │
│  using OwnedArgSlice = absl::Span<const std::string>;           │
└──────────────────────────────────────────────────────────────────┘
                              ↑
                    (used by facade_types_fwd.h)

┌──────────────────────────────────────────────────────────────────┐
│         ✨ NEW: facade/facade_types_fwd.h (lightweight!)         │
│              Include this for type aliases only                  │
├──────────────────────────────────────────────────────────────────┤
│  #include <string_view>                                          │
│  #include <vector>                                               │
│  #include "common/arg_slice.h"  ← Only needs ArgSlice typedef   │
│                                                                  │
│  Exports:                                                        │
│    • CmdArgList, CmdArgVec (simple typedefs)                    │
│    • Protocol, CollectionType (enums)                           │
│    • Forward declarations: ParsedArgs, ErrorReply               │
│                                                                  │
│  Does NOT include:                                               │
│    ❌ backed_args.h (no inlined_vector.h)                       │
│    ❌ arg_range.h (no base/iterator.h)                          │
│    ❌ op_status.h (forward declare only)                        │
└──────────────────────────────────────────────────────────────────┘
                              ↓
                  (40-50 files use this)
                              ↓
        ┌─────────────────────┴─────────────────────┐
        ↓                                           ↓
┌────────────────┐                         ┌────────────────┐
│ server/common.h│                         │  Other files   │
│  (46 files)    │                         │   (~10 files)  │
└────────────────┘                         └────────────────┘
   Includes:                                Includes:
   • facade_types_fwd.h ✅                  • facade_types_fwd.h ✅
   • op_status.h ✅                         
   
   Gets:                                    Gets:
   • Just what it needs ✅                  • Just what it needs ✅
   • No heavy headers! 🎉                   • No heavy headers! 🎉


┌──────────────────────────────────────────────────────────────────┐
│              facade/facade_types.h (full version)                │
│         Include this ONLY if you need ParsedArgs/ErrorReply      │
├──────────────────────────────────────────────────────────────────┤
│  #include "facade/facade_types_fwd.h"   ← Gets lightweight types│
│  #include "common/backed_args.h"        ← For ParsedArgs impl   │
│  #include "facade/op_status.h"          ← For ErrorReply        │
│                                                                  │
│  Exports:                                                        │
│    • Everything from facade_types_fwd.h                         │
│    • ParsedArgs (full definition)                               │
│    • ErrorReply (full definition)                               │
└──────────────────────────────────────────────────────────────────┘
                              ↓
                   (20-30 files use this)
                              ↓
        ┌─────────────────────┴─────────────────────┐
        ↓                                           ↓
┌────────────────┐                         ┌────────────────┐
│ Files using    │                         │ Files using    │
│ ParsedArgs     │                         │ ErrorReply     │
└────────────────┘                         └────────────────┘

Impact: ~40-50 files avoid heavy arg_range.h/backed_args.h chain ✅
```

## Dependency Tree Comparison

### Before (Deep Tree)

```
File.cc
 └── server/common.h
      └── facade/facade_types.h
           ├── common/arg_range.h
           │    ├── absl/types/span.h
           │    │    └── [absl internals] 🔴
           │    └── base/iterator.h
           │         └── [complex templates] 🔴
           ├── common/backed_args.h
           │    └── absl/container/inlined_vector.h
           │         └── [absl internals] 🔴
           ├── facade/op_status.h
           │    └── [simple, OK] ✅
           └── strings/human_readable.h
                └── [string formatting] 🔴

Total depth: 5-6 levels
Heavy headers: 4
Compile time: HIGH 🔴
```

### After (Shallow Tree)

```
File.cc
 └── server/common.h
      └── facade/facade_types_fwd.h
           └── common/arg_slice.h
                └── absl/types/span.h
                     └── [absl internals] ⚠️ (still needed)

Total depth: 3-4 levels
Heavy headers: 1 (span.h only, can't avoid)
Compile time: MEDIUM ✅

Savings: Removed 3 heavy headers!
 ❌ base/iterator.h (complex templates)
 ❌ absl/inlined_vector.h (container)
 ❌ strings/human_readable.h (formatting)
```

## File Usage Patterns

### Pattern 1: Lightweight Users (40-50 files)

```cpp
// Before
#include "facade/facade_types.h"  // Heavy! 🔴

void MyFunction(facade::CmdArgList args) {
  facade::MutableSlice s = args[0];
  // Only uses simple types
}

// After  
#include "facade/facade_types_fwd.h"  // Light! ✅

void MyFunction(facade::CmdArgList args) {
  facade::MutableSlice s = args[0];
  // Still works, but faster compile!
}
```

### Pattern 2: Heavy Users (20-30 files)

```cpp
// Before
#include "facade/facade_types.h"  // Needed for ParsedArgs

void MyFunction(facade::ParsedArgs args) {
  facade::ErrorReply err("error");
  // Uses complex types
}

// After - NO CHANGE NEEDED
#include "facade/facade_types.h"  // Still include full header

void MyFunction(facade::ParsedArgs args) {
  facade::ErrorReply err("error");
  // Same code, still works
}
```

### Pattern 3: Flag Users (11 files)

```cpp
// Before
#include "facade/facade_types.h"  // Heavy! 🔴

ABSL_FLAG(facade::MemoryBytesFlag, maxmemory, ...);

// After
#include "facade/memory_bytes_flag.h"  // Light! ✅

ABSL_FLAG(facade::MemoryBytesFlag, maxmemory, ...);
```

## Compilation Time Flow

### Before: Cascading Recompilation

```
Change arg_range.h
    ↓
Recompile facade_types.h
    ↓
Recompile server/common.h
    ↓
Recompile 46 files including common.h
    ↓
Recompile everything depending on those 46 files
    ↓
Total: 100+ files recompiled 🔴
```

### After: Isolated Recompilation

```
Change arg_range.h
    ↓
Recompile facade_types.h (full version)
    ↓
Recompile 20-30 files using ParsedArgs
    ↓
server/common.h NOT affected (uses facade_types_fwd.h)
    ↓
Total: 20-30 files recompiled ✅

Savings: 70-80 files avoid recompilation! 🎉
```

## Implementation Checklist

```
Phase 1: MemoryBytesFlag (30-45 min)
  ├─ ✅ Create facade/memory_bytes_flag.h
  ├─ ✅ Update 11 files to include new header
  │   ├─ server/main_service.cc
  │   ├─ server/config_registry.cc
  │   ├─ server/engine_shard_set.cc
  │   ├─ server/dfly_main.cc
  │   └─ facade/dragonfly_connection.cc
  ├─ ✅ Remove from facade/facade_types.h
  └─ ✅ Test: make clean && make -j$(nproc)

Phase 2A: Split arg_range (30 min)
  ├─ ✅ Create common/arg_slice.h
  ├─ ✅ Update common/arg_range.h to include it
  └─ ✅ Test: make clean && make -j$(nproc)

Phase 2B: Create forward header (1 hour)
  ├─ ✅ Create facade/facade_types_fwd.h
  ├─ ✅ Update facade/facade_types.h to include it
  └─ ✅ Test: make clean && make -j$(nproc)

Phase 2C: Update clients (1-2 hours)
  ├─ ✅ Update server/common.h (HIGHEST IMPACT)
  ├─ ✅ Update server/family_utils.h
  ├─ ✅ Update server/generic_family.h
  ├─ ✅ Update 10-20 other candidate files
  └─ ✅ Test after each: make -j$(nproc)

Final Validation
  ├─ ✅ Full test suite: ctest -j$(nproc)
  ├─ ✅ IWYU analysis
  ├─ ✅ Measure compilation time
  └─ ✅ Document results
```

## Success Metrics

```
Metric                  | Before | After  | Improvement
─────────────────────────────────────────────────────────
Clean build time        | 100s   | 80-85s | 15-20%
Files including         |        |        |
  arg_range.h          | 78     | ~30    | 60%
  backed_args.h        | 69     | ~25    | 65%
  human_readable.h     | 68     | ~12    | 82%
  facade_types.h       | 67     | ~25    | 63%
Files using lightweight |        |        |
  facade_types_fwd.h   | 0      | ~40    | NEW
```

## The Bottom Line

```
┌────────────────────────────────────────────────────┐
│                   BEFORE                           │
├────────────────────────────────────────────────────┤
│  67 files include facade_types.h                  │
│    → All get 4 heavy headers                      │
│    → Only 20-30 need the heavy stuff              │
│    → 40-50 files get unnecessary bloat            │
│                                                    │
│  Result: SLOW compilation 🔴                      │
└────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────┐
│                    AFTER                           │
├────────────────────────────────────────────────────┤
│  40 files include facade_types_fwd.h (light)      │
│    → Get only what they need                      │
│    → Compile FAST ✅                              │
│                                                    │
│  25 files include facade_types.h (heavy)          │
│    → Only those that actually need it             │
│    → Worth the cost ✅                            │
│                                                    │
│  Result: 15-20% FASTER compilation 🎉             │
└────────────────────────────────────────────────────┘
```

---

**Key Insight**: Most files include heavy headers for features they don't use.
By splitting headers, we let files pay only for what they use.

**The Strategy**: Create lightweight alternatives, migrate incrementally, measure impact.

**Expected Outcome**: 15-20% faster builds with minimal code changes.
