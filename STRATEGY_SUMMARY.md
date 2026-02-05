# Include Reduction Strategy - Executive Summary

## The Problem
IWYU analysis shows that `facade/facade_types.h` creates a heavy dependency chain affecting 67 files:
- Pulls in `absl/types/span.h` (impacts 94 files)
- Pulls in `absl/container/inlined_vector.h` (impacts 88 files)  
- Pulls in `base/iterator.h` (impacts 79 files)
- Pulls in `strings/human_readable.h` (impacts 68 files)

Most files including `facade_types.h` only need simple typedefs, not the complex classes that require these heavy headers.

## The Solution
Split headers to separate lightweight types from heavy dependencies.

### Three-Phase Approach

```
┌─────────────────────────────────────────────────────────┐
│ Phase 1: Extract MemoryBytesFlag (30-45 min)           │
├─────────────────────────────────────────────────────────┤
│ Impact: ~56 files avoid strings/human_readable.h       │
│ Risk:   LOW - isolated change                          │
└─────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────┐
│ Phase 2A: Create arg_slice.h (30 min)                  │
├─────────────────────────────────────────────────────────┤
│ Impact: Enables clean facade_types_fwd.h               │
│ Risk:   LOW - backward compatible                      │
└─────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────┐
│ Phase 2B: Create facade_types_fwd.h (1 hour)           │
├─────────────────────────────────────────────────────────┤
│ Impact: Files can include lightweight types only       │
│ Risk:   LOW - new header, doesn't break existing       │
└─────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────┐
│ Phase 2C: Update client files (1-2 hours)              │
├─────────────────────────────────────────────────────────┤
│ Impact: ~40-50 files avoid heavy headers               │
│ Risk:   LOW - incremental, per-file changes            │
└─────────────────────────────────────────────────────────┘

Expected Total Impact: 15-20% compilation time reduction
```

## Recommended Strategy (REFINED)

Based on edge case analysis, the cleanest approach:

### Phase 1: Extract MemoryBytesFlag

**Create**: `src/facade/memory_bytes_flag.h`
```cpp
#pragma once
#include "strings/human_readable.h"
namespace facade {
using MemoryBytesFlag = strings::MemoryBytesFlag;
}
```

**Remove from**: `src/facade/facade_types.h`
- Remove `#include "strings/human_readable.h"`
- Remove `using MemoryBytesFlag = ...`

**Update**: 11 files that use `MemoryBytesFlag`
- Add `#include "facade/memory_bytes_flag.h"`

**Result**: 56 files no longer include `strings/human_readable.h`

### Phase 2A: Split arg_range.h

**Create**: `src/common/arg_slice.h`
```cpp
#pragma once
#include <absl/types/span.h>
#include <string_view>

namespace cmn {
using ArgSlice = absl::Span<const std::string_view>;
using OwnedArgSlice = absl::Span<const std::string>;
}
```

**Update**: `src/common/arg_range.h`
```cpp
#pragma once
#include "common/arg_slice.h"  // For typedefs
#include "base/iterator.h"     // For ArgRange struct

namespace cmn {
// ArgSlice now in arg_slice.h
struct ArgRange { ... };  // Uses base::it::Wrap
}
```

**Result**: Files needing only `ArgSlice` can include lightweight `arg_slice.h`

### Phase 2B: Create facade_types_fwd.h

**Create**: `src/facade/facade_types_fwd.h`
```cpp
#pragma once
#include <string_view>
#include <vector>
#include "common/arg_slice.h"  // Lightweight!

namespace facade {
// Simple typedefs
using MutableSlice = std::string_view;
using CmdArgVec = std::vector<std::string_view>;
using CmdArgList = cmn::ArgSlice;

// Simple enums
enum class Protocol : uint8_t { MEMCACHE = 1, REDIS = 2 };
enum class CollectionType : uint8_t { ARRAY, SET, MAP, PUSH };

// Forward declarations
class ParsedArgs;
struct ErrorReply;
struct MemcacheCmdFlags;
}
```

**Update**: `src/facade/facade_types.h`
- Include `facade_types_fwd.h` at top
- Remove duplicated typedefs/enums

**Result**: Lightweight header for files needing only type aliases

### Phase 2C: Update Client Files

**Priority targets**:
1. `src/server/common.h` (46 files depend on it) ← HIGHEST IMPACT
2. `src/server/family_utils.h`
3. `src/server/generic_family.h`
4. Other headers using only `CmdArgList`, `CmdArgVec`, `MutableSlice`

**For each file**:
1. Verify doesn't use `ParsedArgs`, `ErrorReply`, `MemcacheCmdFlags`
2. Change: `#include "facade/facade_types.h"` → `#include "facade/facade_types_fwd.h"`
3. Compile and test

**Result**: 40-50 files avoid `backed_args.h` and `arg_range.h` (with its `base/iterator.h`)

## Impact Estimate

### Compilation Time
- **Phase 1**: 3-5% reduction
- **Phase 2**: 10-15% reduction  
- **Total**: 15-20% faster clean builds

### Include Depth
- **Before**: facade_types.h → 4 heavy headers
- **After**: facade_types_fwd.h → 1 header (arg_slice.h → absl/span.h only)
- **Reduction**: 75% fewer transitive includes for lightweight usage

### Files Impacted
- **Phase 1**: ~56 files stop including strings/human_readable.h
- **Phase 2**: ~40-50 files stop including arg_range.h, backed_args.h chains

## Why This Strategy Works

### 1. Minimal Changes
- Creating new headers (non-breaking)
- Updating includes (low risk)
- No refactoring of existing code
- No performance impact

### 2. Incremental Approach
- Each phase independently valuable
- Can stop after Phase 1 if needed
- Each file update independently testable
- Clear rollback path

### 3. Addresses Root Cause
- Separates interface from implementation
- Makes dependencies explicit
- Follows "include what you use" principle
- Industry standard pattern (like `<iosfwd>` vs `<iostream>`)

### 4. Low Risk
- Backward compatible (existing includes still work)
- No ODR violations (no duplicate definitions)
- Headers remain self-contained
- No template instantiation issues

## What We're NOT Doing (And Why)

### ❌ Pimpl Pattern
- **Why not**: Too invasive, performance impact, high risk
- **Complexity**: Would require refactoring 100+ files
- **Performance**: Adds indirection to hot paths

### ❌ Move Templates to .cc Files
- **Why not**: Not possible - templates need full definition at use site
- **Note**: `OpResult<T>`, `ParsedArgs` are templates

### ❌ Remove absl/span.h from arg_slice.h
- **Why not**: `ArgSlice` IS `absl::Span` - can't forward declare
- **Note**: But we CAN avoid `base/iterator.h` which is heavier

### ❌ Full Dependency Refactor
- **Why not**: Not "minimal" - weeks of work, high risk
- **Better**: Our approach gets 80% benefit with 20% effort

## Next Steps

1. **Review** this strategy with team
2. **Measure baseline**: Run compilation time benchmark
3. **Implement Phase 1**: MemoryBytesFlag extraction
4. **Measure Phase 1**: Verify improvement
5. **Implement Phase 2**: If Phase 1 successful
6. **Measure final**: Document total improvement
7. **PR**: Submit with before/after metrics

## Required Artifacts

### Before Starting
- [ ] Baseline compilation time measurement
- [ ] Baseline include count (files including each header)
- [ ] Team approval for strategy

### Phase 1 Complete
- [ ] `facade/memory_bytes_flag.h` created
- [ ] 11 files updated to include it
- [ ] All tests passing
- [ ] Compilation time measured

### Phase 2 Complete
- [ ] `common/arg_slice.h` created
- [ ] `facade/facade_types_fwd.h` created
- [ ] `server/common.h` updated
- [ ] 40-50 files updated to use _fwd.h
- [ ] All tests passing
- [ ] Final compilation time measured

### PR Submission
- [ ] Before/after metrics documented
- [ ] All tests passing
- [ ] No new compiler warnings
- [ ] IWYU analysis clean
- [ ] Code review passed

## Timeline

- **Phase 1**: 30-45 minutes
- **Phase 2A**: 30 minutes
- **Phase 2B**: 1 hour
- **Phase 2C**: 1-2 hours (can be done incrementally)
- **Testing**: 30 minutes
- **Documentation**: 30 minutes
- **Total**: ~4-5 hours of work

Can be spread across multiple days for review/testing between phases.

## Success Criteria

### Must Have
- ✅ All tests pass
- ✅ No new compiler warnings
- ✅ Compilation time reduced
- ✅ IWYU analysis happy

### Should Have  
- ✅ 15%+ compilation time reduction
- ✅ 40+ files using facade_types_fwd.h
- ✅ server/common.h uses lightweight header

### Nice to Have
- ✅ 20% compilation time reduction
- ✅ Clear path for future optimizations
- ✅ Documentation of approach for team

## Risk Mitigation

### Low Risk Items ✅
- Creating new headers
- Moving single typedef
- Backward compatible changes

### Medium Risk Items ⚠️
- Updating server/common.h (many dependents)
  - **Mitigation**: Test compilation after change
  - **Rollback**: Simple git revert

### What Could Go Wrong
1. **Compilation errors in downstream files**
   - **Likelihood**: Low (new headers include needed deps)
   - **Fix**: Add missing includes to failing files

2. **Include order issues**
   - **Likelihood**: Very low (headers self-contained)
   - **Fix**: Ensure all headers have include guards and deps

3. **ODR violations**
   - **Likelihood**: None (no duplicate definitions)
   - **Prevention**: Use include, not duplicate

## Conclusion

**Recommendation**: ✅ Proceed with implementation

**Why**:
1. Clear, measurable benefit (15-20% compilation time reduction)
2. Low risk (incremental, backward compatible)
3. Minimal effort (4-5 hours total)
4. Follows best practices (IWYU, modular headers)
5. Easy rollback if needed

**Not recommended**:
- Doing nothing (compilation times remain slow)
- Big refactoring (too risky, not "minimal")
- Pimpl pattern (too invasive)

**The strategy is sound, low-risk, and high-impact. Recommend proceeding.**

---

## Questions?

See detailed documentation:
- `INCLUDE_REDUCTION_STRATEGY.md` - Full strategy explanation
- `DETAILED_IMPLEMENTATION.md` - Step-by-step implementation guide
- `EDGE_CASES_ANALYSIS.md` - Edge cases and technical deep-dive
