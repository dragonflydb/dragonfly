# Minimal High-Impact Strategy for Include Reduction

## Executive Summary

After analyzing the codebase and IWYU results, here's a **minimal-change, high-impact** strategy to reduce compilation times. The key insight is that we can create lightweight forward/type declaration headers to break the heavy dependency chains.

## Analysis Summary

### Current Situation

The top 5 impactful headers create a dependency chain:

```
facade_types.h (67 files) includes:
  ├── common/arg_range.h (78 files) includes:
  │     ├── absl/types/span.h (94 file impact)
  │     └── base/iterator.h (79 file impact)
  ├── common/backed_args.h (69 files) includes:
  │     └── absl/container/inlined_vector.h (88 file impact)
  ├── facade/op_status.h (70 files)
  └── strings/human_readable.h (68 files)
```

**Problem**: Including `facade_types.h` pulls in 4 heavy headers unnecessarily for files that only need simple typedefs.

### What We Found

1. **facade_types.h** exports:
   - Simple typedefs: `CmdArgList`, `CmdArgVec`, `MutableSlice` (just aliases)
   - Simple enums: `Protocol`, `CollectionType`
   - Complex classes: `ParsedArgs`, `ErrorReply` (these need the heavy includes)
   - One problematic typedef: `using MemoryBytesFlag = strings::MemoryBytesFlag`

2. **server/common.h** (46 files) only uses:
   - Simple typedefs from facade_types.h
   - `OpResult<T>` and `OpStatus`
   - Does NOT need `ParsedArgs` or `ErrorReply` in header

3. **Key observation**: 
   - Most files only need the simple typedefs, not `ParsedArgs`
   - `ParsedArgs` is what forces the include of `arg_range.h` and `backed_args.h`
   - The entire dependency chain exists for a small subset of actual usage

## RECOMMENDED STRATEGY: Create Lightweight Type Headers

### Strategy 1: Split facade_types.h (HIGHEST IMPACT, LOW RISK)

**Create `facade/facade_types_fwd.h`** with just the lightweight types:

```cpp
// facade/facade_types_fwd.h
#pragma once

#include <string_view>
#include <vector>
#include "common/arg_range.h"  // Only for ArgSlice typedef

namespace facade {

// Simple typedefs - no heavy dependencies
using MutableSlice = std::string_view;
using CmdArgVec = std::vector<std::string_view>;
using CmdArgList = cmn::ArgSlice;

// Simple enums - cheap to include
enum class Protocol : uint8_t { MEMCACHE = 1, REDIS = 2 };
enum class CollectionType : uint8_t { ARRAY, SET, MAP, PUSH };

// Forward declarations for complex types
class ParsedArgs;
struct ErrorReply;

}  // namespace facade
```

**Modify `facade/facade_types.h`**:
- Include `facade_types_fwd.h`
- Keep all existing functionality
- Add the heavy includes only here

**Impact**:
- Files needing only typedefs/enums can include `facade_types_fwd.h`
- This breaks the chain: `server/common.h` → `facade_types.h` → 4 heavy headers
- Estimated impact: **~40-50 files** could use the lightweight header

### Strategy 2: Move MemoryBytesFlag (MEDIUM IMPACT, LOW RISK)

**Issue**: `facade_types.h` includes `strings/human_readable.h` (68 file impact) just for:
```cpp
using MemoryBytesFlag = strings::MemoryBytesFlag;
```

**Solution**: Create `facade/memory_bytes_flag.h`:
```cpp
// facade/memory_bytes_flag.h
#pragma once
#include "strings/human_readable.h"

namespace facade {
using MemoryBytesFlag = strings::MemoryBytesFlag;
}
```

**Changes**:
1. Remove `strings/human_readable.h` include from `facade_types.h`
2. Remove the `MemoryBytesFlag` typedef from `facade_types.h`
3. Add `#include "facade/memory_bytes_flag.h"` to the ~11 files that actually use it

**Impact**:
- Removes heavy `strings/human_readable.h` from facade_types.h
- Only 11 files actually use `MemoryBytesFlag`
- Saves 68-11 = **~57 files** from including `strings/human_readable.h`

### Strategy 3: Audit server/common.h Dependencies (LOW HANGING FRUIT)

**Current includes** in `server/common.h`:
```cpp
#include "facade/facade_types.h"  // For simple typedefs
#include "facade/op_status.h"     // For OpStatus/OpResult
#include "util/fibers/fibers.h"
#include "util/fibers/synchronization.h"
```

**Opportunity**: 
- Change `facade/facade_types.h` → `facade/facade_types_fwd.h`
- Keep `facade/op_status.h` (needed for AggregateStatus template)
- **Impact**: server/common.h (46 files) no longer pulls in heavy arg_range.h, backed_args.h chains

### Strategy 4: Check for Unused Includes in Key Headers (QUICK WINS)

**Files to audit** (use IWYU or manual inspection):
- `facade/op_status.h` - very simple, probably clean
- `common/arg_range.h` - needs absl::Span and base::iterator (can't reduce)
- `common/backed_args.h` - needs absl::InlinedVector (can't reduce)
- `server/tx_base.h` - audit what it really needs

**Method**:
```bash
# For each file, check with IWYU
iwyu_tool.py -p build/ src/facade/op_status.h
iwyu_tool.py -p build/ src/server/tx_base.h
```

## Why NOT These Approaches

### ❌ Pimpl Pattern for ParsedArgs
- **Complexity**: High - requires refactoring all usage
- **Performance**: Adds indirection to hot path
- **Risk**: High - could break existing code

### ❌ Move Template Implementations to .cc Files  
- **Not Possible**: Templates need full definition at use site
- `OpResult<T>`, `ParsedArgs` are templates - can't be hidden

### ❌ Forward Declare absl::Span or absl::InlinedVector
- **Not Possible**: Used directly in type definitions and inline methods
- Can't forward declare types that are members or base classes

### ❌ Refactor arg_range.h to Remove Dependencies
- **High Risk**: Core infrastructure used everywhere
- **Low Reward**: The header is clean - dependencies are necessary
- base::iterator.h is needed for base::it::Wrap used in inline Range() method

## IMPLEMENTATION PLAN

### Phase 1: Quick Wins (1-2 hours)
1. ✅ Create `facade/memory_bytes_flag.h`
2. ✅ Update 11 files using MemoryBytesFlag to include new header
3. ✅ Remove MemoryBytesFlag from facade_types.h
4. ✅ Compile and test

**Expected impact**: 50-60 files avoid strings/human_readable.h

### Phase 2: Type Forward Header (2-3 hours)
1. ✅ Create `facade/facade_types_fwd.h` with lightweight types
2. ✅ Update `facade/facade_types.h` to include it
3. ✅ Identify files that can use _fwd.h instead of full header
4. ✅ Update those files (start with server/common.h)
5. ✅ Compile and test incrementally

**Expected impact**: 40-50 files avoid arg_range.h and backed_args.h chains

### Phase 3: Measure and Iterate (1 hour)
1. ✅ Measure compilation time improvement
2. ✅ Run IWYU again to find new opportunities
3. ✅ Identify next targets if needed

## MEASUREMENT

### Before
```bash
time make -j$(nproc) clean all
```

### After Each Phase
```bash
time make -j$(nproc) clean all
# Compare times, focus on:
# - Total wall time
# - Time for server/* files
# - Time for facade/* files
```

### Expected Results
- **Phase 1**: 5-10% reduction in files including strings/human_readable.h
- **Phase 2**: 10-15% reduction in overall compilation time
- **Combined**: 15-20% faster clean builds

## RISK ASSESSMENT

### Low Risk Changes ✅
- Creating new headers (doesn't break existing code)
- Moving single typedef (MemoryBytesFlag) - clear, isolated change

### Medium Risk Changes ⚠️  
- Updating server/common.h - many files depend on it
- Mitigation: Test compilation after each change

### High Risk Changes ❌
- NOT doing any Pimpl refactoring
- NOT touching core infrastructure (arg_range.h internals)

## ALTERNATIVES CONSIDERED

### A. Do Nothing
- **Pro**: No risk
- **Con**: Compilation times remain slow

### B. Full Dependency Refactor
- **Pro**: Maximum benefit
- **Con**: Weeks of work, high risk, not "minimal"

### C. Use Precompiled Headers (PCH)
- **Pro**: Can speed up compilation
- **Con**: Doesn't reduce actual dependencies, just masks the problem
- **Note**: Consider as complementary, not alternative

## CONCLUSION

**Recommended Approach**: Execute Phase 1 + Phase 2

**Why**:
1. **Minimal changes**: Only creating new headers and updating includes
2. **High impact**: Breaks the heavy dependency chain for 40-60 files
3. **Low risk**: No refactoring of existing code structure
4. **Measurable**: Clear before/after comparison
5. **Reversible**: Can easily revert if issues arise

**Next Steps**:
1. Get approval for this strategy
2. Create a branch for the changes
3. Implement Phase 1
4. Measure impact
5. Implement Phase 2
6. Measure total impact
7. Submit PR with measurements

---

## APPENDIX: Dependency Chain Visualization

### Current (Before)
```
server/foo.cc
  └── server/common.h
        └── facade/facade_types.h
              ├── common/arg_range.h
              │     ├── absl/types/span.h (heavy!)
              │     └── base/iterator.h (heavy!)
              ├── common/backed_args.h
              │     └── absl/container/inlined_vector.h (heavy!)
              ├── facade/op_status.h
              └── strings/human_readable.h (heavy!)
```

### After Phase 1
```
server/foo.cc
  └── server/common.h
        └── facade/facade_types.h
              ├── common/arg_range.h
              │     ├── absl/types/span.h (heavy!)
              │     └── base/iterator.h (heavy!)
              ├── common/backed_args.h
              │     └── absl/container/inlined_vector.h (heavy!)
              └── facade/op_status.h
              # strings/human_readable.h removed! ✅
```

### After Phase 2
```
server/foo.cc
  └── server/common.h
        └── facade/facade_types_fwd.h  # Lightweight!
              └── common/arg_range.h   # Only for ArgSlice typedef
                                        # BUT arg_range.h stays thin for this use
```

Only files that actually use ParsedArgs/ErrorReply include the full facade_types.h.
