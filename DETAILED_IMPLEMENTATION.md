# Detailed Implementation Guide

## Phase 1: Extract MemoryBytesFlag (30-45 minutes)

### Step 1.1: Create New Header

**File**: `src/facade/memory_bytes_flag.h`
```cpp
// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include "strings/human_readable.h"

namespace facade {
using MemoryBytesFlag = strings::MemoryBytesFlag;
}  // namespace facade
```

### Step 1.2: Update facade_types.h

**Remove** line 209:
```cpp
using MemoryBytesFlag = strings::MemoryBytesFlag;  // DELETE THIS
```

**Remove** include (line 15):
```cpp
#include "strings/human_readable.h"  // DELETE THIS
```

### Step 1.3: Update Files Using MemoryBytesFlag

Add `#include "facade/memory_bytes_flag.h"` to these files:

1. `src/server/main_service.cc` (lines 96, 1068 use it)
2. `src/server/config_registry.cc` (lines 86-88 use it)
3. `src/server/engine_shard_set.cc` (line 23 uses it)
4. `src/server/dfly_main.cc` (line 74 uses it)
5. `src/facade/dragonfly_connection.cc` (lines 64, 67, 80 use it)

**Example change** for `src/server/main_service.cc`:
```cpp
// Add after other includes:
#include "facade/memory_bytes_flag.h"
```

### Step 1.4: Build and Test
```bash
cd build
make -j$(nproc)
# If successful, run tests
ctest -j$(nproc)
```

### Expected Result
- ✅ ~56 files no longer transitively include strings/human_readable.h
- ⏱️ Estimated 3-5% compilation time reduction

---

## Phase 2: Create Lightweight Type Header (1-2 hours)

### Step 2.1: Create facade_types_fwd.h

**File**: `src/facade/facade_types_fwd.h`
```cpp
// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <string_view>
#include <vector>

namespace cmn {
using ArgSlice = absl::Span<const std::string_view>;
}

namespace facade {

// Lightweight type aliases that don't require heavy headers
using MutableSlice = std::string_view;
using CmdArgVec = std::vector<std::string_view>;
using CmdArgList = cmn::ArgSlice;

// Simple enums - no dependencies
enum class Protocol : uint8_t { MEMCACHE = 1, REDIS = 2 };
enum class CollectionType : uint8_t { ARRAY, SET, MAP, PUSH };

// Forward declarations for complex types defined in facade_types.h
class ParsedArgs;
struct ErrorReply;
struct MemcacheCmdFlags;

}  // namespace facade
```

### Step 2.2: Update facade_types.h

Add at the top (after pragma once):
```cpp
#include "facade/facade_types_fwd.h"
```

**Remove** these lines (now in _fwd.h):
```cpp
using MutableSlice = std::string_view;
using CmdArgVec = std::vector<std::string_view>;
using cmn::ArgSlice;
using CmdArgList = cmn::ArgSlice;

enum class Protocol : uint8_t { MEMCACHE = 1, REDIS = 2 };
enum class CollectionType : uint8_t { ARRAY, SET, MAP, PUSH };
```

### Step 2.3: Update server/common.h (HIGHEST IMPACT)

**Before** (line 13):
```cpp
#include "facade/facade_types.h"
```

**After**:
```cpp
#include "facade/facade_types_fwd.h"  // Only need type aliases
```

**Verify** server/common.h doesn't use:
- `ParsedArgs` - ✅ Not used in header
- `ErrorReply` - ✅ Not used in header  
- `MemcacheCmdFlags` - ✅ Not used in header

**Uses** (all available in _fwd.h):
- `CmdArgList` ✅
- `CmdArgVec` ✅
- `MutableSlice` ✅
- `OpResult` - needs facade/op_status.h (already included separately)

### Step 2.4: Identify and Update Other Candidates

**Files that might only need _fwd.h**:

Use this script to find candidates:
```bash
# Find headers including facade_types.h but not using ParsedArgs/ErrorReply
for f in src/**/*.h; do
  if grep -q '#include.*facade_types.h' "$f"; then
    if ! grep -q 'ParsedArgs\|ErrorReply\|MemcacheCmdFlags' "$f"; then
      echo "Candidate: $f"
    fi
  fi
done
```

**Likely candidates** based on earlier analysis:
1. `src/server/family_utils.h` - uses only `CmdArgList`
2. `src/server/generic_family.h` - uses only `CmdArgList`  
3. `src/server/set_family.h` - check usage
4. `src/server/cluster/cluster_defs.h` - check usage

**For each candidate**:
1. Verify it doesn't use `ParsedArgs`, `ErrorReply`, or `MemcacheCmdFlags`
2. Change include from `facade_types.h` to `facade_types_fwd.h`
3. Compile and test

### Step 2.5: Update CMakeLists.txt (if needed)

If the build system needs to know about the new header:
```cmake
# In src/facade/CMakeLists.txt or equivalent
# Add facade_types_fwd.h to the list of headers
```

### Step 2.6: Build and Test
```bash
cd build
cmake ..
make -j$(nproc)
ctest -j$(nproc)
```

### Expected Result
- ✅ 40-50 files no longer include arg_range.h → absl/span.h chain
- ✅ 40-50 files no longer include backed_args.h → absl/inlined_vector.h chain
- ⏱️ Estimated 10-15% compilation time reduction

---

## Phase 3: Measurement and Validation

### Measurement Script

Create `measure_impact.sh`:
```bash
#!/bin/bash

echo "=== Measuring Include Impact ==="

echo -e "\n1. Files including arg_range.h:"
grep -r "#include.*arg_range.h" src/ --include="*.h" --include="*.cc" | wc -l

echo -e "\n2. Files including backed_args.h:"
grep -r "#include.*backed_args.h" src/ --include="*.h" --include="*.cc" | wc -l

echo -e "\n3. Files including facade_types.h:"
grep -r "#include.*facade_types.h" src/ --include="*.h" --include="*.cc" | wc -l

echo -e "\n4. Files including facade_types_fwd.h:"
grep -r "#include.*facade_types_fwd.h" src/ --include="*.h" --include="*.cc" | wc -l

echo -e "\n5. Files including human_readable.h:"
grep -r "#include.*human_readable.h" src/ --include="*.h" --include="*.cc" | wc -l

echo -e "\n=== Compilation Time ==="
cd build
time make clean
time make -j$(nproc) 2>&1 | tee ../build_time.log
```

### Run Measurements

**Before any changes**:
```bash
git checkout main
bash measure_impact.sh > baseline_metrics.txt
```

**After Phase 1**:
```bash
bash measure_impact.sh > phase1_metrics.txt
diff baseline_metrics.txt phase1_metrics.txt
```

**After Phase 2**:
```bash
bash measure_impact.sh > phase2_metrics.txt
diff phase1_metrics.txt phase2_metrics.txt
```

### Success Criteria

**Phase 1**:
- [ ] All tests pass
- [ ] ~11 files include memory_bytes_flag.h
- [ ] ~50-60 fewer files transitively include human_readable.h
- [ ] Compilation time reduced by 3-5%

**Phase 2**:
- [ ] All tests pass
- [ ] ~40-50 files include facade_types_fwd.h instead of facade_types.h
- [ ] server/common.h uses facade_types_fwd.h
- [ ] ~40-50 fewer files transitively include arg_range.h and backed_args.h
- [ ] Compilation time reduced by 10-15%

---

## Rollback Plan

If anything goes wrong:

### Phase 1 Rollback
```bash
git checkout src/facade/facade_types.h
git rm src/facade/memory_bytes_flag.h
git checkout src/server/*.cc src/facade/*.cc
```

### Phase 2 Rollback
```bash
git checkout src/facade/facade_types.h
git rm src/facade/facade_types_fwd.h
git checkout src/server/*.h
```

---

## Additional Opportunities (Future Work)

### Opportunity 1: Split op_status.h

If `facade/op_status.h` becomes a bottleneck later:

Create `facade/op_status_fwd.h`:
```cpp
namespace facade {
enum class OpStatus : uint16_t;  // Forward declare enum
template <typename T> class OpResult;  // Forward declare template
}
```

But **NOT recommended now** because:
- Most files need the full definition for `OpResult<T>`
- Template implementations can't be separated
- Limited benefit

### Opportunity 2: Check util/fibers Headers

`server/common.h` includes:
```cpp
#include "util/fibers/fibers.h"
#include "util/fibers/synchronization.h"
```

Could these be:
- Forward declared?
- Moved to .cc files?
- Split into lighter headers?

**Investigation needed** - but not part of minimal strategy.

### Opportunity 3: Analyze tx_base.h

`server/tx_base.h` impacts 49 files. Could it be:
- Split into interface and implementation?
- Use forward declarations?

**Low priority** - smaller impact than facade_types.h.

---

## Testing Checklist

After all changes:

- [ ] Clean build succeeds: `make clean && make -j$(nproc)`
- [ ] All tests pass: `ctest -j$(nproc)`
- [ ] No new compiler warnings
- [ ] IWYU is happy: `iwyu_tool.py -p build/ src/`
- [ ] Include-what-you-use doesn't suggest reverting changes
- [ ] Compilation time measured and documented

---

## Documentation Updates

After implementation:

1. **Update CONTRIBUTORS.md** if this is a significant contribution
2. **Update this document** with actual results:
   - Actual compilation time improvements
   - Number of files impacted
   - Any issues encountered
3. **Create PR description** with:
   - Before/after metrics
   - Rationale for changes
   - Testing performed

---

## Example PR Description Template

```markdown
## Reduce Compilation Time via Include Optimization

### Problem
Include analysis showed that `facade_types.h` was pulling in 4 heavy headers 
unnecessarily for files that only needed simple type aliases.

### Solution  
- **Phase 1**: Extracted `MemoryBytesFlag` to separate header
- **Phase 2**: Created `facade_types_fwd.h` for lightweight types

### Impact
- ✅ X files no longer include `strings/human_readable.h` 
- ✅ Y files no longer include heavy `arg_range.h`/`backed_args.h` chain
- ✅ Clean build time reduced from A to B (C% improvement)

### Changes
1. Created `facade/memory_bytes_flag.h`
2. Created `facade/facade_types_fwd.h`
3. Updated `server/common.h` and N other files to use lightweight header

### Testing
- [x] All tests pass
- [x] Clean build succeeds
- [x] IWYU analysis clean
- [x] No new warnings

### Metrics
[Include before/after measurements here]
```
