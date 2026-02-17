# Suggestions for Reducing Header Dependencies

This document provides recommendations for further reducing compilation dependencies in the Dragonfly codebase, based on analysis of issue #4713.

## Completed Improvements

### Move Simple Enums to common_types.h ✅

**Status**: Completed in this PR

Moved the following enums from `src/server/common.h` to `src/server/common_types.h`:
- `GlobalState` enum (ACTIVE, LOADING, SHUTTING_DOWN, TAKEN_OVER)
- `TimeUnit` enum (SEC, MSEC)
- `ExpireFlags` enum (EXPIRE_NX, EXPIRE_XX, EXPIRE_GT, EXPIRE_LT, etc.)

**Impact**: Files that only need these enums can now include `common_types.h` (which only includes `<cstdint>`) instead of `common.h` (which includes `facade/facade_types.h` and other heavy dependencies).

## Additional Opportunities

### 1. Forward Declarations Instead of Includes

**Impact**: Medium-High

Many header files include other headers when forward declarations would suffice. This is especially beneficial for class pointers and references.

**Example Pattern**:
```cpp
// Instead of:
#include "server/engine_shard.h"

// Use:
namespace dfly {
class EngineShard;
}
```

**Recommended Investigation**:
- Scan header files for cases where only pointers/references to classes are used
- Replace includes with forward declarations where possible
- Keep includes only in .cc files

### 2. Pimpl Idiom for Complex Headers

**Impact**: Medium

For frequently-included headers with large private sections, consider using the Pimpl (Pointer to Implementation) idiom to hide implementation details.

**Example**:
```cpp
// header.h
class MyClass {
 public:
  MyClass();
  ~MyClass();
  void DoSomething();
 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

// implementation.cc
class MyClass::Impl {
  // All private members and heavy includes here
};
```

**Candidates**: Classes with many private members that require heavy includes

### 3. Split Large Headers

**Impact**: Low-Medium

Some headers combine multiple responsibilities. Consider splitting them into focused headers.

**Example**: `facade/facade_types.h` currently contains:
- Protocol and CollectionType enums (lightweight)
- ParsedArgs class (depends on backed_args.h)
- ErrorReply struct
- Type aliases and helper functions

Could be split into:
- `facade/protocol_types.h` - Just Protocol and CollectionType enums
- `facade/facade_types.h` - Rest of the content

**Note**: The analysis shows Protocol/CollectionType are only used in 2 headers, so the benefit is limited.

### 4. Review External Dependencies

**Impact**: Low (we don't control these)

The most impactful headers are external:
- `absl/types/span.h` - 94 files
- `absl/container/inlined_vector.h` - 88 files
- `absl/container/flat_hash_set.h` - 75 files
- `absl/container/flat_hash_map.h` - 73 files

**Recommendation**: 
- Ensure these are only included where truly needed
- Consider forward-declaring container types where possible (e.g., pass by const reference)

### 5. Type Aliases in Lightweight Headers

**Impact**: Low

Type aliases like `using ArgSlice = absl::Span<const std::string_view>` cannot be moved to lightweight headers because they depend on the template. However, we can:
- Document which headers provide which type aliases
- Ensure consistent usage patterns
- Avoid redundant typedefs

## Impact Analysis Summary

Based on the include analysis from issue #4713, the most impactful headers are:

| Header | Files Impacted | Type | Reduction Potential |
|--------|----------------|------|---------------------|
| absl/types/span.h | 94 | External | Low (external) |
| absl/container/inlined_vector.h | 88 | External | Low (external) |
| base/iterator.h | 79 | Helio | Low (used widely) |
| common/arg_range.h | 78 | Dragonfly | Medium (review usage) |
| facade/op_status.h | 70 | Dragonfly | Low (already minimal) |
| common/backed_args.h | 69 | Dragonfly | Medium (review usage) |
| server/common.h | 46 | Dragonfly | **High ✅ (done)** |
| facade/facade_types.h | 67 | Dragonfly | Low-Medium (complex) |

## Measuring Impact

To measure the impact of header dependency reductions:

1. **Compilation time**: Time a full rebuild before and after changes
   ```bash
   time ninja clean && time ninja dragonfly
   ```

2. **Incremental compilation**: Touch a header and measure rebuild time
   ```bash
   touch src/server/common.h && time ninja dragonfly
   ```

3. **Include analysis**: Use tools like `include-what-you-use` to identify unnecessary includes
   ```bash
   include-what-you-use src/server/*.cc
   ```

## Best Practices Going Forward

1. **Include only what you use**: Avoid "convenience" includes
2. **Prefer forward declarations**: Use them in headers whenever possible
3. **Keep common_types.h minimal**: Only add truly fundamental, dependency-free types
4. **Document dependencies**: Add comments explaining why heavy includes are needed
5. **Regular audits**: Periodically review includes for optimization opportunities

## Tools and Resources

- **include-what-you-use (IWYU)**: Analyzes includes and suggests removals
- **ClangBuildAnalyzer**: Visualizes compilation time by file
- **Compilation database**: Use `compile_commands.json` for analysis tools

## References

- Issue #4713: https://github.com/dragonflydb/dragonfly/issues/4713
- Include What You Use: https://include-what-you-use.org/
- C++ Core Guidelines on includes: https://isocpp.github.io/CppCoreGuidelines/CppCoreGuidelines#sf8-use-include-guards-for-all-h-files
