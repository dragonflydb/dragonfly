---
description: 'Code review instructions for Dragonfly'
applyTo: '**'
excludeAgent: ["coding-agent"]
---

# Dragonfly Code Review Instructions

Dragonfly is a high-performance, Redis-compatible in-memory data store written in C++20 with a unique shared-nothing, fiber-based architecture. Code reviews must prioritize correctness, security, and architectural compliance specific to this threading model.

## Review Priorities

### 🔴 CRITICAL (Block merge immediately)

**Threading Model Violations** (causes deadlocks/crashes):
- ❌ **NEVER** use `std::thread`, `std::mutex`, `std::condition_variable`, or standard library threading primitives
- ✅ **ALWAYS** use fiber-aware equivalents: `util::fb2::Mutex`, `util::fb2::Fiber`, `util::fb2::CondVar` from `util/fibers/`

**Architecture Violations**:
- ❌ Cross-shard data access without proper synchronization
- ✅ Per-shard operations only (see `src/server/db_slice.cc` for patterns)

**Security Vulnerabilities**:
- Authentication/authorization bypass in ACL code (`src/server/acl/`)
- Exposed secrets, credentials in code or logs
- Buffer overflows, use-after-free, memory safety issues

**Correctness Issues**:
- Race conditions in fiber scheduling
- Logic errors in transaction handling (`src/server/transaction.cc`)
- Data corruption risks in DashTable operations (`src/core/dash.h`)

### 🟡 IMPORTANT (Requires discussion)

**Code Quality**:
- Missing error handling (should return `OpStatus` from `facade/op_status.h`)
- Obvious memory leaks (check ASAN reports)
- Performance bottlenecks in hot paths (unnecessary allocations, N+1 patterns)

**Test Coverage**:
- New features without tests (both C++ unit tests and Python integration tests)
- Changes to critical paths (transactions, replication, cluster) without test coverage
- Modified code that fails existing tests

**Style Violations** (severe only):
- Not following naming conventions: `snake_case` variables, `PascalCase` functions, `kPascalCase` constants
- Code that won't pass pre-commit hooks (clang-format, 100 char limit)

### 🟢 SUGGESTIONS (Non-blocking, comment only if obvious)

- Over-engineering: adding abstraction layers, feature flags, or configurability not requested
- Missing comments on complex fiber synchronization logic
- Premature optimization without profiling

## Dragonfly-Specific Patterns

### ✅ DO: Correct Patterns

**Threading & Synchronization**:
```cpp
// ✅ CORRECT: Fiber-aware mutex
util::fb2::Mutex mutex_;
std::lock_guard<util::fb2::Mutex> lock(mutex_);

// ✅ CORRECT: Fiber-aware operations
util::fb2::Fiber fb = util::fb2::Fiber("name", [&] { /* work */ });
```


**Per-Shard Design**:
```cpp
// ✅ CORRECT: Operate on shard-local data
void DbSlice::SomeOperation() {
  // Access only this shard's data
  auto& db_slice = cntx->ns->GetCurrentDbSlice();
}
```

### ❌ DON'T: Anti-Patterns

**Threading**:
```cpp
// ❌ WRONG: Standard library threading (causes deadlocks!)
std::mutex mutex_;
std::thread worker;
std::condition_variable cv_;
```

**Global State**:
```cpp
// ❌ WRONG: Global mutable state (breaks shared-nothing architecture)
static std::unordered_map<string, int> global_cache;
```

**Build Commands**:
- ❌ Don't suggest `./tools/docker/build.sh` or `make` for incremental builds
- ✅ Use `cd build-dbg && ninja <target>` instead

## Code Review Checklist

When reviewing Dragonfly code, verify:

1. **Architecture Compliance**:
   - [ ] No standard library threading primitives (`std::thread`, `std::mutex`)
   - [ ] No global mutable state
   - [ ] Fiber-aware synchronization used correctly
   - [ ] Follows per-shard, shared-nothing design

2. **Security**:
   - [ ] No OWASP vulnerabilities (injection, XSS, auth bypass)
   - [ ] No hardcoded secrets or credentials
   - [ ] Input validation on command arguments
   - [ ] Safe memory operations (no buffer overflows)

3. **Testing**:
   - [ ] New functionality has test coverage
   - [ ] Tests build and pass: `cd build-dbg && ninja <test> && ./<test>`
   - [ ] No test regressions

4. **Style & Formatting**:
   - [ ] Follows naming conventions (snake_case vars, PascalCase functions)
   - [ ] Will pass pre-commit checks (clang-format, 100 char limit)
   - [ ] Code compiles without warnings (CI uses `-Werror`)

5. **Helio Submodule**:
   - [ ] No direct edits to `helio/` directory (it's a git submodule)

## Common False Positives to Ignore

These are **NOT** issues in Dragonfly's design. Do not comment on:

1. **Single-threaded-looking code**: Per-shard operations intentionally avoid locks
2. **Custom allocators**: mimalloc is used intentionally for performance
3. **Manual memory management**: Required for performance-critical paths
4. **Complex template metaprogramming**: DashTable uses advanced C++20 features
5. **Missing const**: Not always applicable in high-performance code

## Review Style Guidelines

1. **Be specific**: Reference file:line, explain WHY it's wrong
2. **Show examples**: Demonstrate the correct pattern with code
3. **Prioritize**: Security and correctness over style
4. **Link to docs**: Reference `docs/df-share-nothing.md`, `docs/transaction.md`, etc.
5. **Be concise**: Dragonfly team values focused, actionable feedback

## Example Review Comments

**❌ BAD - Too noisy**:
> "Consider using auto here for type inference"

**✅ GOOD - Actionable and specific**:
> "🔴 CRITICAL: Line 42 uses `std::mutex`. This will cause fiber deadlocks. Replace with `util::fb2::Mutex` from helio/util/fibers/. See src/server/set_family.cc:123 for correct pattern."

**✅ GOOD - Security focused**:
> "🔴 SECURITY: Line 58 doesn't validate `user_input` before passing to eval(). Vulnerable to command injection. Add validation or use SafeEval()."

**✅ GOOD - Architecture violation**:
> "🟡 ARCHITECTURE: Line 91 accesses global `cache_map`. Dragonfly uses shared-nothing design - each shard must have its own cache. See docs/df-share-nothing.md"

---

**Key Files Reference**: See AGENTS.md for complete codebase structure, build commands, and testing procedures.
