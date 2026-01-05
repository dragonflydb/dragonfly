# Feature Request: Implement DIGEST and DELEX Commands

## Issue Type
Feature Request

## Summary
Request to implement the `DIGEST` and `DELEX` commands to enhance Dragonfly's compatibility with the latest Redis command set and enable atomic conditional operations on string keys.

## Description

### DIGEST Command
The `DIGEST` command should return a hash digest for the value stored at a specified key. This digest should be a fixed-size hexadecimal string representation of the value, computed using the XXH3 hash algorithm.

**Syntax:**
```
DIGEST key
```

**Behavior:**
- Returns a hexadecimal string digest of the value stored at the key
- Returns `nil` if the key does not exist
- Returns an error if the key exists but is not a string type
- The digest should be computed using the XXH3 hash algorithm for consistency

**Use Cases:**
- Efficient comparison of large string values without transferring full content
- Optimistic concurrency control patterns
- Integrity verification of stored values
- Support for conditional operations using digest comparison

**Complexity:**
- Time: O(N) where N is the length of the string value
- Space: O(1) for the returned digest

### DELEX Command
The `DELEX` command should provide conditional key deletion based on value or digest comparison. This enables atomic compare-and-delete operations.

**Syntax:**
```
DELEX key [IFEQ value | IFNE value | IFDEQ digest | IFDNE digest]
```

**Options:**
- `IFEQ value`: Delete the key only if its value is exactly equal to the specified value
- `IFNE value`: Delete the key only if its value is not equal to the specified value
- `IFDEQ digest`: Delete the key only if its hash digest matches the provided digest
- `IFDNE digest`: Delete the key only if its hash digest does NOT match the provided digest
- No option: Behaves like standard `DEL` command

**Behavior:**
- Returns the number of keys deleted (0 or 1)
- When using conditional options, the key must be a string type
- For digest-based conditions, the digest is computed using XXH3 algorithm

**Use Cases:**
- Atomic delete-if-value-matches operations
- Safe distributed locking (delete lock only if you still own it)
- Optimistic concurrency control for deletions
- Concurrent cleanup operations in distributed environments

**Complexity:**
- `IFEQ`/`IFNE`: O(1) if values are equal length, O(N) for comparison where N is value length
- `IFDEQ`/`IFDNE`: O(N) where N is the length of the string value (digest computation required)

## Benefits

1. **Enhanced Compatibility**: Improves Dragonfly's command compatibility with the latest Redis releases
2. **Atomic Operations**: Enables efficient atomic conditional operations without Lua scripts
3. **Performance**: Digest-based operations allow efficient comparison of large values
4. **Concurrency Control**: Supports optimistic locking patterns natively
5. **Developer Experience**: Simplifies implementation of common distributed system patterns

## Implementation Considerations

### For DIGEST:
- Integrate XXH3 hashing library (likely already available or needs to be added)
- Implement digest computation for string values
- Handle all string encodings properly (raw, integer-encoded, etc.)
- Add appropriate error handling for non-string types
- Consider caching digests for frequently accessed keys (optional optimization)

### For DELEX:
- Implement conditional logic for all four comparison modes
- Reuse DIGEST computation logic for digest-based conditions
- Ensure atomic execution within Dragonfly's transaction framework
- Handle journal/replication appropriately for conditional deletes
- Maintain consistency with existing DEL command behavior when no condition is specified

### Integration Points:
- Add commands to `src/server/generic_family.cc` or `src/server/string_family.cc`
- Follow existing command registration patterns
- Add appropriate command flags (e.g., `CO::WRITE`, `CO::FAST`)
- Ensure proper shard-aware execution
- Add replication journal support

### Testing Requirements:
- Unit tests for DIGEST computation accuracy
- Tests for all DELEX conditional modes
- Edge cases: empty strings, very large strings, non-existent keys
- Type mismatch error handling
- Replication consistency tests
- Integration tests with existing commands (e.g., SET, GET, DEL)
- Performance benchmarks for digest computation

## Additional Context

These commands are part of a broader set of atomic conditional operations that enhance support for distributed systems patterns like:
- Compare-and-set operations
- Compare-and-delete operations
- Optimistic locking
- Lock-free synchronization

The DIGEST command can also be used in conjunction with enhanced SET command options (if/when implemented) such as `IFDEQ` and `IFDNE` for conditional set operations.

## References
- Redis DIGEST command documentation: https://redis.io/docs/latest/commands/digest/
- Redis DELEX command documentation: https://redis.io/docs/latest/commands/delex/
- XXH3 hash algorithm: https://xxhash.com/

## Priority
Medium - These commands enhance compatibility and enable useful concurrency patterns, though workarounds exist using Lua scripts or other mechanisms.

## Labels
- feature request
- redis-compatibility
- commands
