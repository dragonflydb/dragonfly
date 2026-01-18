# Security Patches for Dragonfly Vulnerabilities

This directory contains patches for critical security vulnerabilities discovered during security assessment.

## Patch 1: UDS ACL Bypass Fix (CRITICAL)

**File:** `001-fix-uds-acl-bypass.patch`
**CVE:** TBD
**Severity:** CRITICAL

### Description
Fixes the Unix Domain Socket ACL bypass vulnerability where connections via UDS completely bypassed authentication and ACL validation.

### Changes
- Removes automatic `skip_acl_validation = true` for UDS connections
- Adds `--uds_skip_acl` flag for backward compatibility (disabled by default)
- Adds warning when UDS connections skip ACL validation
- Updates documentation

### Testing
```bash
# Build and test
./helio/blaze.sh
cd build-dbg && ninja security_vulnerability_test
./security_vulnerability_test --gtest_filter="*UDS_ACL*"
```

---

## Patch 2: Lua Script ACL Enforcement (HIGH)

**File:** `002-fix-lua-acl-bypass.patch`
**CVE:** TBD
**Severity:** HIGH

### Description
Ensures Lua scripts executed via EVAL/EVALSHA respect the caller's ACL permissions and cannot escalate privileges.

### Changes
- Saves and restores `skip_acl_validation` state in `CallFromScript`
- Forces ACL validation for user-originated Lua scripts
- Maintains exception for internal/replication contexts

### Testing
```bash
cd build-dbg && ninja security_vulnerability_test
./security_vulnerability_test --gtest_filter="*Lua_Script*"
```

---

## Patch 3: Admin Port Security Hardening (HIGH)

**File:** `003-harden-admin-port-security.patch`
**CVE:** TBD
**Severity:** HIGH

### Description
Strengthens admin port security by deprecating `--admin_nopass` and adding security warnings.

### Changes
- Deprecates `--admin_nopass` flag (still works but logs warning)
- Adds runtime warning when admin port is bound to non-loopback
- Improves documentation on admin port security

### Testing
```bash
# Test with admin_nopass (should see warning)
./dragonfly --admin_port=6380 --admin_nopass=true

# Should see: WARNING: admin_nopass is deprecated and insecure
```

---

## Patch 4: Buffer Overflow Mitigations (MEDIUM)

**File:** `004-fix-buffer-overflows.patch`
**CVE:** TBD
**Severity:** MEDIUM

### Description
Fixes buffer overflow vulnerabilities in legacy C code by replacing unsafe functions with bounds-checked alternatives.

### Changes
- Replaces `memcpy` with bounds checking in critical paths
- Adds assertions for buffer bounds
- Enables ASAN by default in debug builds

### Testing
```bash
# Build with ASAN
./helio/blaze.sh -DWITH_ASAN=ON
cd build-dbg && ninja dragonfly
./dragonfly --alsologtostderr
```

---

## Application Instructions

### For Maintainers
```bash
# Apply all patches
git am patches/001-fix-uds-acl-bypass.patch
git am patches/002-fix-lua-acl-bypass.patch
git am patches/003-harden-admin-port-security.patch
git am patches/004-fix-buffer-overflows.patch

# Build and test
./helio/blaze.sh
cd build-dbg && ninja
ctest -V -L DFLY

# Run security tests
./security_vulnerability_test
```

### For Users (Binary Releases)
If you cannot immediately upgrade to a patched version:

1. **Never use `--admin_nopass` in production**
2. **Restrict Unix socket permissions to `0600`**
3. **Bind admin port to loopback only: `--admin_bind=127.0.0.1`**
4. **Enable TLS for all external connections**
5. **Use firewall rules to restrict network access**
6. **Enable comprehensive ACL policies**

---

## Verification

After applying patches, run the security test suite:

```bash
cd build-dbg
./security_vulnerability_test

# All tests in "Verify_Security_Fixes_Work" should PASS
# All tests ending in "_VULNERABILITY" should now fail (attacks prevented)
```

---

## Disclosure Timeline

- **2026-01-18:** Vulnerabilities discovered
- **2026-01-18:** Initial patches developed
- **TBD:** Review by core maintainers
- **TBD:** Release patched versions
- **TBD+90 days:** Public disclosure

---

## Credits

Security research and patches by: [Security Team]
Contact: [security contact]
