# Security Vulnerability Assessment for Dragonfly

**Date:** 2026-01-18
**Assessment Type:** White-hat Security Research
**Focus Areas:** Privilege Escalation, Remote Code Execution, Buffer Overruns

## Executive Summary

This document outlines security vulnerabilities discovered in Dragonfly's authentication, authorization, and command execution systems. The assessment focuses on potential attack vectors for privilege escalation and remote code execution.

## Vulnerabilities Identified

### 1. **CRITICAL: ACL Bypass via Unix Domain Sockets** 
**Severity:** CRITICAL  
**CVE:** TBD  
**Location:** `src/server/main_service.cc:1884-1886`

#### Description
When connections are made via Unix Domain Sockets (UDS), ACL validation is completely bypassed by setting `skip_acl_validation = true`. This allows any local user with access to the UDS to execute any command regardless of ACL configuration.

#### Vulnerable Code
```cpp
if (owner->socket()->IsUDS()) {
  res->req_auth = false;
  res->skip_acl_validation = true;  // <-- VULNERABILITY
}
```

#### Attack Vector
1. Attacker gains local file system access (e.g., container escape, compromised service)
2. Connects to Dragonfly via Unix socket
3. Executes privileged commands (CONFIG, FLUSHALL, SCRIPT, etc.) without authentication
4. Can modify server configuration, delete data, or execute arbitrary Lua code

#### Impact
- Complete bypass of ACL system
- Privilege escalation from any local user to admin
- Data exfiltration and manipulation
- Potential for lateral movement in containerized environments

#### Recommended Fix
```cpp
if (owner->socket()->IsUDS()) {
  // UDS connections should still require authentication
  // unless explicitly configured otherwise via a secure flag
  res->req_auth = !user_registry_.AuthUser("default", "");
  // Only skip ACL validation if explicitly configured for trusted UDS connections
  res->skip_acl_validation = GetFlag(FLAGS_uds_trusted) && IsTrustedUDSPath(owner->socket()->GetPath());
}
```

---

### 2. **HIGH: Admin Port Authentication Bypass with `admin_nopass` Flag**
**Severity:** HIGH  
**CVE:** TBD  
**Location:** `src/server/main_service.cc:2862-2865`

#### Description
The `--admin_nopass` flag disables authentication on the admin port, allowing unauthenticated access to privileged commands. When combined with network misconfigurations, this can lead to remote privilege escalation.

#### Vulnerable Configuration
```cpp
bool should_skip_auth = (is_privileged && !RequirePrivilegedAuth()) || GetPassword().empty();

bool Service::RequirePrivilegedAuth() const {
  return !GetFlag(FLAGS_admin_nopass);  // <-- VULNERABILITY
}
```

#### Attack Vector
1. Dragonfly started with `--admin_nopass` flag (possibly for "convenience")
2. Admin port exposed to network (misconfigured firewall, container networking)
3. Attacker connects to admin port remotely
4. Executes privileged commands without authentication:
   - `CONFIG SET` - Modify server configuration
   - `SHUTDOWN` - Deny service
   - `BGSAVE /attacker/controlled/path` - Write files to arbitrary locations
   - `MODULE LOAD` - Load malicious modules
   - `SCRIPT LOAD` - Execute arbitrary Lua code

#### Impact
- Remote unauthenticated access to admin commands
- Complete server compromise
- Data exfiltration
- Denial of service
- Arbitrary file write via BGSAVE

#### Recommended Fix
1. Remove or deprecate `--admin_nopass` flag
2. Always require authentication on admin port
3. Add runtime warnings when admin port is bound to non-loopback interfaces
4. Implement IP whitelisting for admin port

---

### 3. **HIGH: Lua Script Privilege Escalation via ACL Bypass**
**Severity:** HIGH  
**CVE:** TBD  
**Location:** `src/server/main_service.cc:2029-2070`

#### Description
Lua scripts executed via EVAL/EVALSHA commands inherit the ACL permissions of the caller, BUT when scripts call Redis commands, the ACL checks use the connection's `skip_acl_validation` flag which may be set to true in certain contexts.

#### Vulnerable Code Flow
```cpp
void Service::CallFromScript(Interpreter::CallArgs& ca, CommandContext* cmd_cntx) {
  // ... script command execution ...
  DispatchCommand(ParsedArgs{ca.args}, cmd_cntx, AsyncPreference::ONLY_SYNC);
  // ACL validation happens in DispatchCommand -> VerifyCommandState
  // But if skip_acl_validation is set, validation is bypassed
}
```

#### Attack Vector
1. User with limited ACL permissions (e.g., only `GET` allowed)
2. Executes Lua script via EVAL:
```lua
-- User only has GET permission
redis.call('SET', 'key', 'value')  -- Should fail but might not in certain contexts
redis.call('FLUSHALL')              -- Should fail but might not in certain contexts
redis.call('CONFIG', 'SET', 'dir', '/tmp')  -- Privilege escalation
```
3. If connection context has `skip_acl_validation=true` (e.g., from replication, UDS), all commands succeed

#### Impact
- Users can escalate privileges beyond their ACL permissions
- Limited users can execute admin commands via Lua
- Data manipulation beyond authorized scope
- Potential for code execution via CONFIG commands

#### Recommended Fix
```cpp
void Service::CallFromScript(Interpreter::CallArgs& ca, CommandContext* cmd_cntx) {
  auto* cntx = cmd_cntx->server_conn_cntx();
  
  // CRITICAL: Ensure Lua scripts respect original ACL permissions
  // Save and temporarily disable skip_acl_validation
  bool original_skip_acl = cntx->skip_acl_validation;
  if (cntx->conn() != nullptr && !cntx->conn()->IsPrivileged()) {
    cntx->skip_acl_validation = false;  // Force ACL validation for user scripts
  }
  
  // ... execute command ...
  
  // Restore original state
  cntx->skip_acl_validation = original_skip_acl;
}
```

---

### 4. **MEDIUM: Command Restriction Bypass via `restricted_commands` Flag Timing**
**Severity:** MEDIUM  
**CVE:** TBD  
**Location:** `src/server/command_registry.cc:278-280`

#### Description
Commands are marked as restricted during registry initialization, but this happens after command definitions are loaded. There's a race condition window where commands might be executed before restriction is applied.

#### Vulnerable Code
```cpp
if (restricted_cmds_.find(k) != restricted_cmds_.end()) {
  cmd.SetRestricted(true);  // Applied during initialization
}
```

#### Attack Vector
1. During server startup, commands are registered
2. Brief window exists before restrictions are applied
3. Fast attacker might execute restricted command before restriction takes effect
4. More likely in cluster scenarios with rolling restarts

#### Impact
- Temporary bypass of command restrictions
- Window for privilege escalation during startup
- Race condition exploitation

#### Recommended Fix
- Apply restrictions atomically during command definition
- Add startup phase where no external connections are accepted until all security policies are in place
- Implement two-phase initialization: security first, then network

---

### 5. **MEDIUM: Buffer Overflows in Legacy Redis C Code**
**Severity:** MEDIUM  
**CVE:** TBD  
**Location:** Various files in `src/redis/` directory

#### Description
Dragonfly includes legacy C code from Redis that uses unsafe buffer operations. While much has been updated, several unsafe patterns remain:

#### Vulnerable Patterns Found
```c
// src/redis/sds.c - uses memcpy without bounds checking in some paths
// src/redis/listpack.c - manual pointer arithmetic
// src/redis/ziplist.c - deprecated but still present
// src/redis/util.c - sprintf usage in some paths
```

#### Attack Vector
1. Attacker sends specially crafted protocol messages
2. Message triggers unsafe buffer operation in legacy C code
3. Buffer overflow occurs
4. Potential for memory corruption, info leak, or RCE

#### Impact
- Memory corruption
- Information disclosure (memory leak)
- Potential remote code execution
- Server crash (DoS)

#### Recommended Fix
1. **Immediate:**
   - Run AddressSanitizer (ASAN) on all test suites
   - Audit all uses of: memcpy, strcpy, strcat, sprintf, gets
   - Replace with safe alternatives: memcpy_s, strncpy, snprintf
   
2. **Long-term:**
   - Migrate legacy C code to C++ with bounds checking
   - Add fuzzing tests for protocol parsing
   - Enable compiler security features: -fstack-protector-strong, -D_FORTIFY_SOURCE=2

---

### 6. **LOW: Information Disclosure via Error Messages**
**Severity:** LOW  
**CVE:** TBD  
**Location:** Various error handling paths

#### Description
Error messages sometimes leak internal information about system state, file paths, or user existence.

#### Examples
```cpp
// Leaks username existence
return ErrorReply(absl::StrCat("-NOPERM ", cntx->authed_username, " ", error_msg));

// Leaks internal paths in debug builds
VLOG(1) << "Non-admin attempt to execute " << cid.name() << " " << tail_args << " "
        << ConnectionLogContext(dfly_cntx.conn());
```

#### Impact
- Information gathering for targeted attacks
- Username enumeration
- Internal path disclosure

#### Recommended Fix
- Sanitize error messages
- Use generic error messages for authentication failures
- Limit verbose logging to debug builds
- Add flag to disable detailed error messages in production

---

## Testing Recommendations

### 1. Unit Tests
Create unit tests for each vulnerability:
- Test UDS ACL bypass
- Test admin_nopass authentication
- Test Lua script ACL enforcement
- Test command restriction enforcement

### 2. Integration Tests
- Test full attack chains
- Test cluster mode security
- Test replication security
- Test TLS enforcement

### 3. Fuzzing
- Protocol parser fuzzing with AFL/libFuzzer
- Lua script fuzzing
- RDB file format fuzzing
- Redis command fuzzing

### 4. Security Scanning
- Static analysis with CodeQL
- Dynamic analysis with Valgrind/ASAN
- Penetration testing of authentication/authorization
- Cluster security testing

---

## Secure Configuration Recommendations

### Minimum Security Baseline
```bash
# Disable dangerous flags
--admin_nopass=false

# Require authentication
--requirepass=<strong-password>

# Bind admin port to loopback only
--admin_bind=127.0.0.1

# Enable TLS for production
--tls
--tls_cert_file=/path/to/cert
--tls_key_file=/path/to/key

# Restrict Unix sockets permissions
# Set socket file to 0600 and owned by dragonfly user

# Enable ACL
--aclfile=/path/to/acl.conf

# Disable dangerous commands for non-admin users
--restricted_commands=FLUSHALL,FLUSHDB,CONFIG,SHUTDOWN,MODULE,SCRIPT
```

### Network Security
1. Use firewall rules to restrict admin port access
2. Deploy Dragonfly behind VPC/private network
3. Use TLS for all external connections
4. Implement rate limiting at load balancer level
5. Monitor for suspicious command patterns

### ACL Best Practices
1. Principle of least privilege - grant minimum necessary permissions
2. Separate users for different applications
3. Use read-only users for reporting/analytics
4. Restrict key patterns with glob rules
5. Regular ACL audits and reviews

---

## Remediation Priority

### Immediate (P0)
1. Fix UDS ACL bypass - deploy patch immediately
2. Document and warn about admin_nopass risks
3. Add runtime warnings for insecure configurations

### Short-term (P1)
1. Fix Lua script ACL bypass
2. Implement UDS authentication options
3. Add security hardening guide to documentation
4. Create security configuration validator

### Medium-term (P2)
1. Audit and fix buffer overflows in C code
2. Implement command restriction improvements
3. Add comprehensive security tests
4. Security-focused code review process

### Long-term (P3)
1. Migrate legacy C code to safe C++
2. Implement security fuzzing in CI/CD
3. Regular penetration testing
4. Security audit by third party

---

## Conclusion

Dragonfly has a solid security foundation with ACL support, TLS, and command restrictions. However, several critical vulnerabilities exist that could allow privilege escalation and unauthorized access. The most severe issues involve ACL bypass via Unix sockets and potential Lua script privilege escalation.

All identified vulnerabilities have clear remediation paths. With proper fixes and secure configuration, Dragonfly can provide a highly secure data store suitable for production environments.

### Disclosure Timeline
- **Discovery:** 2026-01-18
- **Vendor Notification:** [To be determined]
- **Patch Development:** [To be determined]
- **Public Disclosure:** [To be determined - 90 days after vendor notification]

---

**Report prepared by:** Security Research Team  
**Contact:** [security contact information]  
**Classification:** CONFIDENTIAL - For Dragonfly Maintainers Only
