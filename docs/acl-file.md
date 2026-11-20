# ACL file operands

ACL files keep their existing literal, space-separated rules and newline-separated `USER` records.
There is no quoting or escape syntax. Tabs, quotes and backslashes remain literal operand bytes.
CRLF record endings are accepted.

`ACL SETUSER` rejects empty usernames and usernames, key patterns or channel patterns containing
space, LF, CR or NUL. The ACL file reader applies the same validation to parsed usernames and
patterns before replacing live users. A rejected file also prevents startup.
An ACL file accepted by older versions can now be refused at load or startup if a username or
pattern contains CR or NUL, so check ACL files before upgrading.

These restrictions prevent a single operand from becoming extra rules or records when saved,
which could widen permissions or produce a file that cannot be loaded. They follow Redis and
Valkey's rejection of delimiter bytes and NUL, while retaining Dragonfly's literal tab support.
Password arguments are not subject to these restrictions; saved passwords are hashes.

Previously saved files may already contain extra rules or records created by embedded separators.
When those bytes form otherwise valid ACL rules, the original operand boundaries cannot be
recovered or distinguished from intentional rules. Review such files before loading them; the
reader cannot detect all historical rule injection without changing the format.
