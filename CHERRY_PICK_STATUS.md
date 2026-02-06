# Cherry-pick Status for PR #6346 to v1.36-branch

## Completed Work

Successfully cherry-picked commit `3c6d5ed5` from PR #6346 ("Add checks for expiration inconsistency") to the v1.36-branch.

### Cherry-picked Commit
- **Commit SHA**: `882c5d7bdcd1aae1964bb89bfceb116b833ced64`
- **Location**: Local `v1.36-branch`  
- **Based on**: `8e81f2eb` (tag: i1.36.4, origin/v1.36-branch)
- **Author**: Borys <borys@dragonflydb.io>
- **Date**: Thu Jan 8 16:00:14 2026 +0200

### Changes Included

#### src/server/db_slice.cc (+27 lines)
- Added debug check in `FindMutableInternal()` to validate exp_it iterator after PreUpdateBlocking
- Added debug check in `AddOrFindInternal()` to validate exp_it iterator after PreUpdateBlocking
- Added check in `AddOrUpdateInternal()` to detect stale expiration entries

#### src/server/snapshot.cc (+10 lines, -2 lines)  
- Added include for "util/fibers/stacktrace.h"
- Modified `SerializeEntry()` to use LOG(DFATAL) instead of CHECK when expire entry is not found
- Added detailed error logging with stacktrace for debugging

## Issue: PR Base Branch

**PROBLEM**: This PR currently targets `main` branch, but needs to target `v1.36-branch`.

**WHY**: 
- PR #6346 was already merged to `main`
- The goal is to backport these changes to `v1.36-branch`
- A PR from a main-based branch to main would show no changes (they're already there)
- A PR needs to be from a v1.36-based branch to v1.36-branch to show the cherry-picked changes

## Required Manual Steps

Since the automation cannot:
1. Force-push to change branch history
2. Change PR base branch via API
3. Create new branches on remote

**Manual intervention required:**

### Option 1: Update This PR (Recommended)
1. Locally: `git fetch origin`
2. Locally: `git checkout -b backport-6346-to-v1.36 origin/v1.36-branch`
3. Locally: `git cherry-pick 3c6d5ed54ba02fe9633d3723aadcf5e0d3fc270e`
4. Locally: `git push origin backport-6346-to-v1.36:copilot/cherry-pick-changes-from-6346 --force`
5. In GitHub PR UI: Change base branch from `main` to `v1.36-branch`

### Option 2: Create New PR
1. Locally: `git push origin v1.36-branch:backport-pr6346-to-v1.36`  
   (This pushes the local v1.36-branch which has commit 882c5d7b)
2. Create new PR: `backport-pr6346-to-v1.36` → `v1.36-branch`
3. Close this PR

## Verification

To verify the cherry-pick was successful:

```bash
# Check the commit exists
git log v1.36-branch --oneline -5
# Should show: 882c5d7b Add checks for expiration inconsistency (#6346)

# View the changes
git show 882c5d7b --stat
# Should show:
#  src/server/db_slice.cc | 27 +++++++++++++++++++++++++++
#  src/server/snapshot.cc | 12 ++++++++++--
#  2 files changed, 37 insertions(+), 2 deletions(-)

# Compare with original
git diff 3c6d5ed5..882c5d7b
# Should show minimal/no differences (only context lines may differ)
```

## Summary

✅ Cherry-pick completed successfully  
✅ Changes verified to match PR #6346  
❌ PR configuration needs manual correction (base branch)

The technical work is complete. The PR just needs to be properly configured to target v1.36-branch instead of main.
