# Git Commit Protocol — CustomerLake Project

**Created:** 2026-10-15 (V98-REGRESSION-INVESTIGATE)
**Owner:** @devops (Release Engineering)

---

## Problem Statement

On 2026-10-15, the CustomerLake app regressed from v65.1.0 (28 pages, 7,570 lines) to v56.0.0 (20 pages, 6,048 lines) because:
1. @app-developer workspace edits (v57–v65.1) were never committed to `main`
2. @devops Run 69 committed them to branch `devops/run69-proactive-sweep` but did not merge to `main`
3. Checking out `main` reverted workspace files to the git-committed state, appearing to lose all work

## Mandatory Rules

### Rule 1: Commit Before Checkout
**NEVER** switch branches (`git checkout`, `git switch`) without first committing or stashing all workspace changes. Workspace files revert to the target branch's state on checkout.

### Rule 2: Merge Feature Branches Immediately
When @devops creates a feature branch and commits agent work, it MUST be merged to `main` in the same run. Do not leave work on unmerged branches.

### Rule 3: @devops Commit Cadence
@devops MUST commit and push all workspace changes to `main` (or merge feature branches to `main`) at least once per run. The commit-push should be the LAST action before marking tasks complete.

### Rule 4: Push After Merge
After merging a feature branch to `main`, ALWAYS push to origin immediately. Local-only merges are vulnerable to workspace resets.

### Rule 5: @app-developer Notification
After any commit containing app changes, @devops MUST note in the task board that the commit was made, with the commit hash and version number.

## Recovery Procedure

If workspace files appear to have reverted:
1. Check `git reflog` for recent branch operations
2. Look for unmerged feature branches: `git branch -a`
3. Compare file sizes at each branch tip to identify the correct state
4. Fast-forward merge if possible: `git merge --ff-only <branch>`
5. Push immediately after recovery

## Verification Checklist (per @devops run)

- [ ] `git status` shows clean working tree OR all changes committed
- [ ] `git log origin/main..main` shows 0 unpushed commits (or push pending)
- [ ] No unmerged feature branches with pending work
- [ ] APP_VERSION in app.py matches expected version
