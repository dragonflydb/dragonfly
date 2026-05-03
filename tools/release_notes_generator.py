#!/usr/bin/env python3
"""
Release Notes Generator for Dragonfly.

Takes a git commit range (e.g., v1.37.0..v1.38.0) and produces curated,
human-readable release notes in Markdown using Claude.

Each commit is analyzed independently with its message AND a truncated diff,
so the LLM is grounded in actual code changes rather than just commit subjects.
This dramatically reduces the risk of hallucinated or misclassified entries.

The per-commit analyses are then aggregated into themed release notes
(Commands / Performance / Replication / Search / Cluster / Protocol / Security
/ Cloud & Storage / Bug fixes).

Usage
-----
    export ANTHROPIC_API_KEY=...
    pip install -r tools/requirements.txt
    python tools/release_notes_generator.py v1.37.0..v1.38.0
    python tools/release_notes_generator.py v1.37.0..v1.38.0 --output-dir /tmp
    python tools/release_notes_generator.py HEAD~50..HEAD --max-parallel 4
"""

from __future__ import annotations

import argparse
import concurrent.futures
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional

try:
    import anthropic
    from pydantic import BaseModel, Field
except ImportError:  # allow --dry-run without the SDK installed
    anthropic = None  # type: ignore[assignment]
    BaseModel = object  # type: ignore[assignment,misc]

    def Field(*args, **kwargs):  # type: ignore[no-redef]
        return None


MODEL = "claude-sonnet-4-6"

# Bound the size of diff context per commit. Massive diffs (vendored deps,
# generated files, large refactors) would otherwise dominate the context window
# without adding signal. We keep enough to characterize the change.
MAX_DIFF_BYTES_PER_COMMIT = 40_000
MAX_DIFF_LINES_PER_FILE = 200
MAX_FILES_LISTED = 50

DEFAULT_PARALLEL = 8

# Built-in SDK retries with exponential backoff for 429 / 529 / 5xx / network errors.
# The SDK respects the `retry-after` header on rate limits, so most rate-limit
# bursts are absorbed transparently.
SDK_MAX_RETRIES = 8

# Outer retry rounds for commits that still fail after SDK-level retries are
# exhausted. Each round halves concurrency and waits longer before retrying.
DEFAULT_RETRY_ROUNDS = 3


Category = Literal[
    "commands",
    "performance",
    "replication",
    "search",
    "cluster",
    "protocol",
    "security",
    "cloud",
    "tiering",
    "bugfix",
    "internal",
    "ci",
    "docs",
]


class CommitAnalysis(BaseModel):
    category: Category = Field(
        description="Single best-fitting category. Use 'internal' for refactors, "
        "'ci' for build/test infrastructure, 'docs' for documentation-only changes."
    )
    user_facing: bool = Field(
        description="True only if a Dragonfly operator/user can observe this "
        "change in production. Tests, CI, refactors, and dep bumps are NOT user-facing. "
        "Bug fixes that change observable behavior ARE user-facing."
    )
    summary: str = Field(
        description="One short sentence describing the user-visible change. "
        "Empty string if not user-facing. Do not repeat the commit subject verbatim."
    )
    impact: str = Field(
        default="",
        description="Quantified impact if explicitly mentioned in commit (e.g., "
        "'2x faster', '50% less memory'). Empty string otherwise.",
    )
    theme: str = Field(
        default="",
        description="Optional cross-cutting PRODUCT theme: a short lowercase noun "
        "phrase (1-3 words) identifying a product-value topic that may be shared "
        "across multiple commits. Good examples (product value visible to users): "
        "'metrics', 'lua scripting', 'memory tracking', 'json', 'eviction', "
        "'cluster migration', 'pubsub', 'streams'. "
        "DO NOT use infrastructure themes -- leave empty instead for: 'fuzzing', "
        "'testing', 'tests', 'ci', 'build', 'docs', 'refactor', 'cleanup', "
        "'dependencies', 'tooling', 'lint', 'formatting'. Those belong to "
        "category='internal'/'ci'/'docs' and should not surface as release-notes "
        "themes -- readers care about what they get in the product, not how it "
        "was built. Do not just repeat the category name. Leave empty for one-off "
        "changes with no broader product theme.",
    )


ANALYZE_SYSTEM_PROMPT = """\
You are analyzing a single commit from the Dragonfly database project for inclusion
in user-facing release notes. Dragonfly is a Redis/Memcached-compatible in-memory
data store written in C++.

You will be given the commit subject, body, list of changed files, and a (possibly
truncated) unified diff. Use the diff as the source of truth -- commit messages
sometimes overstate or understate what actually changed.

Categories:
- commands     : new/changed Redis or Memcached commands, options, or behavior
- performance  : measurable performance/memory/latency improvements
- replication  : replication, journal, snapshot/saving, RDB
- search       : full-text/vector search module
- cluster      : cluster mode
- protocol     : RESP2/RESP3/Memcached protocol changes
- security     : ACL, TLS, authentication
- cloud        : AWS/GCP/S3 integration
- tiering      : disk-backed/tiered storage
- bugfix       : user-visible bug fix not fitting a category above
- internal     : refactor, cleanup, dep bump, code-style -- NOT user-facing
- ci           : CI/build/test infra -- NOT user-facing
- docs         : documentation-only -- NOT user-facing

Be conservative about user_facing. If the diff only touches tests/, .github/,
docs/, *.md, build files, or is a pure refactor with no behavior change,
set user_facing=false.

The 'theme' field is for cross-cutting topics that may span multiple commits
(metrics, lua scripting, memory tracking, json, fuzzing, eviction, etc.). It is
independent of 'category' -- a commit can have category='performance' AND
theme='memory tracking' if it's part of a broader memory-tracking effort.
Use the diff to identify shared subsystems. If nothing distinctive, leave empty.
"""


COMPOSE_SYSTEM_PROMPT = """\
You are a senior technical writer producing release notes for Dragonfly,
a Redis/Memcached-compatible in-memory data store. Your audience is engineers
and operators upgrading their Dragonfly deployment.

Output rules:

OPENING -- write a substantive 3-5 sentence headline paragraph (or a short
paragraph + 2-3 lead bullets) characterizing the release. Mention the dominant
themes, the most notable improvements, and any major user-facing additions.
This is the tl;dr -- a reader who only reads the opening should walk away
knowing what changed and whether the upgrade matters to them. Don't just list
section names; tell a short story.

SECTIONS -- Group changes under these BASE section headers (omit any section
that has no commits):
    ## Highlights
    ## Commands
    ## Performance
    ## Replication
    ## Search
    ## Cluster
    ## Protocol
    ## Security
    ## Cloud & Storage
    ## Bug fixes

THEMED SECTIONS -- The input may include a "Promoted themes" block listing
cross-cutting themes that span 3+ commits. For each promoted theme:
- Create an extra ## section with a clear Title-Case heading (you choose the
  exact wording; e.g. theme "metrics" -> "## Metrics & Observability",
  theme "fuzzing" -> "## Fuzzing & Testing", theme "json" -> "## JSON").
- Place themed sections in a logical position relative to the base sections
  (usually after the most-related base section, or near the top if it's a
  major theme).
- Each commit listed under a promoted theme MUST appear ONLY in its themed
  section, NOT also in its base-category section. Do not duplicate.

BULLETS -- Within each section: one short sentence + the PR/commit reference in
parentheses, e.g. "Improved snapshot loading throughput (#7070)". Quantified
impact in brackets when present, e.g. "Reduced HNSW memory by [50% less memory] (#6892)".

INVARIANTS:
- "Highlights" is 1-3 bullets summarizing the most impactful changes overall.
  Pull from any category or theme.
- Use ONLY the per-commit summaries provided. Do NOT invent facts, numbers, or
  PR titles. If no quantified impact is given, don't fabricate any.
- Skip non-user-facing commits entirely (internal/ci/docs).
- Output Markdown only. No preamble like "Here are the release notes:".
"""


@dataclass
class Commit:
    sha: str
    short_sha: str
    subject: str
    body: str
    author: str
    date: str
    files_changed: list[str]
    diff: str  # possibly truncated


@dataclass
class AnalyzedCommit:
    commit: Commit
    analysis: CommitAnalysis
    pr_number: Optional[str]


@dataclass
class AnalysisStats:
    total: int  # total commits in range
    succeeded_per_round: list[int]  # how many landed in each round
    failed: list[tuple[Commit, Exception]]  # final unrecoverable failures
    elapsed_s: float

    @property
    def total_succeeded(self) -> int:
        return sum(self.succeeded_per_round)

    @property
    def all_processed(self) -> bool:
        return not self.failed and self.total_succeeded == self.total


PR_RE = re.compile(r"\(#(\d+)\)\s*$")


def run_git(args: list[str], cwd: Path) -> str:
    return subprocess.run(
        ["git", "-C", str(cwd), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def parse_commits(commit_range: str, repo: Path) -> list[Commit]:
    # \x1f = unit separator between fields, \x1e = record separator between commits.
    # Safer than newlines because commit bodies contain newlines.
    fmt = "%H%x1f%h%x1f%s%x1f%an%x1f%aI%x1f%b%x1e"
    raw = run_git(
        ["log", f"--format={fmt}", "--no-merges", commit_range],
        repo,
    )
    commits: list[Commit] = []
    for entry in raw.split("\x1e"):
        entry = entry.strip("\n").strip()
        if not entry:
            continue
        parts = entry.split("\x1f")
        if len(parts) < 6:
            continue
        sha, short_sha, subject, author, date, body = parts[:6]

        files_changed = (
            run_git(["show", "--name-only", "--format=", sha], repo).strip().splitlines()
        )

        diff = _truncate_diff(run_git(["show", "--format=", "--no-color", sha], repo))

        commits.append(
            Commit(
                sha=sha,
                short_sha=short_sha,
                subject=subject,
                body=body.strip(),
                author=author,
                date=date,
                files_changed=files_changed,
                diff=diff,
            )
        )
    return commits


def _truncate_diff(diff: str) -> str:
    # Split on per-file headers. The first chunk before any "diff --git" is
    # usually empty.
    chunks = re.split(r"^diff --git ", diff, flags=re.MULTILINE)
    out: list[str] = []
    total = 0
    files_kept = 0
    files_total = sum(1 for c in chunks if c.strip())
    for i, chunk in enumerate(chunks):
        if not chunk.strip():
            continue
        body = ("diff --git " + chunk) if i > 0 else chunk
        lines = body.splitlines()
        if len(lines) > MAX_DIFF_LINES_PER_FILE:
            elided = len(lines) - MAX_DIFF_LINES_PER_FILE
            lines = lines[:MAX_DIFF_LINES_PER_FILE] + [f"... [{elided} more lines truncated] ..."]
        truncated = "\n".join(lines)
        if total + len(truncated) > MAX_DIFF_BYTES_PER_COMMIT:
            remaining = files_total - files_kept
            out.append(f"\n... [{remaining} more files truncated] ...")
            break
        out.append(truncated)
        total += len(truncated)
        files_kept += 1
    return "\n".join(out)


def extract_pr_number(subject: str) -> Optional[str]:
    m = PR_RE.search(subject)
    return m.group(1) if m else None


def analyze_commit(client: anthropic.Anthropic, commit: Commit) -> AnalyzedCommit:
    files_block = "\n".join(f"  {f}" for f in commit.files_changed[:MAX_FILES_LISTED])
    if len(commit.files_changed) > MAX_FILES_LISTED:
        files_block += f"\n  ... +{len(commit.files_changed) - MAX_FILES_LISTED} more"

    user_content = (
        f"Commit subject: {commit.subject}\n"
        f"Commit body:\n{commit.body or '(none)'}\n\n"
        f"Files changed:\n{files_block}\n\n"
        f"Diff (possibly truncated):\n{commit.diff}"
    )

    response = client.messages.parse(
        model=MODEL,
        max_tokens=600,
        system=ANALYZE_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_content}],
        output_format=CommitAnalysis,
    )
    return AnalyzedCommit(
        commit=commit,
        analysis=response.parsed_output,
        pr_number=extract_pr_number(commit.subject),
    )


def _short_error(e: Exception) -> str:
    """Compact one-liner for an exception. Keeps logs scannable when the SDK
    surfaces a multi-line JSON error body (typical for 429s)."""
    name = type(e).__name__
    if anthropic is not None and isinstance(e, anthropic.RateLimitError):
        return (
            f"{name} 429 (rate limit; will be retried -- consider --max-parallel 1 if persistent)"
        )
    msg = str(e).replace("\n", " ")
    if len(msg) > 180:
        msg = msg[:180] + "..."
    return f"{name}: {msg}"


def _analyze_round(
    client: "anthropic.Anthropic",
    commits: list[Commit],
    parallel: int,
    round_idx: int,
    is_last_round: bool,
) -> tuple[list[AnalyzedCommit], list[tuple[Commit, Exception]]]:
    """Run one parallel pass over `commits`.

    Status legend in the per-commit log:
      OK         -- succeeded on first round
      RECOVERED  -- succeeded on a retry round (round_idx > 0)
      FAIL       -- failed; will be retried in the next round (or permanent if last round)
    """
    succeeded: list[AnalyzedCommit] = []
    failed: list[tuple[Commit, Exception]] = []
    label = f"r{round_idx + 1}"
    ok_tag = "OK       " if round_idx == 0 else "RECOVERED"
    fail_tag = "FAIL (PERMANENT)" if is_last_round else "FAIL (will retry)"

    with concurrent.futures.ThreadPoolExecutor(max_workers=parallel) as pool:
        futures = {pool.submit(analyze_commit, client, c): c for c in commits}
        for i, fut in enumerate(concurrent.futures.as_completed(futures), 1):
            commit = futures[fut]
            try:
                succeeded.append(fut.result())
                print(
                    f"  {label} [{i}/{len(commits)}] {ok_tag} {commit.short_sha} {commit.subject}"
                )
            except Exception as e:
                failed.append((commit, e))
                print(
                    f"  {label} [{i}/{len(commits)}] {fail_tag} {commit.short_sha} "
                    f"{commit.subject}\n      -> {_short_error(e)}",
                    file=sys.stderr,
                )
    return succeeded, failed


def analyze_with_retries(
    client: "anthropic.Anthropic",
    commits: list[Commit],
    initial_parallel: int,
    retry_rounds: int,
) -> tuple[list[AnalyzedCommit], AnalysisStats]:
    """Run analysis with retry rounds for commits that fail SDK-level retries.

    Each retry round halves concurrency (to ease rate limits) and waits longer
    before kicking off, giving the API time to recover from sustained pressure.
    """
    t0 = time.time()
    all_succeeded: list[AnalyzedCommit] = []
    succeeded_per_round: list[int] = []
    remaining = list(commits)
    parallel = initial_parallel
    last_failures: list[tuple[Commit, Exception]] = []
    total_rounds = retry_rounds + 1
    for round_idx in range(total_rounds):
        is_last = round_idx == total_rounds - 1
        if round_idx == 0:
            print(
                f"\n=== Round 1/{total_rounds}: analyzing {len(remaining)} commits "
                f"(parallel={parallel}) ==="
            )
        else:
            wait_s = 30 * round_idx
            print(
                f"\n=== Round {round_idx + 1}/{total_rounds}: retrying "
                f"{len(remaining)} commit(s) that failed (parallel={parallel}, "
                f"waiting {wait_s}s first) ===",
                file=sys.stderr,
            )
            time.sleep(wait_s)

        succeeded, failed = _analyze_round(
            client, remaining, parallel, round_idx, is_last_round=is_last
        )
        all_succeeded.extend(succeeded)
        succeeded_per_round.append(len(succeeded))

        # Round-end summary so the user sees the result of each pass at a glance.
        if round_idx == 0:
            print(
                f"\n  Round 1 done: {len(succeeded)}/{len(remaining)} succeeded, "
                f"{len(failed)} failed" + (" (will retry)" if failed and not is_last else "")
            )
        else:
            print(
                f"\n  Round {round_idx + 1} done: {len(succeeded)} RECOVERED, "
                f"{len(failed)} still failing" + (" (will retry)" if failed and not is_last else "")
            )

        if not failed:
            last_failures = []
            break
        last_failures = failed
        remaining = [c for c, _ in failed]
        parallel = max(1, parallel // 2)

    stats = AnalysisStats(
        total=len(commits),
        succeeded_per_round=succeeded_per_round,
        failed=last_failures,
        elapsed_s=time.time() - t0,
    )
    return all_succeeded, stats


def print_analysis_stats(stats: AnalysisStats, analyzed: list[AnalyzedCommit]) -> None:
    """Print a clear summary of the analysis run.

    Always called -- both on success (before composition) and on failure
    (right before aborting). On failure the analyzed list may be partial.
    """
    print("\n" + "=" * 60)
    print("Analysis summary")
    print("=" * 60)
    print(f"  Total commits in range:    {stats.total}")
    print(
        f"  Successfully analyzed:     {stats.total_succeeded}"
        + ("  (ALL)" if stats.all_processed else "")
    )
    print(f"  Failed (unrecoverable):    {len(stats.failed)}")
    print(f"  Elapsed:                   {stats.elapsed_s:.1f}s")

    if len(stats.succeeded_per_round) > 1 or stats.failed:
        print("  Per-round breakdown:")
        for i, n in enumerate(stats.succeeded_per_round, 1):
            tag = "first try" if i == 1 else "RECOVERED via retry"
            print(f"    round {i}: {n:>4d} succeeded  ({tag})")

        recovered = sum(stats.succeeded_per_round[1:])
        if recovered > 0:
            print(f"\n  Retry rescued {recovered} commit(s) that failed initially.")

    if analyzed:
        by_category: dict[str, int] = {}
        user_facing = 0
        for a in analyzed:
            by_category[a.analysis.category] = by_category.get(a.analysis.category, 0) + 1
            if a.analysis.user_facing:
                user_facing += 1
        print(f"  User-facing commits:       {user_facing}/{len(analyzed)}")
        print("  By category:")
        for cat in sorted(by_category, key=lambda c: -by_category[c]):
            print(f"    {cat:14s} {by_category[cat]}")

    if stats.failed:
        print("\n  Unrecoverable failures:")
        for commit, err in stats.failed:
            print(f"    {commit.short_sha} {commit.subject}\n      -> {_short_error(err)}")
    print("=" * 60)


PROMOTE_THEME_MIN_SIZE = 3

# Predefined categories that already have their own section -- themes that
# duplicate them shouldn't be promoted.
_BASE_CATEGORY_NAMES = {
    "commands",
    "performance",
    "replication",
    "search",
    "cluster",
    "protocol",
    "security",
    "cloud",
    "tiering",
    "bugfix",
}

# Infrastructure themes that should NEVER be promoted to a release-notes
# section -- readers want to know what they get in the product, not how it
# was built. Even if the LLM tags a commit with one of these (or if a few
# user-facing bug fixes legitimately share an infra theme), we keep them
# inside the regular category sections instead.
_INFRA_THEME_BLOCKLIST = {
    # testing & fuzzing
    "fuzzing",
    "fuzz",
    "fuzz testing",
    "testing",
    "tests",
    "test",
    "test infrastructure",
    "test harness",
    "integration tests",
    "unit tests",
    "e2e tests",
    # ci & build
    "ci",
    "ci/cd",
    "build",
    "build system",
    "packaging",
    "release",
    # docs
    "docs",
    "documentation",
    "readme",
    # cleanup & tooling
    "refactor",
    "refactoring",
    "cleanup",
    "code cleanup",
    "code style",
    "style",
    "formatting",
    "lint",
    "linting",
    "dependencies",
    "deps",
    "dep bump",
    "dependency upgrade",
    "tooling",
    "dev tooling",
    "dev tools",
    "developer tooling",
    "logging",
    "log cleanup",  # internal logging -- not a user-visible feature
}


def detect_promoted_themes(
    user_facing: list[AnalyzedCommit],
    min_size: int = PROMOTE_THEME_MIN_SIZE,
) -> dict[str, list[AnalyzedCommit]]:
    """Group user-facing commits by `theme` and return clusters with >= min_size.

    Filtered out:
    - empty themes
    - themes that just duplicate a predefined category name (already have a section)
    - infrastructure themes (fuzzing/testing/ci/docs/refactor/...) -- those don't
      represent product value to readers of release notes
    """
    buckets: dict[str, list[AnalyzedCommit]] = {}
    for a in user_facing:
        theme = a.analysis.theme.strip().lower()
        if not theme:
            continue
        if theme in _BASE_CATEGORY_NAMES or theme in _INFRA_THEME_BLOCKLIST:
            continue
        buckets.setdefault(theme, []).append(a)
    return {t: cs for t, cs in buckets.items() if len(cs) >= min_size}


def compose_release_notes(
    client: anthropic.Anthropic,
    analyzed: list[AnalyzedCommit],
    commit_range: str,
) -> str:
    user_facing = [a for a in analyzed if a.analysis.user_facing]
    if not user_facing:
        return (
            f"# Release Notes -- {commit_range}\n\n"
            "_No user-facing changes detected in this range._\n"
        )

    promoted = detect_promoted_themes(user_facing)
    promoted_shas = {a.commit.sha for cs in promoted.values() for a in cs}

    if promoted:
        print(
            f"  Promoted themes (>={PROMOTE_THEME_MIN_SIZE} commits each): "
            + ", ".join(f"{t}({len(cs)})" for t, cs in promoted.items())
        )

    bullets: list[str] = []
    for a in user_facing:
        ref = f"#{a.pr_number}" if a.pr_number else a.commit.short_sha
        impact = f" [{a.analysis.impact}]" if a.analysis.impact else ""
        # Mark themed commits so the LLM places them only in the theme section.
        # Use the SAME normalized form (lowercased + stripped) that
        # detect_promoted_themes() bucketed by, so the per-commit tag and the
        # "Promoted themes" header agree byte-for-byte and the LLM doesn't see
        # them as distinct themes.
        if a.commit.sha in promoted_shas:
            theme_key = a.analysis.theme.strip().lower()
            theme_tag = f" {{theme: {theme_key}}}"
        else:
            theme_tag = ""
        bullets.append(f"- [{a.analysis.category}]{impact} {a.analysis.summary} ({ref}){theme_tag}")

    full_changelog = [f"- {a.commit.subject}" for a in analyzed]

    promoted_block = ""
    if promoted:
        lines = ["", "Promoted themes (3+ commits each -- render as standalone sections):"]
        for theme, cs in promoted.items():
            # `theme` is already the normalized key from detect_promoted_themes().
            refs = ", ".join(f"#{a.pr_number}" if a.pr_number else a.commit.short_sha for a in cs)
            lines.append(f"  - {theme} ({len(cs)} commits): {refs}")
        promoted_block = "\n".join(lines) + "\n"

    payload = (
        f"Range: {commit_range}\n"
        f"Total commits: {len(analyzed)}\n"
        f"User-facing commits: {len(user_facing)}\n"
        f"{promoted_block}\n"
        "Per-commit summaries (use these as the source of truth):\n" + "\n".join(bullets) + "\n\n"
        "Full commit list (for the appended What's Changed section):\n" + "\n".join(full_changelog)
    )

    response = client.messages.create(
        model=MODEL,
        max_tokens=8000,
        system=COMPOSE_SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": (
                    f"Produce release notes for Dragonfly {commit_range}.\n\n"
                    f"{payload}\n\n"
                    "After all themed sections, append a final section:\n"
                    "## What's Changed\n"
                    "with the full commit list verbatim (one bullet per commit subject)."
                ),
            }
        ],
    )
    # Concatenate ALL text blocks. The model usually returns a single block,
    # but multi-block responses are valid and silently dropping the tail would
    # truncate the release notes. Raise if there are zero text blocks so we
    # never silently write an empty file.
    text_parts = [b.text for b in response.content if b.type == "text"]
    if not text_parts:
        raise RuntimeError(
            f"composition response contained no text blocks "
            f"(stop_reason={response.stop_reason}, "
            f"block_types={[b.type for b in response.content]})"
        )
    return "\n".join(text_parts)


def sanitize_for_filename(s: str) -> str:
    # v1.38.1..v1.38.2 -> v1.38.1_to_v1.38.2 for a cleaner filename
    s = s.replace("...", "_to_").replace("..", "_to_")
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s).strip("_")


def _positive_int(s: str) -> int:
    """argparse type=: accept only integers >= 1."""
    try:
        v = int(s)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected an integer, got {s!r}")
    if v < 1:
        raise argparse.ArgumentTypeError(f"must be >= 1, got {v}")
    return v


def _non_negative_int(s: str) -> int:
    """argparse type=: accept only integers >= 0."""
    try:
        v = int(s)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected an integer, got {s!r}")
    if v < 0:
        raise argparse.ArgumentTypeError(f"must be >= 0, got {v}")
    return v


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "commit_range",
        help="Git commit range, e.g. v1.37.0..v1.38.0 or HEAD~50..HEAD",
    )
    parser.add_argument(
        "--repo",
        default=".",
        help="Path to the Dragonfly repo (default: current dir)",
    )
    parser.add_argument(
        "--output-dir",
        default=".",
        help="Where to write release_notes_<range>.md (default: current dir)",
    )
    parser.add_argument(
        "--max-parallel",
        type=_positive_int,
        default=DEFAULT_PARALLEL,
        help=f"Concurrent per-commit analyses, integer >= 1 " f"(default: {DEFAULT_PARALLEL})",
    )
    parser.add_argument(
        "--retry-rounds",
        type=_non_negative_int,
        default=DEFAULT_RETRY_ROUNDS,
        help=f"Outer retry rounds for commits that fail SDK retries, integer "
        f">= 0 (default: {DEFAULT_RETRY_ROUNDS}). Each round halves concurrency "
        f"and waits longer.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Collect commits and print stats, but skip Claude API calls",
    )
    args = parser.parse_args()

    repo = Path(args.repo).resolve()
    if not (repo / ".git").exists():
        print(f"error: {repo} is not a git repository", file=sys.stderr)
        return 1

    if not args.dry_run and anthropic is None:
        print(
            "error: the 'anthropic' package is not installed. Install with:\n"
            "       pip install -r tools/requirements.txt\n"
            "(or pass --dry-run to validate the commit range without API calls)",
            file=sys.stderr,
        )
        return 1

    if not args.dry_run and not os.environ.get("ANTHROPIC_API_KEY"):
        print(
            "error: ANTHROPIC_API_KEY env var is required (or pass --dry-run)",
            file=sys.stderr,
        )
        return 1

    print(f"Fetching commits in {args.commit_range} from {repo} ...")
    t0 = time.time()
    commits = parse_commits(args.commit_range, repo)
    if not commits:
        print("No commits found in range.", file=sys.stderr)
        return 1
    print(f"  {len(commits)} commits fetched in {time.time() - t0:.1f}s")

    if args.dry_run:
        for c in commits[:20]:
            print(f"  {c.short_sha} {c.subject}")
        if len(commits) > 20:
            print(f"  ... +{len(commits) - 20} more")
        return 0

    # max_retries lifts the SDK's built-in exponential-backoff retry on
    # 429 / 529 / 5xx / connection errors, so most rate-limit pressure is
    # absorbed transparently inside each call.
    client = anthropic.Anthropic(max_retries=SDK_MAX_RETRIES)

    analyzed, stats = analyze_with_retries(
        client,
        commits,
        initial_parallel=args.max_parallel,
        retry_rounds=args.retry_rounds,
    )

    # Stats are always shown -- both before composition (success path) and
    # before aborting (failure path). User wants explicit visibility either way.
    print_analysis_stats(stats, analyzed)

    if not stats.all_processed:
        print(
            f"\nERROR: only {stats.total_succeeded}/{stats.total} commits were "
            f"analyzed successfully after {args.retry_rounds + 1} round(s). "
            f"Aborting before composition -- refusing to write incomplete release notes.",
            file=sys.stderr,
        )
        print(
            "Hint: re-run with a higher --retry-rounds, lower --max-parallel, "
            "or wait for rate limits to recover.",
            file=sys.stderr,
        )
        return 2

    # Restore git-log order (newest first, like `git log` itself).
    order = {c.sha: i for i, c in enumerate(commits)}
    analyzed.sort(key=lambda a: order.get(a.commit.sha, 0))

    print("\nAll commits analyzed. Composing release notes ...")
    try:
        notes = compose_release_notes(client, analyzed, args.commit_range)
    except Exception as e:
        # Composition is one shot; the SDK's max_retries already covered
        # transient errors. If we land here, something durable broke.
        print(f"\nERROR: composition failed ({type(e).__name__}: {e})", file=sys.stderr)
        return 3

    out_dir = Path(args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    fname = f"release_notes_{sanitize_for_filename(args.commit_range)}.md"
    out_path = out_dir / fname
    out_path.write_text(notes, encoding="utf-8")
    print(f"\nWrote {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
