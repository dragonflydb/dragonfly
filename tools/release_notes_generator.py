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
    # Anthropic backend (requires ANTHROPIC_API_KEY):
    export ANTHROPIC_API_KEY=...
    pip install -r tools/requirements.txt
    python tools/release_notes_generator.py v1.37.0..v1.38.0
    python tools/release_notes_generator.py v1.37.0..v1.38.0 --output-dir /tmp
    python tools/release_notes_generator.py HEAD~50..HEAD --max-parallel 4

    # GitHub Copilot backend (uses Copilot CLI auth, no API key needed):
    pip install github-copilot-sdk
    python tools/release_notes_generator.py v1.37.0..v1.38.0 --backend copilot
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
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
except ImportError:
    anthropic = None  # type: ignore[assignment]

try:
    from pydantic import BaseModel, Field
except ImportError:  # allow --dry-run without pydantic installed
    BaseModel = object  # type: ignore[assignment,misc]

    def Field(*args, **kwargs):  # type: ignore[no-redef]
        return None


try:
    from copilot import CopilotClient as _CopilotClient
    from copilot.generated.session_events import (
        AssistantMessageData as _CopilotAssistantMessageData,
    )
    from copilot.generated.session_events import (
        AssistantStreamingDeltaData as _CopilotAssistantStreamingDeltaData,
    )
    from copilot.generated.session_events import AssistantUsageData as _CopilotAssistantUsageData
    from copilot.session import PermissionHandler as _CopilotPermissionHandler

    copilot_sdk_available = True
except ImportError:
    _CopilotClient = None  # type: ignore[assignment,misc]
    _CopilotAssistantMessageData = None  # type: ignore[assignment]
    _CopilotAssistantStreamingDeltaData = None  # type: ignore[assignment]
    _CopilotAssistantUsageData = None  # type: ignore[assignment]
    _CopilotPermissionHandler = None  # type: ignore[assignment]
    copilot_sdk_available = False


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Model identifiers differ between backends:
# - Anthropic SDK uses the raw model slug.
# - GitHub Copilot SDK uses its own model aliases.
ANTHROPIC_MODEL = "claude-sonnet-4-6"
ANTHROPIC_CACHE_CONTROL = {"type": "ephemeral"}

# Copilot model alias -- use the ID from `CopilotClient.list_models()`.
# Available models as of 2026-05 (billing multiplier vs GPT-4.1 baseline):
#   claude-sonnet-4.6   -- Claude Sonnet 4.6   200K ctx  1.0×  (recommended)
#   claude-opus-4.6     -- Claude Opus 4.6     200K ctx  3.0×  (highest quality)
#   gpt-5.3-codex       -- GPT-5.3-Codex       400K ctx  1.0×
COPILOT_MODEL = "claude-sonnet-4.6"

# Bound the size of diff context per commit. Massive diffs (vendored deps,
# generated files, large refactors) would otherwise dominate the context window
# without adding signal.
MAX_DIFF_BYTES_PER_COMMIT = 40_000
MAX_DIFF_LINES_PER_FILE = 400
MAX_FILES_LISTED = 50

DEFAULT_PARALLEL = 8

# Built-in SDK retries with exponential backoff for 429 / 529 / 5xx / network errors.
SDK_MAX_RETRIES = 8

# Outer retry rounds for commits that still fail after SDK-level retries are exhausted.
DEFAULT_RETRY_ROUNDS = 3

# Per-commit analysis token budgets.
# Anthropic's constrained structured-output decode converges faster, so 600 is enough.
# JSON-in-text backends (Copilot etc.) need more headroom for the JSON envelope.
ANALYZE_MAX_TOKENS_STRUCTURED = 600
ANALYZE_MAX_TOKENS_JSON = 1000

COMPOSE_MAX_TOKENS = 16_000
COMPOSE_HEARTBEAT_S = 15
COMPOSE_TOKEN_PROGRESS_INTERVAL = 250

# How many consecutive all-failed commits before aborting a round early.
FAIL_FAST_THRESHOLD = 5

CACHE_SUBDIR = ".release_notes_cache"
PROMOTE_THEME_MIN_SIZE = 3

logger = logging.getLogger(__name__)
PR_RE = re.compile(r"\(#(\d+)\)\s*$")


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

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
        description="Single best-fitting category. IMPORTANT: any commit that "
        "primarily fixes a crash, use-after-free, memory corruption, data loss, "
        "race condition, or other stability/correctness defect MUST be 'bugfix', "
        "regardless of the subsystem it touches. Domain categories (search, "
        "replication, cluster, tiering, …) are ONLY for new features, enhancements, "
        "or improvements — not for fixes. Use 'internal' for refactors, 'ci' for "
        "build/test infrastructure, 'docs' for documentation-only changes."
    )
    user_facing: bool = Field(
        description="True only if a Dragonfly operator/user can observe this "
        "change in production. Tests, CI, refactors, and dep bumps are NOT user-facing. "
        "Bug fixes that change observable behavior ARE user-facing."
    )
    summary: str = Field(
        description="Up-to 5 sentences describing the user-visible change. "
        "Single sentence if not user-facing. Do not repeat the commit subject verbatim."
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
    from_cache: bool = False


@dataclass
class AnalysisStats:
    total: int
    succeeded_per_round: list[int]
    failed: list[tuple[Commit, Exception]]
    elapsed_s: float

    @property
    def total_succeeded(self) -> int:
        return sum(self.succeeded_per_round)

    @property
    def all_processed(self) -> bool:
        return not self.failed and self.total_succeeded == self.total


@dataclass
class _ComposeStats:
    """Live streaming statistics exposed to the heartbeat loop via AnthropicBackend."""

    output_tokens: Optional[int] = None
    input_tokens: Optional[int] = None
    reasoning_tokens: Optional[int] = None
    text_chars: int = 0
    text_chunks: int = 0
    cache_read_input_tokens: Optional[int] = None
    cache_creation_input_tokens: Optional[int] = None
    cache_read_tokens: Optional[int] = None
    cache_write_tokens: Optional[int] = None
    response_bytes: int = 0


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

ANALYZE_SYSTEM_PROMPT = """\
You are analyzing a single commit from the Dragonfly database project for inclusion
in user-facing release notes. Dragonfly is a Redis/Memcached-compatible in-memory
data store written in C++.

You will be given the commit subject, body, list of changed files, and a (possibly
truncated) unified diff. Use the diff as the source of truth -- commit messages
sometimes overstate or understate what actually changed.

Categories:
- commands     : new/changed Redis or Memcached commands, options, or behavior (features only)
- performance  : measurable performance/memory/latency improvements (features only)
- replication  : replication, journal, snapshot/saving, RDB (features only)
- search       : full-text/vector search module (features only)
- cluster      : cluster mode (features only)
- protocol     : RESP2/RESP3/Memcached protocol changes (features only)
- security     : ACL, TLS, authentication (features only)
- cloud        : AWS/GCP/S3 integration (features only)
- tiering      : disk-backed/tiered storage (features only)
- bugfix       : ANY user-visible bug fix -- crashes, use-after-free, data corruption,
                 race conditions, memory safety issues, wrong results, starvation,
                 or other stability/correctness defects IN ANY SUBSYSTEM.
                 When in doubt between a domain category and bugfix, pick bugfix.
- internal     : refactor, cleanup, dep bump, code-style -- NOT user-facing
- ci           : CI/build/test infra -- NOT user-facing
- docs         : documentation-only -- NOT user-facing

CRITICAL RULE: If the primary purpose of the commit is to fix something that was
broken (crash, wrong behavior, memory error, data loss, race), use 'bugfix'.
Domain categories (commands, search, replication, …) are reserved for commits
whose primary purpose is to ADD or IMPROVE functionality.

Be conservative about user_facing. If the diff only touches tests/, .github/,
docs/, *.md, build files, or is a pure refactor with no behavior change,
set user_facing=false.

The 'theme' field is for cross-cutting topics that may span multiple commits
(metrics, lua scripting, memory tracking, json, fuzzing, eviction, etc.). It is
independent of 'category' -- a commit can have category='performance' AND
theme='memory tracking' if it's part of a broader memory-tracking effort.
Use the diff to identify shared subsystems. If nothing distinctive, leave empty.
"""

_ANALYZE_JSON_SUFFIX = """\
Respond with a JSON object only (no other text), matching this exact shape:
{
  "category": "<one of the category values listed above>",
  "user_facing": <true|false>,
  "summary": "<up to five sentences, or single sentence if not user-facing>",
  "impact": "<quantified impact if explicitly mentioned, else empty string>",
  "theme": "<short cross-cutting product theme noun phrase, or empty string>"
}"""

# Used by non-Anthropic backends that cannot do constrained structured-output decoding.
ANALYZE_SYSTEM_PROMPT_JSON = ANALYZE_SYSTEM_PROMPT + _ANALYZE_JSON_SUFFIX

COMPOSE_SYSTEM_PROMPT = """\
You are a senior technical writer producing release notes for Dragonfly,
a Redis/Memcached-compatible in-memory data store. Your audience is engineers
and operators upgrading their Dragonfly deployment.

Output rules:

TITLE -- The very first line must be a Markdown H1 title derived from the commit
range, if possible.

OPENING -- immediately after the title, write a substantive 3-5 sentence headline
paragraph (or a short paragraph + 2-3 lead bullets) characterizing the release.
Mention the dominant themes, the most notable improvements, and any major
user-facing additions. This is the tl;dr -- a reader who only reads the opening
should walk away knowing what changed and whether the upgrade matters to them.
Don't just list section names; tell a short story.

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
- NEVER move commits whose category is `bugfix` into themed sections. All
    `[bugfix]` commits MUST remain under `## Bug Fixes` even if they have a
    non-empty theme.

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


# ---------------------------------------------------------------------------
# JSON parsing utility (used by non-Anthropic backends)
# ---------------------------------------------------------------------------


def _parse_commit_analysis_json(text: str) -> CommitAnalysis:
    """Parse a CommitAnalysis from a JSON string produced by a text-only backend."""
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    start = text.find("{")
    if start == -1:
        raise ValueError(f"No JSON object found in LLM response: {text[:300]}")
    try:
        data, _ = json.JSONDecoder().raw_decode(text, start)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Failed to parse JSON from LLM response: {exc}\nText: {text[:300]}")
    return CommitAnalysis(
        category=data["category"],
        user_facing=bool(data["user_facing"]),
        summary=data.get("summary", ""),
        impact=data.get("impact", ""),
        theme=data.get("theme", ""),
    )


def _dedupe_highlights_section(notes: str) -> str:
    """Keep only the first '## Highlights' section.

    Copilot occasionally emits a second placeholder Highlights section. Drop later
    duplicates to keep the output stable.
    """
    lines = notes.splitlines()
    out: list[str] = []
    seen_highlights = False
    skip_duplicate = False

    for line in lines:
        if line == "## Highlights":
            if seen_highlights:
                skip_duplicate = True
                continue
            seen_highlights = True
        elif skip_duplicate and line.startswith("## "):
            skip_duplicate = False

        if not skip_duplicate:
            out.append(line)

    return "\n".join(out).rstrip() + "\n"


def _strip_leading_preamble(notes: str) -> str:
    """Drop any non-Markdown preamble that appears before the first H1 title.

    Some backends occasionally prepend confirmations like "Here are the release
    notes:" despite the prompt asking for Markdown-only output. If an H1 exists,
    keep the document starting from that title.
    """
    match = re.search(r"(?m)^# ", notes)
    if not match:
        return notes
    return notes[match.start() :].lstrip()


# ---------------------------------------------------------------------------
# LLM backends
# ---------------------------------------------------------------------------


class LLMBackend:
    """Minimal async LLM interface. Subclasses implement analyze_commit and call."""

    name: str = "base"
    model: str
    analyze_system_prompt: str
    analyze_max_tokens: int

    async def analyze_commit(self, user: str) -> CommitAnalysis:
        raise NotImplementedError

    async def call(self, system: str, user: str, max_tokens: int) -> str:
        raise NotImplementedError

    async def compose(self, system: str, user: str, max_tokens: int) -> str:
        """Compose release notes. Backends may override for streaming / progress reporting."""
        return await self.call(system, user, max_tokens)

    def compose_progress_status(self) -> str:
        """Return a comma-prefixed status string for the heartbeat line, or ''."""
        return ""

    def post_process_notes(self, notes: str) -> str:
        """Apply any backend-specific fixes to the composed release notes."""
        return notes

    def notes_filename(self, safe_range: str) -> str:
        raise NotImplementedError


class AnthropicBackend(LLMBackend):
    """Thin async wrapper around the synchronous Anthropic SDK."""

    name = "anthropic"
    model = ANTHROPIC_MODEL
    analyze_system_prompt = ANALYZE_SYSTEM_PROMPT
    analyze_max_tokens = ANALYZE_MAX_TOKENS_STRUCTURED

    def __init__(self, client: "anthropic.Anthropic") -> None:
        self._client = client
        self._compose_stats = _ComposeStats()

    @staticmethod
    def _opt_int(obj, attr: str) -> Optional[int]:
        val = getattr(obj, attr, None)
        return int(val) if val is not None else None

    @staticmethod
    def _extract_text_blocks(response) -> str:
        parts = [b.text for b in response.content if b.type == "text"]
        if not parts:
            raise RuntimeError(
                f"response contained no text blocks "
                f"(stop_reason={response.stop_reason}, "
                f"block_types={[b.type for b in response.content]})"
            )
        return "\n".join(parts)

    async def analyze_commit(self, user: str) -> CommitAnalysis:
        response = await asyncio.to_thread(
            self._client.messages.parse,
            model=self.model,
            max_tokens=self.analyze_max_tokens,
            system=self.analyze_system_prompt,
            messages=[{"role": "user", "content": user}],
            output_format=CommitAnalysis,
        )
        return response.parsed_output

    async def call(self, system: str, user: str, max_tokens: int) -> str:
        response = await asyncio.to_thread(
            self._client.messages.create,
            model=self.model,
            max_tokens=max_tokens,
            cache_control=ANTHROPIC_CACHE_CONTROL,
            system=system,
            messages=[{"role": "user", "content": user}],
        )
        logger.info(
            "anthropic_cache context=call cache_read=%s cache_creation=%s input=%s output=%s",
            self._opt_int(response.usage, "cache_read_input_tokens"),
            self._opt_int(response.usage, "cache_creation_input_tokens"),
            self._opt_int(response.usage, "input_tokens"),
            self._opt_int(response.usage, "output_tokens"),
        )
        return self._extract_text_blocks(response)

    def _sync_stream(self, system: str, user: str, max_tokens: int) -> str:
        """Synchronous streaming compose; updates self._compose_stats as tokens arrive."""
        stats = _ComposeStats()
        self._compose_stats = stats  # exposed to compose_progress_status (read by heartbeat)
        last_progress_logged = -1

        with self._client.messages.stream(
            model=self.model,
            max_tokens=max_tokens,
            cache_control=ANTHROPIC_CACHE_CONTROL,
            system=system,
            messages=[{"role": "user", "content": user}],
        ) as stream:
            for event in stream:
                snapshot_usage = getattr(stream.current_message_snapshot, "usage", None)
                if snapshot_usage is not None:
                    if (v := self._opt_int(snapshot_usage, "output_tokens")) is not None:
                        stats.output_tokens = v
                    if (v := self._opt_int(snapshot_usage, "cache_read_input_tokens")) is not None:
                        stats.cache_read_input_tokens = v
                    if (
                        v := self._opt_int(snapshot_usage, "cache_creation_input_tokens")
                    ) is not None:
                        stats.cache_creation_input_tokens = v

                if (
                    event.type == "content_block_delta"
                    and getattr(event.delta, "type", None) == "text_delta"
                ):
                    stats.text_chars += len(getattr(event.delta, "text", ""))
                    stats.text_chunks += 1

                if event.type == "message_delta":
                    out_tok = self._opt_int(getattr(event, "usage", None), "output_tokens")
                    if (
                        out_tok is not None
                        and out_tok >= last_progress_logged + COMPOSE_TOKEN_PROGRESS_INTERVAL
                    ):
                        logger.info(
                            "compose_progress backend=%s output_tokens=%s/%s",
                            self.name,
                            out_tok,
                            max_tokens,
                        )
                        last_progress_logged = out_tok

            final = stream.get_final_message()

        if (v := self._opt_int(final.usage, "output_tokens")) is not None:
            stats.output_tokens = v
        stats.cache_read_input_tokens = self._opt_int(final.usage, "cache_read_input_tokens")
        stats.cache_creation_input_tokens = self._opt_int(
            final.usage, "cache_creation_input_tokens"
        )
        logger.info(
            "anthropic_cache context=compose cache_read=%s cache_creation=%s input=%s output=%s",
            stats.cache_read_input_tokens,
            stats.cache_creation_input_tokens,
            self._opt_int(final.usage, "input_tokens"),
            stats.output_tokens,
        )
        return self._extract_text_blocks(final)

    async def compose(self, system: str, user: str, max_tokens: int) -> str:
        return await asyncio.to_thread(self._sync_stream, system, user, max_tokens)

    def compose_progress_status(self) -> str:
        s = self._compose_stats
        parts = []
        if s.output_tokens is not None:
            parts.append(f"output_tokens={s.output_tokens}")
        if s.text_chars > 0:
            parts.append(f"text_chars={s.text_chars}")
            parts.append(f"text_chunks={s.text_chunks}")
        if s.cache_read_input_tokens not in (None, 0):
            parts.append(f"cache_read_input_tokens={s.cache_read_input_tokens}")
        if s.cache_creation_input_tokens not in (None, 0):
            parts.append(f"cache_creation_input_tokens={s.cache_creation_input_tokens}")
        return (", " + ", ".join(parts)) if parts else ""

    def notes_filename(self, safe_range: str) -> str:
        return f"release_notes_{safe_range}.md"


class CopilotBackend(LLMBackend):
    """Backend that routes requests through the GitHub Copilot SDK.

    Authentication is handled by the GitHub Copilot CLI -- no API key required.
    """

    name = "copilot"
    model = COPILOT_MODEL
    analyze_system_prompt = ANALYZE_SYSTEM_PROMPT_JSON
    analyze_max_tokens = ANALYZE_MAX_TOKENS_JSON

    def __init__(self) -> None:
        self._compose_stats = _ComposeStats()

    @staticmethod
    def _opt_int(value: object) -> Optional[int]:
        return int(value) if value is not None else None

    async def _call_with_stats(
        self,
        system: str,
        user: str,
        max_tokens: int,
        *,
        context: str,
        track_progress: bool,
    ) -> str:
        # Budget ~120 ms per output token, with a 120 s floor for small calls.
        timeout_s = max(120.0, max_tokens * 0.12)
        usage_event = None
        stats = _ComposeStats()
        if track_progress:
            self._compose_stats = stats

        def on_event(event) -> None:
            nonlocal usage_event
            data = event.data
            if _CopilotAssistantStreamingDeltaData is not None and isinstance(
                data, _CopilotAssistantStreamingDeltaData
            ):
                if track_progress:
                    stats.response_bytes = int(data.total_response_size_bytes)
                return

            if _CopilotAssistantUsageData is not None and isinstance(
                data, _CopilotAssistantUsageData
            ):
                usage_event = data
                if track_progress:
                    stats.input_tokens = self._opt_int(data.input_tokens)
                    stats.output_tokens = self._opt_int(data.output_tokens)
                    stats.reasoning_tokens = self._opt_int(data.reasoning_tokens)
                    stats.cache_read_tokens = self._opt_int(data.cache_read_tokens)
                    stats.cache_write_tokens = self._opt_int(data.cache_write_tokens)
                return

            if (
                track_progress
                and _CopilotAssistantMessageData is not None
                and isinstance(data, _CopilotAssistantMessageData)
            ):
                if data.content:
                    stats.text_chars = len(data.content)
                if data.output_tokens is not None:
                    stats.output_tokens = int(data.output_tokens)

        async with _CopilotClient() as client:
            async with await client.create_session(
                on_permission_request=_CopilotPermissionHandler.approve_all,
                model=self.model,
                system_message={"content": system},
                infinite_sessions={"enabled": False},
                on_event=on_event,
                streaming=track_progress,
            ) as session:
                response = await session.send_and_wait(user, timeout=timeout_s)

        if response is None or response.data is None:
            raise RuntimeError("Empty response from Copilot backend")
        text = response.data.content
        if not text:
            raise RuntimeError("Copilot backend returned empty text content")

        if track_progress:
            stats.text_chars = len(text)
            stats.text_chunks = max(stats.text_chunks, 1 if text else 0)

        if usage_event is not None:
            logger.info(
                "copilot_usage context=%s model=%s input_tokens=%s output_tokens=%s "
                "reasoning_tokens=%s cache_read_tokens=%s cache_write_tokens=%s duration=%s",
                context,
                usage_event.model,
                self._opt_int(usage_event.input_tokens),
                self._opt_int(usage_event.output_tokens),
                self._opt_int(usage_event.reasoning_tokens),
                self._opt_int(usage_event.cache_read_tokens),
                self._opt_int(usage_event.cache_write_tokens),
                usage_event.duration,
            )
        else:
            logger.info("copilot_usage context=%s unavailable=true", context)

        return text

    async def analyze_commit(self, user: str) -> CommitAnalysis:
        text = await self._call_with_stats(
            self.analyze_system_prompt,
            user,
            self.analyze_max_tokens,
            context="analyze",
            track_progress=False,
        )
        return _parse_commit_analysis_json(text)

    async def call(self, system: str, user: str, max_tokens: int) -> str:
        return await self._call_with_stats(
            system,
            user,
            max_tokens,
            context="call",
            track_progress=False,
        )

    async def compose(self, system: str, user: str, max_tokens: int) -> str:
        return await self._call_with_stats(
            system,
            user,
            max_tokens,
            context="compose",
            track_progress=True,
        )

    def compose_progress_status(self) -> str:
        s = self._compose_stats
        parts = []
        if s.output_tokens is not None:
            parts.append(f"output_tokens={s.output_tokens}")
        if s.input_tokens is not None:
            parts.append(f"input_tokens={s.input_tokens}")
        if s.reasoning_tokens not in (None, 0):
            parts.append(f"reasoning_tokens={s.reasoning_tokens}")
        if s.response_bytes > 0:
            parts.append(f"response_bytes={s.response_bytes}")
        if s.text_chars > 0:
            parts.append(f"text_chars={s.text_chars}")
        if s.cache_read_tokens not in (None, 0):
            parts.append(f"cache_read_tokens={s.cache_read_tokens}")
        if s.cache_write_tokens not in (None, 0):
            parts.append(f"cache_write_tokens={s.cache_write_tokens}")
        return (", " + ", ".join(parts)) if parts else ""

    def post_process_notes(self, notes: str) -> str:
        return _dedupe_highlights_section(_strip_leading_preamble(notes))

    def notes_filename(self, safe_range: str) -> str:
        return f"release_notes_copilot_{safe_range}.md"


# ---------------------------------------------------------------------------
# Disk cache for per-commit analysis results
# ---------------------------------------------------------------------------


def _make_cache_key(backend: LLMBackend, sha: str) -> str:
    """Stable SHA-256 fingerprint covering all inputs that affect the result."""
    h = hashlib.sha256()
    for part in (
        backend.name,
        backend.model,
        str(backend.analyze_max_tokens),
        backend.analyze_system_prompt,
        sha,
    ):
        encoded = part.encode()
        h.update(len(encoded).to_bytes(4, "little"))
        h.update(encoded)
    return h.hexdigest()


def _cache_load(cache_dir: Path, key: str) -> Optional[CommitAnalysis]:
    path = cache_dir / f"{key}.json"
    if not path.exists():
        return None
    try:
        return CommitAnalysis(**json.loads(path.read_text(encoding="utf-8")))
    except Exception:
        return None  # treat corrupt entries as a miss


def _cache_save(cache_dir: Path, key: str, analysis: CommitAnalysis) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / f"{key}.json").write_text(
        json.dumps(analysis.model_dump(), indent=2),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Git / commit parsing
# ---------------------------------------------------------------------------


def run_git(args: list[str], cwd: Path) -> str:
    return subprocess.run(
        ["git", "-C", str(cwd), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout


def _truncate_diff(diff: str) -> str:
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


def parse_commits(commit_range: str, repo: Path) -> list[Commit]:
    # \x1f = unit separator between fields, \x1e = record separator between commits.
    fmt = "%H%x1f%h%x1f%s%x1f%an%x1f%aI%x1f%b%x1e"
    raw = run_git(["log", f"--format={fmt}", "--no-merges", commit_range], repo)
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


def extract_pr_number(subject: str) -> Optional[str]:
    m = PR_RE.search(subject)
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# Error helpers
# ---------------------------------------------------------------------------


def _short_error(e: Exception) -> str:
    """Compact one-liner for an exception. Keeps logs scannable for multi-line SDK errors."""
    name = type(e).__name__
    if anthropic is not None and isinstance(e, anthropic.RateLimitError):
        return (
            f"{name} 429 (rate limit; will be retried -- consider --max-parallel 1 if persistent)"
        )
    msg = str(e).replace("\n", " ")
    if len(msg) > 180:
        msg = msg[:180] + "..."
    return f"{name}: {msg}"


def _is_fatal_error(e: Exception) -> bool:
    """True for errors that won't resolve with retries (auth / model-not-available)."""
    msg = str(e)
    if "not available" in msg or "JSON-RPC Error" in msg:
        return True
    if "authentication" in msg.lower() or "unauthorized" in msg.lower():
        return True
    return False


# ---------------------------------------------------------------------------
# Per-commit analysis
# ---------------------------------------------------------------------------


def _build_commit_user_content(commit: Commit) -> str:
    files_block = "\n".join(f"  {f}" for f in commit.files_changed[:MAX_FILES_LISTED])
    if len(commit.files_changed) > MAX_FILES_LISTED:
        files_block += f"\n  ... +{len(commit.files_changed) - MAX_FILES_LISTED} more"
    return (
        f"Commit subject: {commit.subject}\n"
        f"Commit body:\n{commit.body or '(none)'}\n\n"
        f"Files changed:\n{files_block}\n\n"
        f"Diff (possibly truncated):\n{commit.diff}"
    )


async def _analyze_commit_async(
    backend: LLMBackend, commit: Commit, cache_dir: Optional[Path] = None
) -> AnalyzedCommit:
    """Analyze a single commit via backend, with optional disk caching.

    The cache key covers backend name, model, analyze_max_tokens, system prompt,
    and commit SHA -- so any change to prompts or model busts the cache.
    """
    cache_key = _make_cache_key(backend, commit.sha) if cache_dir is not None else None

    if cache_key is not None:
        cached = _cache_load(cache_dir, cache_key)
        if cached is not None:
            logger.info(
                "commit_analysis cache_hit backend=%s sha=%s subject=%s analysis=%s",
                backend.name,
                commit.short_sha,
                json.dumps(commit.subject),
                cached.model_dump_json(),
            )
            return AnalyzedCommit(
                commit=commit,
                analysis=cached,
                pr_number=extract_pr_number(commit.subject),
                from_cache=True,
            )

    analysis = await backend.analyze_commit(_build_commit_user_content(commit))

    logger.info(
        "commit_analysis backend=%s sha=%s subject=%s analysis=%s",
        backend.name,
        commit.short_sha,
        json.dumps(commit.subject),
        analysis.model_dump_json(),
    )

    if cache_key is not None:
        _cache_save(cache_dir, cache_key, analysis)

    return AnalyzedCommit(
        commit=commit,
        analysis=analysis,
        pr_number=extract_pr_number(commit.subject),
        from_cache=False,
    )


async def _analyze_round(
    backend: LLMBackend,
    commits: list[Commit],
    parallel: int,
    round_idx: int,
    is_last_round: bool,
    cache_dir: Optional[Path] = None,
) -> tuple[list[AnalyzedCommit], list[tuple[Commit, Exception]]]:
    """Run one async parallel pass over commits.

    Status legend in the per-commit log:
      OK         -- succeeded on first round
      RECOVERED  -- succeeded on a retry round (round_idx > 0)
      FAIL       -- failed; will be retried next round (or permanent if last round)
    """
    succeeded: list[AnalyzedCommit] = []
    failed: list[tuple[Commit, Exception]] = []
    label = f"r{round_idx + 1}"
    ok_tag = "OK       " if round_idx == 0 else "RECOVERED"
    fail_tag = "FAIL (PERMANENT)" if is_last_round else "FAIL (will retry)"
    completed = 0
    abort_event = asyncio.Event()
    lock = asyncio.Lock()
    semaphore = asyncio.Semaphore(parallel)

    async def analyze_one(commit: Commit) -> None:
        nonlocal completed
        if abort_event.is_set():
            return  # fast path: skip without consuming the semaphore slot
        async with semaphore:
            if abort_event.is_set():
                return
            try:
                result = await _analyze_commit_async(backend, commit, cache_dir)
                async with lock:
                    completed += 1
                    succeeded.append(result)
                    print(
                        f"  {label} [{completed}/{len(commits)}] {ok_tag} "
                        f"{commit.short_sha} {commit.subject}"
                    )
            except Exception as e:
                async with lock:
                    completed += 1
                    failed.append((commit, e))
                    print(
                        f"  {label} [{completed}/{len(commits)}] {fail_tag} "
                        f"{commit.short_sha} {commit.subject}\n      -> {_short_error(e)}",
                        file=sys.stderr,
                    )
                    if (
                        not abort_event.is_set()
                        and len(succeeded) == 0
                        and len(failed) >= FAIL_FAST_THRESHOLD
                        and all(_is_fatal_error(err) for _, err in failed)
                    ):
                        print(
                            f"\nFATAL: first {len(failed)} commits all failed with a "
                            f"non-retriable error -- aborting round early.\n"
                            f"  Last error: {_short_error(e)}",
                            file=sys.stderr,
                        )
                        abort_event.set()

    await asyncio.gather(*[analyze_one(c) for c in commits])

    # Any commit not in succeeded or failed was silently skipped due to abort.
    # Add them to failed so the caller can retry them in the next round.
    if abort_event.is_set():
        processed = {a.commit.sha for a in succeeded} | {c.sha for c, _ in failed}
        for commit in commits:
            if commit.sha not in processed:
                failed.append((commit, RuntimeError("skipped due to fatal backend error")))

    return succeeded, failed


async def analyze_with_retries(
    backend: LLMBackend,
    commits: list[Commit],
    initial_parallel: int,
    retry_rounds: int,
    cache_dir: Optional[Path] = None,
) -> tuple[list[AnalyzedCommit], AnalysisStats]:
    """Run analysis with retry rounds for commits that fail backend-level retries.

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
                f"(parallel={parallel}, backend={backend.name}) ==="
            )
        else:
            wait_s = 30 * round_idx
            print(
                f"\n=== Round {round_idx + 1}/{total_rounds}: retrying "
                f"{len(remaining)} commit(s) that failed (parallel={parallel}, "
                f"waiting {wait_s}s first) ===",
                file=sys.stderr,
            )
            await asyncio.sleep(wait_s)

        succeeded, failed = await _analyze_round(
            backend, remaining, parallel, round_idx, is_last_round=is_last, cache_dir=cache_dir
        )
        all_succeeded.extend(succeeded)
        succeeded_per_round.append(len(succeeded))

        if round_idx == 0:
            retry_note = " (will retry)" if failed and not is_last else ""
            print(
                f"\n  Round 1 done: {len(succeeded)}/{len(remaining)} succeeded, "
                f"{len(failed)} failed{retry_note}"
            )
        else:
            retry_note = " (will retry)" if failed and not is_last else ""
            print(
                f"\n  Round {round_idx + 1} done: {len(succeeded)} RECOVERED, "
                f"{len(failed)} still failing{retry_note}"
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
    """Print a clear summary of the analysis run."""
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
        user_facing = cache_hits = 0
        for a in analyzed:
            by_category[a.analysis.category] = by_category.get(a.analysis.category, 0) + 1
            user_facing += a.analysis.user_facing
            cache_hits += a.from_cache
        print(f"  User-facing commits:       {user_facing}/{len(analyzed)}")
        print(f"  Cache hits:                {cache_hits}/{len(analyzed)}")
        print(f"  Fresh backend calls:       {len(analyzed) - cache_hits}/{len(analyzed)}")
        print("  By category:")
        for cat in sorted(by_category, key=lambda c: -by_category[c]):
            print(f"    {cat:14s} {by_category[cat]}")

    if stats.failed:
        print("\n  Unrecoverable failures:")
        for commit, err in stats.failed:
            print(f"    {commit.short_sha} {commit.subject}\n      -> {_short_error(err)}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Theme detection & release notes composition
# ---------------------------------------------------------------------------

# Predefined categories that already have their own section.
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

# Infrastructure themes that should never be promoted to a release-notes section.
_INFRA_THEME_BLOCKLIST = {
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
    "ci",
    "ci/cd",
    "build",
    "build system",
    "packaging",
    "release",
    "docs",
    "documentation",
    "readme",
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
    "log cleanup",
}


def detect_promoted_themes(
    user_facing: list[AnalyzedCommit],
    min_size: int = PROMOTE_THEME_MIN_SIZE,
) -> dict[str, list[AnalyzedCommit]]:
    """Group user-facing non-bugfix commits by theme; return clusters with >= min_size."""
    buckets: dict[str, list[AnalyzedCommit]] = {}
    for a in user_facing:
        if a.analysis.category == "bugfix":
            continue  # bugfix commits must stay in Bug Fixes, never in theme sections
        theme = a.analysis.theme.strip().lower()
        if not theme or theme in _BASE_CATEGORY_NAMES or theme in _INFRA_THEME_BLOCKLIST:
            continue
        buckets.setdefault(theme, []).append(a)
    return {t: cs for t, cs in buckets.items() if len(cs) >= min_size}


def _build_composition_payload(
    commit_range: str,
    analyzed: list[AnalyzedCommit],
    user_facing: list[AnalyzedCommit],
    promoted: dict[str, list[AnalyzedCommit]],
) -> str:
    promoted_shas = {a.commit.sha for cs in promoted.values() for a in cs}

    bullets: list[str] = []
    for a in user_facing:
        ref = f"#{a.pr_number}" if a.pr_number else a.commit.short_sha
        impact = f" [{a.analysis.impact}]" if a.analysis.impact else ""
        theme_tag = (
            f" {{theme: {a.analysis.theme.strip().lower()}}}"
            if a.commit.sha in promoted_shas
            else ""
        )
        bullets.append(f"- [{a.analysis.category}]{impact} {a.analysis.summary} ({ref}){theme_tag}")

    full_changelog = [f"- {a.commit.subject}" for a in analyzed]

    promoted_block = ""
    if promoted:
        lines = ["", "Promoted themes (3+ commits each -- render as standalone sections):"]
        for theme, cs in promoted.items():
            refs = ", ".join(f"#{a.pr_number}" if a.pr_number else a.commit.short_sha for a in cs)
            lines.append(f"  - {theme} ({len(cs)} commits): {refs}")
        promoted_block = "\n".join(lines) + "\n"

    return (
        f"Range: {commit_range}\n"
        f"Total commits: {len(analyzed)}\n"
        f"User-facing commits: {len(user_facing)}\n"
        f"{promoted_block}\n"
        "Per-commit summaries (use these as the source of truth):\n"
        + "\n".join(bullets)
        + "\n\nFull commit list (for the appended What's Changed section):\n"
        + "\n".join(full_changelog)
    )


async def _heartbeat_compose(
    backend: LLMBackend,
    compose_task: "asyncio.Task[str]",
    user_facing_count: int,
) -> str:
    """Drive the compose task and print periodic progress lines until it finishes."""
    started_at = time.monotonic()
    while not compose_task.done():
        elapsed_s = int(time.monotonic() - started_at)
        status = backend.compose_progress_status()
        print(
            f"  Compose in progress: backend={backend.name}, elapsed={elapsed_s}s, "
            f"user-facing commits={user_facing_count}{status} ..."
        )
        try:
            await asyncio.wait_for(asyncio.shield(compose_task), timeout=COMPOSE_HEARTBEAT_S)
        except TimeoutError:
            continue
    return await compose_task


async def compose_release_notes(
    backend: LLMBackend,
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
    if promoted:
        print(
            f"  Promoted themes (>={PROMOTE_THEME_MIN_SIZE} commits each): "
            + ", ".join(f"{t}({len(cs)})" for t, cs in promoted.items())
        )

    payload = _build_composition_payload(commit_range, analyzed, user_facing, promoted)
    user_message = (
        f"Produce release notes for Dragonfly {commit_range}.\n\n"
        f"{payload}\n\n"
        "After all themed sections, append a final section:\n"
        "## What's Changed\n"
        "with the full commit list verbatim (one bullet per commit subject)."
    )

    compose_task = asyncio.create_task(
        backend.compose(COMPOSE_SYSTEM_PROMPT, user_message, max_tokens=COMPOSE_MAX_TOKENS)
    )
    notes = await _heartbeat_compose(backend, compose_task, len(user_facing))
    return backend.post_process_notes(notes)


# ---------------------------------------------------------------------------
# CLI helpers
# ---------------------------------------------------------------------------


def sanitize_for_filename(s: str) -> str:
    s = s.replace("...", "_to_").replace("..", "_to_")
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s).strip("_")


def _positive_int(s: str) -> int:
    try:
        v = int(s)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected an integer, got {s!r}")
    if v < 1:
        raise argparse.ArgumentTypeError(f"must be >= 1, got {v}")
    return v


def _non_negative_int(s: str) -> int:
    try:
        v = int(s)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected an integer, got {s!r}")
    if v < 0:
        raise argparse.ArgumentTypeError(f"must be >= 0, got {v}")
    return v


def _build_arg_parser() -> argparse.ArgumentParser:
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
        help=f"Concurrent per-commit analyses, integer >= 1 (default: {DEFAULT_PARALLEL})",
    )
    parser.add_argument(
        "--retry-rounds",
        type=_non_negative_int,
        default=DEFAULT_RETRY_ROUNDS,
        help=(
            f"Outer retry rounds for commits that fail SDK retries, integer "
            f">= 0 (default: {DEFAULT_RETRY_ROUNDS}). Each round halves concurrency "
            f"and waits longer."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Collect commits and print stats, but skip API calls",
    )
    parser.add_argument(
        "--backend",
        choices=["anthropic", "copilot"],
        default="anthropic",
        help=(
            "LLM backend to use: 'anthropic' (default, requires ANTHROPIC_API_KEY) "
            "or 'copilot' (uses GitHub Copilot CLI auth, no API key needed -- "
            "requires the github-copilot-sdk package and an authenticated Copilot CLI)."
        ),
    )
    parser.add_argument(
        "--cache-dir",
        default=None,
        help=(
            f"Directory for per-commit analysis cache (default: <repo>/{CACHE_SUBDIR}). "
            f"Cached results are keyed by commit SHA, backend, model, and prompts, "
            f"so any meaningful change automatically invalidates stale entries."
        ),
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable the per-commit disk cache entirely.",
    )
    return parser


def _check_prerequisites(backend_name: str) -> Optional[str]:
    """Return an error message string if prerequisites are missing, else None."""
    if backend_name == "copilot":
        if not copilot_sdk_available:
            return (
                "error: the 'copilot' package is not installed. Install with:\n"
                "       pip install github-copilot-sdk\n"
                "(or pass --backend anthropic to use the Anthropic SDK directly)"
            )
    else:
        if anthropic is None:
            return (
                "error: the 'anthropic' package is not installed. Install with:\n"
                "       pip install -r tools/requirements.txt\n"
                "(or pass --dry-run to validate the commit range without API calls)"
            )
        if not os.environ.get("ANTHROPIC_API_KEY"):
            return (
                "error: ANTHROPIC_API_KEY env var is required for the anthropic backend\n"
                "(or pass --backend copilot to use GitHub Copilot CLI auth)"
            )
    return None


def _build_backend(backend_name: str) -> LLMBackend:
    if backend_name == "copilot":
        return CopilotBackend()
    return AnthropicBackend(anthropic.Anthropic(max_retries=SDK_MAX_RETRIES))


def _resolve_cache_dir(no_cache: bool, cache_dir_arg: Optional[str], repo: Path) -> Optional[Path]:
    if no_cache:
        return None
    cache_dir = Path(cache_dir_arg).resolve() if cache_dir_arg else repo / CACHE_SUBDIR
    cache_dir.mkdir(parents=True, exist_ok=True)
    print(f"  Cache dir: {cache_dir}")
    return cache_dir


class _AnalysisIncompleteError(RuntimeError):
    pass


async def _run_async(
    backend: LLMBackend,
    commits: list[Commit],
    commit_range: str,
    max_parallel: int,
    retry_rounds: int,
    cache_dir: Optional[Path],
    output_dir: Path,
) -> None:
    analyzed, stats = await analyze_with_retries(
        backend,
        commits,
        initial_parallel=max_parallel,
        retry_rounds=retry_rounds,
        cache_dir=cache_dir,
    )
    print_analysis_stats(stats, analyzed)

    if not stats.all_processed:
        raise _AnalysisIncompleteError(
            f"only {stats.total_succeeded}/{stats.total} commits analyzed successfully "
            f"after {retry_rounds + 1} round(s). Aborting -- refusing to write incomplete notes."
        )

    # Restore git-log order (newest first, like `git log` itself).
    order = {c.sha: i for i, c in enumerate(commits)}
    analyzed.sort(key=lambda a: order.get(a.commit.sha, 0))

    print("\nAll commits analyzed. Composing release notes ...")
    notes = await compose_release_notes(backend, analyzed, commit_range)

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / backend.notes_filename(sanitize_for_filename(commit_range))
    out_path.write_text(notes, encoding="utf-8")
    print(f"\nWrote {out_path}")


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        force=True,
    )
    args = _build_arg_parser().parse_args()

    repo = Path(args.repo).resolve()
    if not (repo / ".git").exists():
        print(f"error: {repo} is not a git repository", file=sys.stderr)
        return 1

    if not args.dry_run:
        if err := _check_prerequisites(args.backend):
            print(err, file=sys.stderr)
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

    backend = _build_backend(args.backend)
    cache_dir = _resolve_cache_dir(args.no_cache, args.cache_dir, repo)

    try:
        asyncio.run(
            _run_async(
                backend,
                commits,
                args.commit_range,
                args.max_parallel,
                args.retry_rounds,
                cache_dir,
                Path(args.output_dir).resolve(),
            )
        )
    except _AnalysisIncompleteError as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        print(
            "Hint: re-run with a higher --retry-rounds, lower --max-parallel, "
            "or wait for rate limits to recover.",
            file=sys.stderr,
        )
        return 2
    except Exception as e:
        print(f"\nERROR: composition failed ({type(e).__name__}: {e})", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
