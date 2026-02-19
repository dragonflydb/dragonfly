#!/usr/bin/env python3
"""Generate PR-targeted fuzzing inputs from a code diff using an LLM.

Fuzzing terminology used in this file:
  - Seed:  An initial input file for the fuzzer. Each seed is a sequence of
           commands encoded in RESP wire format (see fuzz/seeds/resp/*.resp for examples).
           The fuzzer starts from these seeds and mutates them to explore code paths.
  - Targeted seed:  A seed crafted specifically to exercise code paths changed in a PR.
           We send the PR diff + all existing seeds to an LLM, and it generates new seeds
           that target the changed code.
  - Focus commands:  A list of command names (e.g. ["SET", "GET"]) that the
           AFL++ mutator should prefer. When set, the mutator picks these commands ~70%
           of the time instead of choosing uniformly from all known commands.

Flow:
  1. Read unified diff from stdin, extract changed C++ file paths.
  2. Load all existing seed files so the LLM knows what's already covered.
  3. Call Claude API: send the diff + seeds, get back JSON with command arrays + focus commands.
  4. Encode commands as RESP wire format, write to output dir.

The LLM returns commands as plain arrays (e.g. ["SET", "key", "value"]) and we handle
RESP encoding ourselves — this avoids JSON escaping issues and byte-count mismatches.

When ANTHROPIC_API_KEY is not available (e.g. fork PRs), exits with no output and
the fuzzer runs with the existing seed corpus as-is.

Usage:
    git diff base..HEAD | python3 fuzz/generate_targeted_seeds.py --output-dir /tmp/seeds
"""

import argparse
import glob
import json
import os
import re
import sys

# Max diff lines to send to the LLM (Haiku handles ~200K tokens, so this is generous)
MAX_DIFF_LINES = 20000

LLM_SYSTEM_PROMPT = """\
You are a fuzzing expert for Dragonfly, a Redis-compatible in-memory database written in C++.

Your job: given a code diff and existing seed files, generate NEW fuzzing seeds that \
target the changed code paths. You also return a list of Redis commands to focus on.

## Dragonfly architecture (for context)
- src/server/*_family.cc — command implementations (e.g. string_family.cc has GET/SET/INCR)
- src/server/main_service.cc — command dispatch, MULTI/EXEC
- src/server/db_slice.cc — per-shard key-value storage
- src/facade/redis_parser.cc — RESP protocol parsing
- src/facade/dragonfly_connection.cc — connection handling
- src/core/ — data structures (dash table, dense_set, compact_object, etc.)
- src/server/journal/ — replication journal
- src/server/cluster/ — cluster mode
- src/server/search/ — search module (FT.* commands)
- src/server/tiering/ — SSD tiering

## What to generate
Based on the diff, figure out:
1. What commands are affected (new, modified, or impacted by infrastructure changes)
2. What edge cases the changes introduce (boundary values, empty inputs, error paths)
3. What command sequences would stress the changed code

## Output format
Return valid JSON (no markdown, no explanation):
{
  "focus_commands": ["CMD1", "CMD2", ...],
  "seeds": [
    {
      "name": "pr_something.resp",
      "commands": [
        ["SET", "mykey", "myvalue"],
        ["GET", "mykey"]
      ]
    }
  ]
}

Each "commands" entry is a list of Redis commands. Each command is a list of strings \
(command name + arguments). We handle RESP wire encoding — just give plain strings.

CRITICAL: Output must be valid JSON. Do NOT use code expressions like "x" * 1024 or \
string concatenation. For long values write actual repeated characters inline, e.g. \
"xxxxxxxxxx" (just the literal string). Keep values short (under 100 chars) — \
the fuzzer will mutate and grow them.

Rules for seeds:
- 3-10 commands per seed, forming a logical sequence
- Include setup commands before queries (e.g. SET before GET)
- Test edge cases from the diff: boundary values, empty/huge inputs, type mismatches
- Include at least one seed wrapping commands in MULTI/EXEC
- Generate 3-8 seeds total
- Prefix all names with "pr_"
"""


def extract_changed_files(diff_text):
    """Extract C++/header file paths from a unified diff."""
    files = []
    for match in re.finditer(r"^diff --git a/(.+?) b/(.+?)$", diff_text, re.MULTILINE):
        path = match.group(2)
        if re.search(r"\.(cc|h)$", path):
            files.append(path)
    return sorted(set(files))


def load_example_seeds(seeds_dir):
    """Load ALL existing seed files to show the LLM what's already covered.

    We send every seed so the LLM has full context about existing coverage
    and can generate complementary seeds for new/changed code paths.
    """
    examples = []
    for path in sorted(glob.glob(os.path.join(seeds_dir, "*.resp"))):
        name = os.path.basename(path)
        with open(path) as f:
            examples.append({"name": name, "content": f.read()})
    return examples


def truncate_diff(diff_text, max_lines=MAX_DIFF_LINES):
    """Truncate diff to max_lines."""
    lines = diff_text.splitlines(True)
    if len(lines) <= max_lines:
        return diff_text, len(lines)
    return "".join(lines[:max_lines]), max_lines


def encode_resp(commands):
    """Encode a list of commands as RESP wire format.

    Each command is a list of string arguments, e.g. ["SET", "key", "value"].
    Returns bytes in RESP format: *N\\r\\n$len\\r\\narg\\r\\n...
    """
    result = bytearray()
    for cmd in commands:
        if not cmd:
            continue
        result.extend(b"*%d\r\n" % len(cmd))
        for arg in cmd:
            arg_bytes = arg.encode() if isinstance(arg, str) else arg
            result.extend(b"$%d\r\n%s\r\n" % (len(arg_bytes), arg_bytes))
    return bytes(result)


def call_llm(diff_text, changed_files, example_seeds, api_key, model):
    """Call Claude API to generate targeted seeds from the diff."""
    try:
        import anthropic
    except ImportError:
        print("anthropic package not available", file=sys.stderr)
        return None

    truncated, num_lines = truncate_diff(diff_text)

    # Build examples section — show existing seeds so the LLM knows what's covered
    examples_text = ""
    for ex in example_seeds:
        examples_text += "--- %s ---\n%s\n\n" % (ex["name"], ex["content"].rstrip())

    prompt = (
        "Here are ALL existing seed files (RESP wire format) so you know what's already covered:\n\n"
        "%s\n"
        "Now analyze this diff and generate targeted fuzzing seeds.\n\n"
        "Changed files: %s\n\n"
        "Diff (%d lines):\n```\n%s\n```\n\n"
        "Respond with valid JSON only."
    ) % (examples_text, ", ".join(changed_files), num_lines, truncated)

    client = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model=model,
        max_tokens=16384,
        system=LLM_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )

    text = response.content[0].text.strip()

    # Try to extract JSON from the response (LLMs sometimes wrap in markdown)
    json_match = re.search(r"```(?:json)?\s*\n(.*?)\n```", text, re.DOTALL)
    if json_match:
        text = json_match.group(1)

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Try to find the outermost { ... } and parse that
    brace_match = re.search(r"\{.*\}", text, re.DOTALL)
    if brace_match:
        try:
            return json.loads(brace_match.group(0))
        except json.JSONDecodeError:
            pass

    # Log raw response for debugging and raise
    print("Raw LLM response (first 2000 chars):\n%s" % text[:2000], file=sys.stderr)
    raise ValueError("Could not parse LLM response as JSON")


def write_output(output_dir, focus_commands, seeds):
    """Write seed files and focus_commands.json to output directory."""
    os.makedirs(output_dir, exist_ok=True)

    focus_path = os.path.join(output_dir, "focus_commands.json")
    with open(focus_path, "w") as f:
        json.dump(focus_commands, f)
    print("Wrote %d focus commands to %s" % (len(focus_commands), focus_path), file=sys.stderr)

    written = 0
    for seed in seeds:
        name = seed.get("name") or "pr_seed_%d.resp" % written
        if not name.endswith(".resp"):
            name += ".resp"
        path = os.path.join(output_dir, name)
        with open(path, "wb") as f:
            f.write(seed["content"])
        written += 1

    print("Wrote %d seed files to %s" % (written, output_dir), file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Generate targeted fuzzing seeds from a PR diff")
    parser.add_argument(
        "--output-dir",
        default="fuzz/seeds/pr_targeted",
        help="Directory to write seeds and focus_commands.json",
    )
    parser.add_argument(
        "--seeds-dir",
        default=None,
        help="Directory with existing seed files (auto-detected if not set)",
    )
    parser.add_argument(
        "--api-key", default=None, help="Anthropic API key (or set ANTHROPIC_API_KEY env var)"
    )
    parser.add_argument("--model", default="claude-haiku-4-5-20251001", help="Claude model to use")
    args = parser.parse_args()

    api_key = args.api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("No ANTHROPIC_API_KEY set, skipping seed generation", file=sys.stderr)
        return

    diff_text = sys.stdin.read()
    if not diff_text.strip():
        print("No diff provided, skipping", file=sys.stderr)
        return

    changed_files = extract_changed_files(diff_text)
    if not changed_files:
        print("No C++ files in diff, skipping", file=sys.stderr)
        return

    print("Changed C++ files: %s" % ", ".join(changed_files), file=sys.stderr)

    # Find seeds directory
    seeds_dir = args.seeds_dir
    if not seeds_dir:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        seeds_dir = os.path.join(script_dir, "seeds", "resp")

    example_seeds = load_example_seeds(seeds_dir)
    print("Loaded %d existing seeds" % len(example_seeds), file=sys.stderr)

    try:
        result = call_llm(diff_text, changed_files, example_seeds, api_key, args.model)
    except Exception as e:
        print("LLM call failed: %s" % e, file=sys.stderr)
        return

    if not result:
        return

    # Extract focus commands
    focus_commands = result.get("focus_commands", [])
    if not isinstance(focus_commands, list):
        focus_commands = []

    # Encode command arrays as RESP and collect valid seeds
    valid_seeds = []
    for s in result.get("seeds", []):
        if not isinstance(s, dict) or "commands" not in s:
            continue
        commands = s["commands"]
        if not isinstance(commands, list) or not commands:
            continue
        # Filter out non-list entries and ensure all args are strings
        clean_commands = []
        for cmd in commands:
            if isinstance(cmd, list) and cmd:
                clean_commands.append([str(arg) for arg in cmd])
        if not clean_commands:
            continue
        content = encode_resp(clean_commands)
        if content:
            valid_seeds.append({"name": s.get("name") or "", "content": content})
        else:
            print("Discarding empty seed: %s" % s.get("name", "?"), file=sys.stderr)

    if not valid_seeds and not focus_commands:
        print("LLM returned no usable output", file=sys.stderr)
        return

    print(
        "Generated %d seeds, %d focus commands" % (len(valid_seeds), len(focus_commands)),
        file=sys.stderr,
    )
    write_output(args.output_dir, focus_commands, valid_seeds)


if __name__ == "__main__":
    main()
