#!/usr/bin/env python3
"""Generate PR-targeted fuzzing inputs from a code diff using an LLM.

Fuzzing terminology used in this file:
  - Seed:  An initial input file for the fuzzer. Each seed is a sequence of
           commands encoded in RESP wire format (see fuzz/seeds/resp/*.resp for examples).
           The fuzzer starts from these seeds and mutates them to explore code paths.
  - Targeted seed:  A seed crafted specifically to exercise code paths changed in a PR.
           We send the PR diff + a few existing seeds (as format examples) to an LLM, and
           it generates new seeds that target the changed code.
  - Focus commands:  A list of command names (e.g. ["SET", "GET"]) that the
           AFL++ mutator should prefer. When set, the mutator picks these commands ~70%
           of the time instead of choosing uniformly from all known commands.

Flow:
  1. Read unified diff from stdin, extract changed C++ file paths.
  2. Load a few existing seed files as format examples for the LLM.
  3. Call Claude API: send the diff + examples, get back JSON with seeds + focus commands.
  4. Fix RESP encoding (LLMs miscount byte lengths), validate, write to output dir.

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

# Max diff lines to send to the LLM (keeps cost/latency down)
MAX_DIFF_LINES = 8000

# How many existing seed files to include as format examples
NUM_EXAMPLE_SEEDS = 5

LLM_SYSTEM_PROMPT = """\
You are a fuzzing expert for Dragonfly, a Redis-compatible in-memory database written in C++.

Your job: given a code diff and example seed files, generate NEW fuzzing seeds that \
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

## RESP protocol format
Each command is a RESP array:
*<N>\\r\\n     (N = number of arguments including command name)
$<len>\\r\\n   (length of next argument)
<arg>\\r\\n    (the argument itself)
...repeated for each argument...

Example — SET mykey myvalue:
*3\\r\\n$3\\r\\nSET\\r\\n$5\\r\\nmykey\\r\\n$7\\r\\nmyvalue\\r\\n

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
    {"name": "pr_something.resp", "content": "*3\\r\\n$3\\r\\nSET\\r\\n..."},
    ...
  ]
}

Rules for seeds:
- 3-10 commands per seed, forming a logical sequence
- Include setup commands before queries (e.g. SET before GET)
- Test edge cases from the diff: boundary values, empty/huge inputs, type mismatches
- Include at least one seed wrapping commands in MULTI/EXEC
- Generate 3-8 seeds total
- Prefix all names with "pr_"
- Use literal \\r\\n in content strings"""


def extract_changed_files(diff_text):
    """Extract C++/header file paths from a unified diff."""
    files = []
    for match in re.finditer(r"^diff --git a/(.+?) b/(.+?)$", diff_text, re.MULTILINE):
        path = match.group(2)
        if re.search(r"\.(cc|h)$", path):
            files.append(path)
    return sorted(set(files))


def load_example_seeds(seeds_dir, count=NUM_EXAMPLE_SEEDS):
    """Load existing seed files to show the LLM the expected RESP format.

    We pick a diverse set covering different command families so the LLM
    understands the format and can generate similar files for new commands.
    """
    # Pick seeds that cover different command families
    preferred = [
        "string_ops.resp",
        "hash_ops.resp",
        "list_ops.resp",
        "transaction.resp",
        "json.resp",
        "set_ops.resp",
        "zset_ops.resp",
        "stream_ops.resp",
    ]

    examples = []
    seen = set()

    # First try preferred files
    for name in preferred:
        path = os.path.join(seeds_dir, name)
        if os.path.isfile(path) and name not in seen:
            with open(path) as f:
                examples.append({"name": name, "content": f.read()})
            seen.add(name)
        if len(examples) >= count:
            break

    # Fill remaining from directory
    if len(examples) < count:
        for path in sorted(glob.glob(os.path.join(seeds_dir, "*.resp"))):
            name = os.path.basename(path)
            if name not in seen:
                with open(path) as f:
                    examples.append({"name": name, "content": f.read()})
                seen.add(name)
            if len(examples) >= count:
                break

    return examples


def truncate_diff(diff_text, max_lines=MAX_DIFF_LINES):
    """Truncate diff to max_lines."""
    lines = diff_text.splitlines(True)
    if len(lines) <= max_lines:
        return diff_text, len(lines)
    return "".join(lines[:max_lines]), max_lines


def validate_resp(content):
    """Validate that content is parseable as RESP protocol."""
    pos = 0
    data = content.encode() if isinstance(content, str) else content
    found_any = False

    while pos < len(data):
        while pos < len(data) and data[pos : pos + 1] in (b"\r", b"\n", b" "):
            pos += 1
        if pos >= len(data):
            break
        if data[pos : pos + 1] != b"*":
            return False

        end = data.find(b"\r\n", pos)
        if end < 0:
            return False
        try:
            nargs = int(data[pos + 1 : end])
        except ValueError:
            return False
        if nargs < 0:
            return False
        pos = end + 2

        for _ in range(nargs):
            if pos >= len(data) or data[pos : pos + 1] != b"$":
                return False
            end = data.find(b"\r\n", pos)
            if end < 0:
                return False
            try:
                slen = int(data[pos + 1 : end])
            except ValueError:
                return False
            pos = end + 2
            if slen < 0:
                continue
            if pos + slen + 2 > len(data):
                return False
            if data[pos + slen : pos + slen + 2] != b"\r\n":
                return False
            pos += slen + 2

        found_any = True

    return found_any


def call_llm(diff_text, changed_files, example_seeds, api_key, model):
    """Call Claude API to generate targeted seeds from the diff."""
    try:
        import anthropic
    except ImportError:
        print("anthropic package not available", file=sys.stderr)
        return None

    truncated, num_lines = truncate_diff(diff_text)

    # Build examples section
    examples_text = ""
    for ex in example_seeds:
        examples_text += "--- %s ---\n%s\n\n" % (ex["name"], ex["content"].rstrip())

    prompt = (
        "Here are existing seed files for reference (RESP format):\n\n"
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

    # Handle markdown code blocks
    json_match = re.search(r"```(?:json)?\s*\n(.*?)\n```", text, re.DOTALL)
    if json_match:
        text = json_match.group(1)

    return json.loads(text)


def normalize_resp_content(content):
    """Normalize LLM-generated RESP content to have proper \\r\\n."""
    if not isinstance(content, str):
        return content
    content = content.replace("\\r\\n", "\r\n")
    if "\r\n" not in content and "\n" in content:
        content = content.replace("\n", "\r\n")
    return content


def fix_resp_content(content):
    """Re-encode RESP content, fixing both *N array counts and $len bulk string lengths.

    LLMs frequently miscount byte lengths and argument counts. This parses the
    intent (command names + argument payloads) and re-encodes with correct values.
    """
    data = content.encode() if isinstance(content, str) else content
    commands = []
    pos = 0

    while pos < len(data):
        # Skip whitespace between commands
        while pos < len(data) and data[pos : pos + 1] in (b"\r", b"\n", b" "):
            pos += 1
        if pos >= len(data):
            break
        if data[pos : pos + 1] != b"*":
            pos += 1
            continue

        end = data.find(b"\r\n", pos)
        if end < 0:
            break
        try:
            nargs = int(data[pos + 1 : end])
        except ValueError:
            break
        pos = end + 2

        args = []
        for _ in range(nargs):
            if pos >= len(data) or data[pos : pos + 1] != b"$":
                break
            end = data.find(b"\r\n", pos)
            if end < 0:
                break
            pos = end + 2
            # Find actual payload by looking for next \r\n
            payload_end = data.find(b"\r\n", pos)
            if payload_end < 0:
                args.append(data[pos:])
                pos = len(data)
                break
            args.append(data[pos:payload_end])
            pos = payload_end + 2

        if args:
            commands.append(args)

    # Re-encode with correct counts and lengths
    result = bytearray()
    for cmd in commands:
        result.extend(b"*%d\r\n" % len(cmd))
        for arg in cmd:
            result.extend(b"$%d\r\n%s\r\n" % (len(arg), arg))

    return bytes(result)


def write_output(output_dir, focus_commands, seeds):
    """Write seed files and focus_commands.json to output directory."""
    os.makedirs(output_dir, exist_ok=True)

    focus_path = os.path.join(output_dir, "focus_commands.json")
    with open(focus_path, "w") as f:
        json.dump(focus_commands, f)
    print("Wrote %d focus commands to %s" % (len(focus_commands), focus_path), file=sys.stderr)

    written = 0
    for seed in seeds:
        name = seed.get("name", "pr_seed_%d.resp" % written)
        if not name.endswith(".resp"):
            name += ".resp"
        path = os.path.join(output_dir, name)
        content = seed["content"]
        if isinstance(content, bytes):
            with open(path, "wb") as f:
                f.write(content)
        else:
            with open(path, "w", newline="") as f:
                f.write(content)
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
    print("Loaded %d example seeds" % len(example_seeds), file=sys.stderr)

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

    # Extract and validate seeds
    valid_seeds = []
    for s in result.get("seeds", []):
        if not isinstance(s, dict) or "content" not in s:
            continue
        s["content"] = normalize_resp_content(s["content"])
        s["content"] = fix_resp_content(s["content"])
        if validate_resp(s["content"]):
            valid_seeds.append(s)
        else:
            print("Discarding invalid seed: %s" % s.get("name", "?"), file=sys.stderr)

    if not valid_seeds and not focus_commands:
        print("LLM returned no usable output", file=sys.stderr)
        return

    print(
        "LLM generated %d valid seeds, %d focus commands" % (len(valid_seeds), len(focus_commands)),
        file=sys.stderr,
    )
    write_output(args.output_dir, focus_commands, valid_seeds)


if __name__ == "__main__":
    main()
