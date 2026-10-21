#!/usr/bin/env python3
"""Create deterministic public issue and pull request links for dashboard tests."""

from __future__ import annotations

import argparse
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 3
DEFAULT_REPOSITORY = "dragonflydb/dragonfly"
NAME_ONLY_MIN_LENGTH = 12
MIN_ALIAS_LENGTH = 8
CLOSED_HISTORY_DAYS = 180
MAX_CLOSED_HISTORY_RECORDS = 500


def github_api(url: str, token: str) -> Any:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(
        url,
        headers=headers,
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", "replace")
        raise RuntimeError(f"GitHub API request failed ({error.code}): {detail}") from error


def github_pages(endpoint: str, token: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    page = 1
    while True:
        separator = "&" if "?" in endpoint else "?"
        page_items = github_api(f"{endpoint}{separator}per_page=100&page={page}", token)
        if not isinstance(page_items, list):
            raise RuntimeError(f"Unexpected GitHub API response from {endpoint}")
        items.extend(page_items)
        if len(page_items) < 100:
            return items
        page += 1


def github_recent_pages(
    endpoint: str, token: str, cutoff: datetime, maximum: int, include: Any = lambda item: True
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    page = 1
    while True:
        separator = "&" if "?" in endpoint else "?"
        page_items = github_api(
            f"{endpoint}{separator}per_page=100&page={page}&sort=updated&direction=desc", token
        )
        if not isinstance(page_items, list):
            raise RuntimeError(f"Unexpected GitHub API response from {endpoint}")
        recent_items = []
        reached_cutoff = False
        for item in page_items:
            updated_at = datetime.fromisoformat(item["updated_at"].replace("Z", "+00:00"))
            if updated_at < cutoff:
                reached_cutoff = True
                break
            closed_at = datetime.fromisoformat(item["closed_at"].replace("Z", "+00:00"))
            if closed_at >= cutoff and include(item):
                recent_items.append(item)
        items.extend(recent_items)
        if len(items) >= maximum:
            return items[:maximum]
        if len(page_items) < 100 or reached_cutoff:
            return items
        page += 1


def normalize(value: str) -> str:
    return " ".join(value.casefold().split())


def record_text(record: dict[str, Any]) -> str:
    return normalize(f"{record.get('title', '')}\n{record.get('body') or ''}")


def unparameterized(value: str) -> str:
    value = re.sub(r"\[.*\]$", "", value)
    prefix, separator, name = value.rpartition("::")
    if separator:
        prefix = re.sub(r"/\d+$", "", prefix)
        return f"{prefix}{separator}{re.sub(r'/[^/]+$', '', name)}"
    return value


def without_gtest_instantiation(value: str) -> str:
    prefix, separator, name = value.rpartition("::")
    group, slash, suite = prefix.rpartition("/")
    if separator and slash and not suite.isdigit():
        return f"{suite}{separator}{name}"
    return value


def aliases_for_test(test: dict[str, Any]) -> list[tuple[str, str]]:
    full = test.get("display_name", "")
    aliases = [(normalize(full), "exact")]
    base = normalize(unparameterized(full))
    if base and base != aliases[0][0]:
        aliases.append((base, "parameterized"))
    if test.get("level") == "gtest":
        aliases.extend(
            (alias.replace("::", "."), match) for alias, match in tuple(aliases) if "::" in alias
        )
        stripped_aliases = [
            (without_gtest_instantiation(alias), match) for alias, match in tuple(aliases)
        ]
        aliases.extend(
            (alias, match)
            for alias, match in stripped_aliases
            if alias not in {existing for existing, _ in aliases}
        )
        aliases.extend(
            (alias.replace("::", "."), match)
            for alias, match in stripped_aliases
            if "::" in alias
            and alias.replace("::", ".") not in {existing for existing, _ in aliases}
        )
    name = normalize(test.get("name", ""))
    if test.get("level") == "pytest":
        classname = test.get("classname", "")
        if classname and name:
            parts = classname.split(".")
            class_index = next(
                (index for index, part in enumerate(parts) if part.startswith("Test")), len(parts)
            )
            module = "/".join(parts[:class_index])
            node_id = normalize(f"{module}.py::{'::'.join([*parts[class_index:], name])}")
            if node_id not in {alias for alias, _ in aliases}:
                aliases.append((node_id, "exact"))
            base_node_id = normalize(unparameterized(node_id))
            if base_node_id != node_id and base_node_id not in {alias for alias, _ in aliases}:
                aliases.append((base_node_id, "parameterized"))
    if len(name) >= NAME_ONLY_MIN_LENGTH and name not in {alias for alias, _ in aliases}:
        aliases.append((name, "name"))
    base_name = unparameterized(name)
    if test.get("level") == "gtest":
        base_name = re.sub(r"/[^/]+$", "", base_name)
    base_name = normalize(base_name)
    if len(base_name) >= NAME_ONLY_MIN_LENGTH and base_name not in {alias for alias, _ in aliases}:
        aliases.append((base_name, "parameterized"))
    return [(alias, match) for alias, match in aliases if len(alias) >= MIN_ALIAS_LENGTH]


def match_aliases(aliases: list[tuple[str, str]], text: str) -> str | None:
    for alias, match in aliases:
        if alias not in text:
            continue
        right_boundary = r"(?![\w]|\[|/)" if match == "parameterized" else r"(?![\w])"
        if match == "parameterized" and re.search(rf"{re.escape(alias)}(?:\[[^]]*\]|/)", text):
            continue
        bare_name = match == "name" or (
            match == "parameterized" and "::" not in alias and "/" not in alias
        )
        left_boundary = r"(?<![\w.:/])" if bare_name else r"(?<![\w])"
        if re.search(rf"{left_boundary}{re.escape(alias)}{right_boundary}", text):
            return match
    return None


def issue_match(test: dict[str, Any], issue: dict[str, Any]) -> str | None:
    return match_aliases(aliases_for_test(test), record_text(issue))


def pull_request_references_issue(
    pull_request: dict[str, Any], issue_number: int, repository: str
) -> bool:
    text = record_text(pull_request)
    escaped_repository = re.escape(repository)
    return any(
        re.search(pattern, text) is not None
        for pattern in (
            rf"(?<![\w/])#{issue_number}\b",
            rf"(?<![\w/]){escaped_repository}#{issue_number}\b",
            rf"https?://github\.com/{escaped_repository}/issues/{issue_number}\b",
        )
    )


def record_state(record: dict[str, Any]) -> str:
    if record.get("merged_at"):
        return "merged"
    return record.get("state", "closed")


def build_links(
    tests: list[dict[str, Any]],
    issues: list[dict[str, Any]],
    pull_requests: list[dict[str, Any]],
    repository: str = DEFAULT_REPOSITORY,
) -> dict[str, dict[str, Any]]:
    links: dict[str, dict[str, Any]] = {}
    issue_records = [(issue, record_text(issue)) for issue in issues if "pull_request" not in issue]
    pull_request_records = [
        (pull_request, record_text(pull_request)) for pull_request in pull_requests
    ]
    for test in tests:
        aliases = aliases_for_test(test)
        matched_issues = []
        for issue, text in issue_records:
            match = match_aliases(aliases, text)
            if match:
                matched_issues.append(
                    {
                        "number": issue["number"],
                        "url": issue["html_url"],
                        "title": issue["title"],
                        "state": record_state(issue),
                        "match": match,
                        "eligible_for_assignee": record_state(issue) == "open"
                        and match in {"exact", "parameterized"},
                        "assignees": [assignee["login"] for assignee in issue.get("assignees", [])],
                    }
                )
        matched_issues.sort(key=lambda issue: issue["number"], reverse=True)
        issue_numbers = {issue["number"] for issue in matched_issues}
        matched_prs = []
        for pull_request, text in pull_request_records:
            if match_aliases(aliases, text) or any(
                pull_request_references_issue(pull_request, number, repository)
                for number in issue_numbers
            ):
                matched_prs.append(
                    {
                        "number": pull_request["number"],
                        "url": pull_request["html_url"],
                        "title": pull_request["title"],
                        "state": record_state(pull_request),
                    }
                )
        matched_prs.sort(key=lambda pull_request: pull_request["number"], reverse=True)
        if matched_issues or matched_prs:
            links[test["id"]] = {"issues": matched_issues, "pull_requests": matched_prs}
    return links


def load_tests(data_dir: Path) -> list[dict[str, Any]]:
    tests: dict[str, dict[str, Any]] = {}
    for range_file in (data_dir / "ranges").glob("*.json"):
        for test in json.loads(range_file.read_text())["tests"]:
            if test.get("failures", 0) > 0:
                tests[test["id"]] = test
    return list(tests.values())


def write_links(data_dir: Path, repository: str, token: str) -> None:
    tests = load_tests(data_dir)
    base_url = f"https://api.github.com/repos/{urllib.parse.quote(repository, safe='/')}"
    issues = github_pages(f"{base_url}/issues?state=open", token)
    pull_requests = github_pages(f"{base_url}/pulls?state=open", token)
    cutoff = datetime.now(timezone.utc) - timedelta(days=CLOSED_HISTORY_DAYS)
    issues.extend(
        github_recent_pages(
            f"{base_url}/issues?state=closed",
            token,
            cutoff,
            MAX_CLOSED_HISTORY_RECORDS,
            lambda item: "pull_request" not in item,
        )
    )
    pull_requests.extend(
        github_recent_pages(
            f"{base_url}/pulls?state=closed", token, cutoff, MAX_CLOSED_HISTORY_RECORDS
        )
    )
    links = build_links(tests, issues, pull_requests, repository)
    write_link_payload(data_dir, repository, links)
    print(
        f"Matched {len(links)} dashboard tests to open or recent closed records from "
        f"{repository}",
        flush=True,
    )


def write_link_payload(data_dir: Path, repository: str, links: dict[str, dict[str, Any]]) -> None:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "repository": repository,
        "links": links,
    }
    (data_dir / "issue-links.json").write_text(json.dumps(payload, separators=(",", ":")))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data_dir", type=Path)
    parser.add_argument("--repository", default=DEFAULT_REPOSITORY)
    parser.add_argument("--token", default=os.environ.get("GITHUB_TOKEN", ""))
    args = parser.parse_args()
    try:
        write_links(args.data_dir, args.repository, args.token)
    except (RuntimeError, urllib.error.URLError, TimeoutError) as error:
        print(f"GitHub link lookup failed: {error}", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
