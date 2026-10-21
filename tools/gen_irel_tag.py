#!/usr/bin/env python3
"""
Print the command to tag the fetched release branch HEAD for an internal release.
Choose the release line with --release or interactively.
Fetches and validates release history; does not create or push tags.

Examples:
    ./tools/gen_irel_tag.py
    ./tools/gen_irel_tag.py --release 1.40
    ./tools/gen_irel_tag.py --release v1.40-branch
"""

import argparse
import re
import subprocess
import sys

UPSTREAM_RE = re.compile(
    r"(?:https://github\.com/|git@github\.com:|ssh://git@github\.com(?::22)?/)"
    r"dragonflydb/dragonfly(?:\.git)?/?"
)
BRANCH_RE = re.compile(r"^v(\d+)\.(\d+)-branch$")
RELEASE_TAG_RE = re.compile(r"^([vi])(\d+)\.(\d+)\.(\d+)$")
NUM_BRANCH_CHOICES = 5


class CeremonyError(Exception):
    pass


def git(*args, check=True):
    res = subprocess.run(["git", *args], capture_output=True, text=True)
    if check and res.returncode != 0:
        raise CeremonyError(f"git {' '.join(args)} failed:\n{res.stderr.strip()}")
    return res


def git_out(*args):
    return git(*args).stdout.strip()


def interactive():
    return sys.stdin.isatty()


def ask(prompt, default=None):
    suffix = f" [{default}]" if default else ""
    try:
        answer = input(f"{prompt}{suffix}: ").strip()
    except EOFError:
        raise CeremonyError("aborted: no input")
    return answer or (default or "")


def choose(title, options, manual_prompt):
    print(f"\n{title}")
    for i, (_, label) in enumerate(options, 1):
        print(f"  {i}) {label}")
    manual_idx = len(options) + 1
    print(f"  {manual_idx}) type manually")
    while True:
        answer = ask("Choose", "1" if options else str(manual_idx))
        if answer.isdigit() and 1 <= int(answer) <= manual_idx:
            idx = int(answer)
            if idx == manual_idx:
                return ask(manual_prompt)
            return options[idx - 1][0]
        print(f"  Enter a number between 1 and {manual_idx}.")


def find_remote(requested):
    remotes = git_out("remote").split()
    candidates = [requested] if requested else remotes
    for name in candidates:
        if name not in remotes:
            raise CeremonyError(f"no such git remote: {name}")
        fetch_urls = git_out("remote", "get-url", "--all", name).splitlines()
        push_urls = git_out("remote", "get-url", "--push", "--all", name).splitlines()
        if (
            len(fetch_urls) == len(push_urls) == 1
            and UPSTREAM_RE.fullmatch(fetch_urls[0])
            and UPSTREAM_RE.fullmatch(push_urls[0])
        ):
            return name
        if requested:
            raise CeremonyError(
                f"remote {name} must have exactly one fetch URL and one push URL, both pointing "
                "to github.com/dragonflydb/dragonfly via HTTPS or SSH"
            )
    raise CeremonyError(
        "no git remote has a single fetch and push destination at github.com/dragonflydb/dragonfly; "
        "configure one and pass --remote"
    )


def ls_remote(remote, *patterns):
    refs = {}
    for line in git_out("ls-remote", remote, *patterns).splitlines():
        sha, ref = line.split("\t")
        if ref.endswith("^{}"):
            refs[ref[: -len("^{}")]] = sha
        else:
            refs.setdefault(ref, sha)
    return refs


def release_branches(remote):
    branches = []
    for ref in ls_remote(remote, "refs/heads/v*-branch"):
        name = ref[len("refs/heads/") :]
        m = BRANCH_RE.match(name)
        if m:
            branches.append(((int(m[1]), int(m[2])), name))
    return [name for _, name in sorted(branches, reverse=True)]


def normalize_branch(text):
    text = text.strip()
    if not text.startswith("v"):
        text = "v" + text
    if not text.endswith("-branch"):
        text += "-branch"
    m = BRANCH_RE.match(text)
    if not m:
        raise CeremonyError(f"not a release line: {text!r} (expected e.g. 1.40 or v1.40-branch)")
    return text, (int(m[1]), int(m[2]))


def release_tags(remote, major, minor):
    tags = []
    refs = ls_remote(remote, f"refs/tags/v{major}.{minor}.*", f"refs/tags/i{major}.{minor}.*")
    for ref, sha in refs.items():
        name = ref[len("refs/tags/") :]
        m = RELEASE_TAG_RE.match(name)
        if m and (int(m[2]), int(m[3])) == (major, minor):
            tags.append((int(m[4]), name, sha))
    return sorted(tags)


def is_ancestor(ancestor, descendant):
    result = git("merge-base", "--is-ancestor", ancestor, descendant, check=False)
    if result.returncode not in (0, 1):
        raise CeremonyError(f"could not check release ancestry:\n{result.stderr.strip()}")
    return result.returncode == 0


def have_commit(sha):
    return git("cat-file", "-e", f"{sha}^{{commit}}", check=False).returncode == 0


def pick_branch(args, remote):
    if args.release:
        return normalize_branch(args.release)
    if not interactive():
        raise CeremonyError("--release is required when not running interactively")
    branches = release_branches(remote)[:NUM_BRANCH_CHOICES]
    text = choose(
        "Release branch:", [(b, b) for b in branches], "Release line (e.g. 1.40 or v1.40-branch)"
    )
    return normalize_branch(text)


def ceremony(args):
    remote = find_remote(args.remote)
    branch, (major, minor) = pick_branch(args, remote)

    if f"refs/heads/{branch}" not in ls_remote(remote, f"refs/heads/{branch}"):
        raise CeremonyError(f"no release branch {branch} on {remote}")
    git("fetch", "--no-tags", remote, f"refs/heads/{branch}")
    commit = git_out("rev-parse", "--verify", "FETCH_HEAD^{commit}")

    tags = release_tags(remote, major, minor)
    for _, name, _ in tags[::-1][:10]:
        print(f"tag={name!r}")
    if not any(name == f"v{major}.{minor}.0" for _, name, _ in tags):
        raise CeremonyError(
            f"v{major}.{minor}.0 does not exist yet; internal releases on this line must come "
            "after the first public release"
        )
    prev_patch, prev_tag, prev_sha = tags[-1]
    tag = f"i{major}.{minor}.{prev_patch + 1}"

    if not have_commit(prev_sha):
        git("fetch", "--no-tags", remote, f"refs/tags/{prev_tag}")
        if not have_commit(prev_sha):
            raise CeremonyError(
                f"previous release commit {prev_sha} for {prev_tag} is unavailable after fetch; "
                "the remote tag may have changed; rerun to review"
            )
    if not is_ancestor(prev_sha, commit):
        raise CeremonyError(
            f"cannot verify that {remote}/{branch} HEAD ({commit}) contains the previous release "
            f"{prev_tag} ({prev_sha}); check for diverged or shallow history before retrying"
        )

    warnings = []
    already = [name for _, name, sha in tags if sha == commit]
    if already:
        warnings.append(f"this commit is already tagged as {', '.join(already)}")

    local = git("rev-parse", "--verify", "--quiet", f"refs/tags/{tag}^{{commit}}", check=False)
    if local.returncode == 0:
        raise CeremonyError(
            f"a local tag {tag} already exists (stale?); remove it with `git tag -d {tag}`"
        )

    print(f"\nTag:      {tag}")
    print(f"Commit:   {git_out('log', '-1', '--format=%h %s', commit)}")
    print(f"Branch:   {remote}/{branch}")
    print(f"Previous: {prev_tag}")
    changes = git_out("log", "--format=%h %s", f"{prev_sha}..{commit}").splitlines()
    print(f"Changes since {prev_tag}:")
    for line in changes or ["(none)"]:
        print(f"  {line}")
    for w in warnings:
        print(f"WARNING:  {w}")

    if release_tags(remote, major, minor) != tags:
        raise CeremonyError("release tags changed during validation; rerun to review")

    print("\nRun this command to create the local tag (rerun this script if tagging later):")
    print(f"git tag {tag} {commit}")


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--release", help="release line: 1.40, v1.40 or v1.40-branch")
    parser.add_argument("--remote", help="git remote for dragonflydb/dragonfly (auto-detected)")
    args = parser.parse_args()

    try:
        ceremony(args)
    except CeremonyError as e:
        sys.exit(f"error: {e}")
    except KeyboardInterrupt:
        sys.exit("\nAborted.")


if __name__ == "__main__":
    main()
