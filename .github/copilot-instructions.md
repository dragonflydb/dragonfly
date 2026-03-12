---
description: 'Code review guidelines for GitHub copilot in this project'
applyTo: '**'
excludeAgent: ["coding-agent"]
---

# Code Review Instructions

Keep reviews high-signal and minimal. Only comment on real bugs with high confidence.

## Comment Only When
- The issue is a correctness, security, concurrency, or architecture problem.
- The impact is clear and non-trivial.
- You can point to concrete evidence in the diff (not speculation).

## Avoid
- Style, formatting, naming, or minor performance nits.
- Optional refactors or “nice to have” suggestions.
- Praise, restating the code, or long explanations.
- Duplicate comments for the same root cause.

## Review Style
- Be terse: 1-2 sentences per issue.
- Include file and line references when possible.
- If no issues are found, say “No issues found.”
- Provide concrete suggestions for fixes when possible, or examples to illustrate the problem.
