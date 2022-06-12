# Contributing to Dragonfly DB

Thank you for your interest in Dragonfly DB.

Feel free to browse our [Discussions](https://github.com/dragonflydb/dragonfly/discussions) and [Issues](https://github.com/dragonflydb/dragonfly/issues)

# Best Practices

# Signoff Commits
All community submissions must include a signoff.

```bash
git commit -s -m '...'
```

# Squash Commits
Please squash all commits for a change into a single commit (this can be done using "git rebase -i"). Do your best to have a well-formed commit message for the change.

# Use Conventional Commits
This repo uses [Conventional Commmits](https://www.conventionalcommits.org/en/v1.0.0/)


The Conventional Commits specification is a lightweight convention on top of commit messages.
It provides an easy set of rules for creating an explicit commit history;
which makes it easier to write automated tools on top of.
This convention dovetails with [SemVer](http://semver.org),
by describing the features, fixes, and breaking changes made in commit messages.

The commit message should be structured as follows:

---

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```
---

This repo uses automated tools to standardize the formatting of code, text files and commits.
 - [Pre-commit hooks](#pre-commit-hooks) validate and automatically apply code
   formatting rules.

# `pre-commit` hooks
The Dragonfly DB team has agreed to systematically use a number of pre-commit hooks to
normalize formatting of code. You need to install and enable pre-commit to have these used
when you do your own commits.

## Fork and Clone the DragonflyDB repository
```
# Fork https://github.com/dragonflydb/dragonfly

# Clone from your fork
git@github.com:<YOURUSERNAME>/dragonfly.git

cd dragonfly

# IMPORTANT! Enable our pre-commit message hooks
# This will ensure your commits match our formatting requirements
pre-commit install --hook-type commit-msg
```

This step must be done on each machine you wish to develop and contribute from to activate the `commit-msg` hook client-side.


Once you have done these things, we look forward to adding your contributions and improvements to the Dragonfly DB project.


## License terms for contributions
This Project welcomes contributions, suggestions, and feedback.  Dragonfly uses the BSL license. You represent that if you do not own copyright in the code that you have the authority to submit it under the BSL. All feedback, suggestions, or contributions are not confidential.


## THANK YOU FOR YOUR CONTRIBUTIONS