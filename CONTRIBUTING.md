# Contributing to Dragonfly DB

Thank you for your interest in Dragonfly DB.

Feel free to browse our [Discussions](https://github.com/dragonflydb/dragonfly/discussions) and [Issues](https://github.com/dragonflydb/dragonfly/issues)

## Build from source

See [building from source](./docs/build-from-source.md)

Please note that to build a development/debug version,
it's better to alter the configure and build steps above with:

```sh
./helio/blaze.sh   # without '-release' flag. Creates build-dbg subfolder
cd build-dbg && ninja dragonfly
```

## Before you make your changes

```sh
cd dragonfly   # project root

# Make sure you have 'pre-commit', 'clang-format' and black is installed
pip install pre-commit clang-format
pip install pre-commit black

# IMPORTANT! Enable our pre-commit message hooks
# This will ensure your commits match our formatting requirements
pre-commit install
```

This step must be done on each machine you wish to develop and contribute from to activate the `commit-msg` and `commit` hooks client-side.

Once you have done these things, we look forward to adding your contributions and improvements to the Dragonfly DB project.

## Unit testing

```
# Build a specific test
cd build-dbg && ninja [test_name]
# e.g cd build-dbg && ninja generic_family_test

# Run
./[test_name]
# e.g ./generic_family_test
```

## Rendering Helm golden files

A Golang golden test is included in the dragonfly helm chart. This test will render the chart and compare the output to a golden file. If the output has changed, the test will fail and the golden file will need to be updated. This can be done by running:

```bash
cd contrib/charts/dragonfly
go test -v ./... -update
```

This makes it easy to see the changes in the rendered output without having to manually run the `helm template` and diff the output.

## Signoff Commits

All community submissions must include a signoff.

```bash
git commit -s -m '...'
```

## Squash Commits

Please squash all commits for a change into a single commit (this can be done using "git rebase -i"). Do your best to have a well-formed commit message for the change.

## Use Conventional Commits

This repo uses [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/)

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

This repo uses automated tools to standardize the formatting of code, text files, and commits.

- [Pre-commit hooks](#pre-commit-hooks) validate and automatically apply code
   formatting rules.

## `pre-commit` hooks

The Dragonfly DB team has agreed to systematically use several pre-commit hooks to
normalize the formatting of code. You need to install and enable pre-commit to have these used
when you do your commits.

## Git workflow

### Repository Setup

After cloning the repository, run the script to set up Git to ignore local changes in specific files:

```bash
chmod +x setup-git-ignore-local.sh
./setup-git-ignore-local.sh
```

To see all available options, run:
```bash
./setup-git-ignore-local.sh --help
```

### Special Handling of Certain Files

The project has files (e.g., `.vscode/launch.json`) that:
- Are included in the repository as default templates
- Local changes to these files will NOT appear in `git status`
- This allows you to make your own configurations that won't be accidentally committed

### If You Need to Commit Changes to Ignored Files

There are two ways to handle this:

#### Option 1: Using the script to unignore all files

1. Unignore all configured files:
   ```bash
   ./setup-git-ignore-local.sh --unignore
   ```

2. Add the file and create a commit as usual:
   ```bash
   git add <file_path>
   git commit -m "fix: Updated configuration"
   ```

3. After committing, re-enable ignoring:
   ```bash
   ./setup-git-ignore-local.sh
   ```

#### Option 2: Manually unignore specific files

1. Disable ignoring for a specific file:
   ```bash
   git update-index --no-assume-unchanged <file_path>
   # For example:
   git update-index --no-assume-unchanged .vscode/launch.json
   ```

2. Add the file and create a commit as usual:
   ```bash
   git add <file_path>
   git commit -m "fix: Updated configuration"
   ```

3. After committing, re-enable ignoring for that file:
   ```bash
   git update-index --assume-unchanged <file_path>
   ```

## Codebase guidelines

This repo conforms to the Google's C++ Style Guide. Keep in mind we use an older version of the
style guide which can be found [here](https://github.com/google/styleguide/blob/505ba68c74eb97e6966f60907ce893001bedc706/cppguide.html).

Any exceptions to the rules specified in the style guide will be documented here.

## License terms for contributions

Please see our [CLA agreement](./CLA.txt)

## THANK YOU FOR YOUR CONTRIBUTIONS
