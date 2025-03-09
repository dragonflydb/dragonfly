## Git workflow

### Repository Setup

After cloning the repository, run the script to set up Git to ignore local changes in specific files:

```bash
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
