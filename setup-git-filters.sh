#!/bin/bash

# Setting up Git filter for ignoring local changes in certain files
git config filter.ignorelocalchanges.smudge cat
git config filter.ignorelocalchanges.clean cat
git config filter.ignorelocalchanges.required true

# Setting assume-unchanged flag for files
git update-index --assume-unchanged .vscode/launch.json
git update-index --assume-unchanged .vscode/*.db
git update-index --assume-unchanged .vscode/settings.json

echo "Git filters successfully configured!"
echo "Now files like .vscode/launch.json won't show up in git status when locally modified"
echo "If you want to commit changes to these files, first run:"
echo "git update-index --no-assume-unchanged .vscode/launch.json"
echo "After committing, don't forget to run again:"
echo "git update-index --assume-unchanged .vscode/launch.json"
