#!/bin/bash
# =============================================================================
# clean_codeql_chain.sh - Nuclear Cleanup for GitHub CodeQL Analyses
#
# PURPOSE:
#   Permanently deletes ALL historical CodeQL analysis runs and associated alerts
#   (open, closed, dismissed) from your repository's Security → Code scanning tab.
#   This is useful when you have thousands of noisy/false-positive alerts from
#   third-party deps, clang-opt, build artifacts, etc., and want a clean slate
#   after adding SARIF filters.
#
# WHY THIS SCRIPT:
#   GitHub only allows deleting the MOST RECENT analysis in each group (ref + tool + category).
#   Deleting one promotes the previous one → this script follows the chain using
#   'confirm_delete_url' until the entire group is wiped.
#
# USAGE:
#   1. Make sure you're authenticated: gh auth login (HTTPS recommended)
#   2. chmod +x clean_codeql_chain.sh
#   3. ./clean_codeql_chain.sh
#
# SAFETY:
#   - Only affects YOUR repo (glevkovich/dragonfly by default)
#   - Irreversible: deleted analyses/alerts history cannot be recovered
#   - After running: re-trigger your CodeQL workflow → only real alerts (with your filter) will appear
#
# Last tested: Works as of Feb 2026 with GitHub CLI v2.XX
# =============================================================================


set -euo pipefail

REPO="glevkovich/dragonfly"
PER_PAGE=100

echo "Starting chained cleanup of CodeQL analyses for $REPO..."

# Fetch all analyses, sorted newest first
analyses_json=$(gh api "/repos/$REPO/code-scanning/analyses?per_page=$PER_PAGE" --jq '.')

if [ -z "$analyses_json" ] || [ "$analyses_json" = "[]" ]; then
  echo "No analyses found. Security tab should be clean."
  exit 0
fi

deleted_count=0

# Process each potentially deletable analysis
echo "$analyses_json" | jq -r '.[] | select(.deletable == true) | .id' | while read -r id; do
  echo "Starting chain deletion from deletable analysis ID: $id"

  current_url="/repos/$REPO/code-scanning/analyses/$id?confirm_delete=true"  # Start with confirm to nuke all

  while true; do
    echo "  Deleting at: $current_url"

    response=$(gh api -X DELETE "$current_url" --jq '.' 2>&1 || true)

    if echo "$response" | grep -q "Analysis specified is not deletable" || echo "$response" | grep -q "400"; then
      echo "  → Skipped/Stopped: Not deletable or error"
      break
    fi

    if echo "$response" | grep -q "deleted" || [ -z "$response" ]; then
      echo "  → Deleted successfully!"
      ((deleted_count++))
    else
      echo "  → Response: $response"
    fi

    # Parse next confirm_delete_url from response (if any)
    next_confirm=$(echo "$response" | jq -r '.confirm_delete_url // empty' 2>/dev/null)
    if [ -z "$next_confirm" ] || [ "$next_confirm" = "null" ]; then
      echo "  → Chain complete (no more confirm_delete_url)"
      break
    fi

    # Extract path from full URL (e.g., /repos/.../analyses/123?confirm_delete)
    current_url="${next_confirm#https://api.github.com}"
    sleep 0.8
  done

  sleep 1  # Pause between groups
done

echo "Cleanup finished! Deleted at least $deleted_count analyses."
echo "Verify remaining:"
echo "  gh api \"/repos/$REPO/code-scanning/analyses?per_page=10\""
echo "Refresh https://github.com/$REPO/security/code-scanning"
echo "Then re-run your workflow for a clean scan (only intentional bug should show)."
