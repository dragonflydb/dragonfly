#!/usr/bin/env bash

# This file is not standalone - source it from the builder action
# Do not set any shell options - assume callers own shell options such as set -euo pipefail.

actions_cache_deps_validate_suffix() {
  local suffix="$1"

  if [[ -z "${suffix//[[:space:]]/}" || "$suffix" == "default" ]]; then
    echo "::error title=deps-cache::enable-deps-cache is 'true' but cache-key-suffix is empty or 'default'. Pass a specific cache-key-suffix (e.g. the container image) so deps caches don't collide across configs."
    return 1
  fi
}

actions_cache_deps_set_key() {
  local suffix="$1"
  local deps_cache_hash="$2"
  local runner_os="$3"
  local runner_arch="$4"
  local build_type="$5"
  local cxx_compiler="$6"
  local sanitizers="$7"
  local cxx_flags="$8"
  local with_aws="$9"
  local build_dir="${10}"
  local cxx_flags_hash

  actions_cache_deps_validate_suffix "$suffix" || return
  if [[ -z "$deps_cache_hash" ]]; then
    echo "::error title=deps-cache::hashFiles() matched no files - refusing to cache under a degenerate key. Check the file list in this step."
    return 1
  fi
  cxx_flags_hash=$(printf '%s' "$cxx_flags" | sha256sum | cut -d ' ' -f1)
  {
    echo "key=dfly-deps-${suffix}-${runner_os}-${runner_arch}-${build_type}-${cxx_compiler}-${sanitizers}-${cxx_flags_hash}-${with_aws}-${deps_cache_hash}"
    echo 'paths<<EOF'
    echo "${build_dir}/third_party"
    echo "${build_dir}/_deps"
    echo "${build_dir}/.ninja_log"
    echo "${build_dir}/CMakeFiles/*_project-complete"
    echo 'EOF'
    echo "manifest_path=${build_dir}/.deps-cache-manifest"
  } >> "$GITHUB_OUTPUT"
}

actions_cache_deps_expand_paths() {
  local paths="$1"
  local -n output_paths="$2"
  local cache_path
  local -a cache_paths
  local -a matched_paths

  shopt -s nullglob
  mapfile -t cache_paths <<< "$paths"
  output_paths=()
  for cache_path in "${cache_paths[@]}"; do
    mapfile -t matched_paths < <(compgen -G "$cache_path" | sort)
    output_paths+=("${matched_paths[@]}")
  done
}

actions_cache_deps_validate_restore() {
  local workspace="$1"
  local manifest_path="$2"
  local paths="$3"
  local cache_key="$4"
  local manifest_tool
  local validation_status=0
  local -a expanded_cache_paths

  manifest_tool="$(dirname "${BASH_SOURCE[0]}")/deps_cache_manifest.py"
  cd "$workspace" || return
  actions_cache_deps_expand_paths "$paths" expanded_cache_paths
  python3 "$manifest_tool" validate \
    --root "$workspace" --manifest "$manifest_path" \
    "${expanded_cache_paths[@]}" || validation_status=$?
  case "$validation_status" in
    0)
      echo "deps_cache_is_valid=true" >> "$GITHUB_OUTPUT"
      ;;
    1 | 3)
      echo "::warning title=deps-cache::Invalid or unreadable restored dependency cache; discarding local restore, attempting remote deletion, and rebuilding dependencies"
      echo "Discarding restored paths for cache key: $cache_key"
      rm -rf "${expanded_cache_paths[@]}" "$manifest_path"
      echo "deps_cache_is_valid=false" >> "$GITHUB_OUTPUT"
      ;;
    *)
      echo "::error title=deps-cache::Manifest validation failed with infrastructure error $validation_status"
      return "$validation_status"
      ;;
  esac
}

actions_cache_deps_delete_invalid_remote() {
  local cache_key="$1"
  local ref="$2"
  local event_name="$3"
  local base_ref="$4"
  local repository="$5"
  local cache_ref
  local deleted=false
  local -a cache_refs=("$ref")

  if ! command -v curl >/dev/null 2>&1; then
    echo "::warning title=deps-cache::'curl' not found; remote cache remains until reaped"
    return
  fi
  if [[ "$event_name" == "pull_request" ]]; then
    cache_refs+=("refs/heads/$base_ref")
  fi
  for cache_ref in "${cache_refs[@]}"; do
    if curl --fail --silent --show-error --output /dev/null \
      --request DELETE \
      --header "Accept: application/vnd.github+json" \
      --header "Authorization: Bearer ${GH_TOKEN}" \
      --header "X-GitHub-Api-Version: 2022-11-28" \
      --get \
      --data-urlencode "key=$cache_key" \
      --data-urlencode "ref=$cache_ref" \
      "https://api.github.com/repos/${repository}/actions/caches"; then
      echo "Removed remote dependency cache: $cache_key (ref: $cache_ref)"
      deleted=true
      break
    fi
  done
  if [[ "$deleted" == false ]]; then
    echo "::warning title=deps-cache::Could not delete remote cache; it may require 'actions: write' permission"
  fi
}

actions_cache_deps_report_status() {
  local cache_hit="$1"
  local is_valid="$2"
  local key="$3"
  local status

  if [[ "$cache_hit" != "true" ]]; then
    status="MISS - no matching cache found, full cold third-party build expected"
  elif [[ "$is_valid" == "true" ]]; then
    status="VERIFIED HIT (exact key match) - manifest, third_party/_deps, and Ninja build log restored"
  elif [[ -z "$is_valid" ]]; then
    status="UNKNOWN - exact cache hit was not validated"
  else
    status="INVALID HIT - restored cache failed manifest validation; rebuilding dependencies locally"
  fi
  echo "Third-party deps cache: $status"
  echo "  primary key: $key"
}

actions_cache_deps_generate_manifest() {
  local workspace="$1"
  local manifest_path="$2"
  local paths="$3"
  local manifest_tool
  local -a expanded_cache_paths

  manifest_tool="$(dirname "${BASH_SOURCE[0]}")/deps_cache_manifest.py"
  cd "$workspace" || return
  actions_cache_deps_expand_paths "$paths" expanded_cache_paths
  python3 "$manifest_tool" generate \
    --root "$workspace" --manifest "$manifest_path" \
    "${expanded_cache_paths[@]}"
}
