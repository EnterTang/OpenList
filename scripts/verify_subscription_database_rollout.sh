#!/usr/bin/env bash
set -euo pipefail

: "${OPENLIST_BASE_URL:?set OPENLIST_BASE_URL, for example http://127.0.0.1:5244}"
: "${OPENLIST_AUTH_TOKEN:?set OPENLIST_AUTH_TOKEN in the calling environment}"

base_url="${OPENLIST_BASE_URL%/}"
token="${OPENLIST_AUTH_TOKEN}"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

request() {
  local name="$1"
  local path="$2"
  local output="$tmp_dir/$name.json"
  curl --fail-with-body --silent --show-error \
    --connect-timeout 5 --max-time 30 \
    -H "Authorization: $token" \
    -o "$output" \
    -w "$name http=%{http_code} total=%{time_total}s size=%{size_download}\n" \
    "$base_url$path"
  if ! rg -q '"code"[[:space:]]*:[[:space:]]*200' "$output"; then
    echo "$name returned an unexpected API response" >&2
    sed -n '1,5p' "$output" >&2
    return 1
  fi
}

request subscription_list '/api/admin/subscription/list?page=1&per_page=20'
request subscription_config '/api/admin/subscription/config'

echo 'subscription database rollout smoke checks passed'
