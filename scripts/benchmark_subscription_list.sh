#!/usr/bin/env bash
set -euo pipefail

: "${OPENLIST_BASE_URL:?set OPENLIST_BASE_URL, for example http://127.0.0.1:5244}"
: "${OPENLIST_AUTH_TOKEN:?set OPENLIST_AUTH_TOKEN in the calling environment}"

request_count="${OPENLIST_BENCH_REQUESTS:-100}"
concurrency="${OPENLIST_BENCH_CONCURRENCY:-20}"
request_path="${OPENLIST_BENCH_PATH:-/api/admin/subscription/list?page=1&per_page=20}"
p95_limit="${OPENLIST_BENCH_P95_LIMIT:-2.0}"
base_url="${OPENLIST_BASE_URL%/}"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

if ! [[ "$request_count" =~ ^[1-9][0-9]*$ && "$concurrency" =~ ^[1-9][0-9]*$ ]]; then
  echo 'OPENLIST_BENCH_REQUESTS and OPENLIST_BENCH_CONCURRENCY must be positive integers' >&2
  exit 2
fi

export OPENLIST_BASE_URL OPENLIST_AUTH_TOKEN OPENLIST_BENCH_PATH="$request_path" tmp_dir

run_request() {
  local request_id="$1"
  local body_file="$tmp_dir/body-$request_id.json"
  local result_file="$tmp_dir/result-$request_id"
  local result

  if ! result="$(curl --silent --show-error --connect-timeout 5 --max-time 30 \
    -H "Authorization: $OPENLIST_AUTH_TOKEN" \
    -o "$body_file" -w '%{http_code} %{time_total}' \
    "$OPENLIST_BASE_URL$OPENLIST_BENCH_PATH" 2>"$tmp_dir/error-$request_id")"; then
    printf 'curl_error 30\n' >"$result_file"
    return 0
  fi
  if rg -qi 'database is locked|lock timeout|database locked' "$body_file"; then
    printf 'lock_error %s\n' "$result" >"$result_file"
    return 0
  fi
  if ! rg -q '"code"[[:space:]]*:[[:space:]]*200' "$body_file"; then
    printf 'api_error %s\n' "$result" >"$result_file"
    return 0
  fi
  printf '%s\n' "$result" >"$result_file"
}
export -f run_request

seq 1 "$request_count" | xargs -P "$concurrency" -n 1 bash -c 'run_request "$1"' _

results="$tmp_dir/results"
cat "$tmp_dir"/result-* >"$results"
total="$(wc -l <"$results" | tr -d ' ')"
failures="$(awk '$1 != 200 {count++} END {print count+0}' "$results")"
lock_errors="$(awk '$1 == "lock_error" {count++} END {print count+0}' "$results")"
latencies="$tmp_dir/latencies"
awk '$1 == 200 {print $2}' "$results" | sort -n >"$latencies"

if [[ ! -s "$latencies" ]]; then
  echo "no successful responses; failures=$failures/$total" >&2
  exit 1
fi

percentile() {
  local fraction="$1"
  awk -v fraction="$fraction" 'BEGIN {line=1} {values[NR]=$1} END {index=int(NR*fraction+0.999999); if (index < 1) index=1; print values[index]}' "$latencies"
}

echo "requests=$total concurrency=$concurrency failures=$failures lock_errors=$lock_errors"
echo "p50_seconds=$(percentile 0.50)"
p95="$(percentile 0.95)"
echo "p95_seconds=$p95"
echo "p99_seconds=$(percentile 0.99)"

if [[ "$failures" -ne 0 ]]; then
  echo 'benchmark completed with HTTP/API failures' >&2
  exit 1
fi
if ! awk -v value="$p95" -v limit="$p95_limit" 'BEGIN {exit !(value <= limit)}'; then
  echo "p95 latency ${p95}s exceeds ${p95_limit}s" >&2
  exit 1
fi
