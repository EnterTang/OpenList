#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HELPER="$SCRIPT_DIR/prepare-embedded-redis.sh"
LOCAL_RELEASE_SCRIPT="$SCRIPT_DIR/build-release-local.sh"

fail() {
  echo "not ok - $*" >&2
  exit 1
}

pass() {
  echo "ok - $*"
}

assert_fails() {
  if "$@" >/dev/null 2>&1; then
    fail "command unexpectedly succeeded: $*"
  fi
}

make_fixture() {
  local fixture_dir="$1"
  local name

  mkdir -p "$fixture_dir"
  for name in \
    redis-server.exe \
    msys-2.0.dll \
    msys-crypto-3.dll \
    msys-gcc_s-seh-1.dll \
    msys-ssl-3.dll \
    msys-stdc++-6.dll
  do
    printf 'fixture for %s\n' "$name" >"$fixture_dir/$name"
  done
  printf 'Redis license fixture\n' >"$fixture_dir/COPYING.redis"
  printf 'Redis Windows license fixture\n' >"$fixture_dir/LICENSE.redis-windows"
}

tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/prepare-embedded-redis-test.XXXXXX")"
trap 'rm -rf "$tmp_dir"' EXIT

[ -f "$HELPER" ] || fail "helper script does not exist: $HELPER"

source_output="$tmp_dir/source-output.zip"
source_log="$(EMBEDDED_REDIS_OUTPUT="$source_output" bash -c 'source "$1"' _ "$HELPER")"
[ -z "$source_log" ] || fail "sourcing helper produced output: $source_log"
[ ! -e "$source_output" ] || fail "sourcing helper executed main"
pass "helper can be sourced without executing main"

# shellcheck source=prepare-embedded-redis.sh
source "$HELPER"

digest_fixture="$tmp_dir/digest.txt"
printf 'checksum fixture\n' >"$digest_fixture"
correct_digest="$(sha256_file "$digest_fixture")"
verify_sha256 "$digest_fixture" "$correct_digest"
assert_fails verify_sha256 "$digest_fixture" "0000000000000000000000000000000000000000000000000000000000000000"
pass "verify_sha256 accepts matching content and rejects mismatches"

missing_fixture="$tmp_dir/missing"
make_fixture "$missing_fixture"
rm "$missing_fixture/msys-ssl-3.dll"
assert_fails assemble_payload "$missing_fixture" "$tmp_dir/missing.zip"
pass "assemble_payload rejects a missing required file"

unexpected_fixture="$tmp_dir/unexpected"
make_fixture "$unexpected_fixture"
printf 'unexpected\n' >"$unexpected_fixture/redis-cli.exe"
assert_fails assemble_payload "$unexpected_fixture" "$tmp_dir/unexpected.zip"
pass "assemble_payload rejects unexpected files"

valid_fixture="$tmp_dir/valid"
make_fixture "$valid_fixture"
first_zip="$tmp_dir/first.zip"
second_zip="$tmp_dir/second.zip"
assemble_payload "$valid_fixture" "$first_zip"
assemble_payload "$valid_fixture" "$second_zip"

expected_list="$tmp_dir/expected-list.txt"
actual_list="$tmp_dir/actual-list.txt"
cat >"$expected_list" <<'FILES'
COPYING.redis
LICENSE.redis-windows
msys-2.0.dll
msys-crypto-3.dll
msys-gcc_s-seh-1.dll
msys-ssl-3.dll
msys-stdc++-6.dll
redis-server.exe
FILES
unzip -Z1 "$first_zip" | LC_ALL=C sort >"$actual_list"
cmp -s "$expected_list" "$actual_list" || fail "payload file list did not match expected eight files"

while IFS= read -r name; do
  case "$name" in
    */*) fail "payload entry is not flat: $name" ;;
  esac
  [ -n "$(unzip -p "$first_zip" "$name")" ] || fail "payload entry is empty: $name"
done <"$actual_list"

[ "$(sha256_file "$first_zip")" = "$(sha256_file "$second_zip")" ] || fail "payload assembly is not deterministic"
pass "assemble_payload creates an exact, flat, nonempty, deterministic payload"

generated_dir="$tmp_dir/generated"
mkdir -p "$generated_dir"
printf '\n' >"$generated_dir/.gitkeep"
cp "$first_zip" "$generated_dir/redis-windows.zip"
clean_payload "$generated_dir/redis-windows.zip"
[ ! -e "$generated_dir/redis-windows.zip" ] || fail "clean_payload did not remove generated archive"
[ -f "$generated_dir/.gitkeep" ] || fail "clean_payload removed .gitkeep"
pass "clean_payload removes only the generated archive"

curl_args="$tmp_dir/curl-args.txt"
curl() {
  printf '%s\n' "$@" >"$curl_args"
}
download_file "https://example.invalid/fixture.zip" "$tmp_dir/download.zip"
grep -Fx -- '--continue-at' "$curl_args" >/dev/null || fail "download_file does not enable resumable downloads"
grep -Fx -- '--speed-time' "$curl_args" >/dev/null || fail "download_file does not bound low-speed stalls"
unset -f curl
pass "download_file enables resumable retries and a low-speed timeout"

release_usage="$(bash "$LOCAL_RELEASE_SCRIPT" --help)"
case "$release_usage" in
  *"embeds Redis"*) ;;
  *) fail "local release usage does not disclose embedded Redis" ;;
esac
case "$release_usage" in
  *"network access"*) ;;
  *) fail "local release usage does not disclose the Windows network requirement" ;;
esac
pass "local Windows release usage discloses Redis embedding and network access"

echo "all prepare-embedded-redis tests passed"
