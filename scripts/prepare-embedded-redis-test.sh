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

failed_staging_dir="$tmp_dir/failed-staging"
cleanup_fixture="$tmp_dir/cleanup-fixture"
make_fixture "$cleanup_fixture"
mktemp() {
  case "$*" in
    *embedded-redis-payload*)
      mkdir "$failed_staging_dir"
      printf '%s\n' "$failed_staging_dir"
      ;;
    *) return 1 ;;
  esac
}
assemble_without_inherited_exit_trap() (
  trap - EXIT
  assemble_payload "$@"
)
set +e
assemble_without_inherited_exit_trap "$cleanup_fixture" "$tmp_dir/mktemp-failure.zip" >/dev/null 2>&1
assembly_status=$?
set -e
[ "$assembly_status" -ne 0 ] || fail "assemble_payload unexpectedly survived temporary output creation failure"
unset -f assemble_without_inherited_exit_trap
unset -f mktemp
[ ! -e "$failed_staging_dir" ] || fail "assemble_payload leaked staging after temporary output creation failed"
pass "assemble_payload cleans staging when temporary output creation fails"

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

# shellcheck source=build-release-local.sh
source "$LOCAL_RELEASE_SCRIPT"

for target in windows-amd64 amd64 all; do
  target_requires_windows_amd64 "$target" || fail "$target did not route through Windows AMD64 preparation"
done
for target in linux-amd64-musl darwin-amd64; do
  if target_requires_windows_amd64 "$target"; then
    fail "$target incorrectly routed through Windows AMD64 preparation"
  fi
done
pass "local release routing recognizes every target that emits Windows AMD64"

require_info_zip_unzip
pass "local release accepts unzip with required zipinfo modes"

unzip() {
  echo "fake unzip: zipinfo modes are unsupported" >&2
  return 2
}
unzip_probe_error="$tmp_dir/unzip-probe-error.txt"
if require_info_zip_unzip 2>"$unzip_probe_error"; then
  fail "unzip feature probe accepted an implementation without zipinfo modes"
fi
unset -f unzip
grep -F "Info-ZIP-style -Z1 and -Z -l" "$unzip_probe_error" >/dev/null ||
  fail "unzip feature probe did not explain the required modes"
pass "local release rejects unzip implementations without required zipinfo modes"

windows_amd64_files="$(GOOS=windows GOARCH=amd64 go list -f '{{join .GoFiles " "}}' ./internal/embeddedredis)"
case " $windows_amd64_files " in
  *" payload_windows.go "*) ;;
  *) fail "Windows AMD64 does not select the embedded payload implementation" ;;
esac

windows_arm64_files="$(GOOS=windows GOARCH=arm64 go list -f '{{join .GoFiles " "}}' ./internal/embeddedredis)"
case " $windows_arm64_files " in
  *" payload_other.go "*) ;;
  *) fail "Windows ARM64 does not select the unavailable-payload implementation" ;;
esac
case " $windows_arm64_files " in
  *" payload_windows.go "*) fail "Windows ARM64 selected the x64 embedded payload implementation" ;;
esac
pass "embedded Redis payload selection is limited to Windows AMD64"

echo "all prepare-embedded-redis tests passed"
