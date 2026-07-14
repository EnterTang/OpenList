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
  if ("$@") >/dev/null 2>&1; then
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
    msys-ssl-3.dll
  do
    printf 'fixture for %s\n' "$name" >"$fixture_dir/$name"
  done
  printf 'Redis license fixture\n' >"$fixture_dir/COPYING.redis"
  printf 'Redis Windows license fixture\n' >"$fixture_dir/LICENSE.redis-windows"
  printf 'MSYS2 runtime license fixture\n' >"$fixture_dir/LICENSE.msys2-runtime"
  printf 'LGPL fixture\n' >"$fixture_dir/LICENSE.LGPL-3.0"
  printf 'OpenSSL license fixture\n' >"$fixture_dir/LICENSE.openssl"
  printf 'Third-party notices fixture\n' >"$fixture_dir/THIRD_PARTY_NOTICES.txt"
}

tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/prepare-embedded-redis-test.XXXXXX")"
trap 'rm -rf "$tmp_dir"' EXIT

[ -f "$HELPER" ] || fail "helper script does not exist: $HELPER"

release_output_probe="$tmp_dir/release-output-probe.sh"
cat >"$release_output_probe" <<'PROBE'
#!/usr/bin/env bash
set -eu
source "$1"
printf '%s\n' "$EMBEDDED_REDIS_OUTPUT"
/bin/bash -u -c 'printf "%s\n" "$EMBEDDED_REDIS_OUTPUT"'
PROBE
expected_release_output="$(cd "$SCRIPT_DIR/.." && pwd)/internal/embeddedredis/assets/generated/redis-windows.zip"
release_output_probe_result="$(env -u EMBEDDED_REDIS_OUTPUT /bin/bash "$release_output_probe" "$LOCAL_RELEASE_SCRIPT")"
expected_release_output_probe_result="$(printf '%s\n%s' "$expected_release_output" "$expected_release_output")"
[ "$release_output_probe_result" = "$expected_release_output_probe_result" ] ||
  fail "local release did not define and export the default embedded Redis output in a clean shell"
pass "local release defines and exports one default embedded Redis output before sourcing helpers"

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
LICENSE.LGPL-3.0
LICENSE.msys2-runtime
LICENSE.openssl
LICENSE.redis-windows
THIRD_PARTY_NOTICES.txt
msys-2.0.dll
msys-crypto-3.dll
msys-ssl-3.dll
redis-server.exe
FILES
unzip -Z1 "$first_zip" | LC_ALL=C sort >"$actual_list"
cmp -s "$expected_list" "$actual_list" || fail "payload file list did not match the expected licensed runtime files"

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

dash_named_clean_dir="$tmp_dir/dash-named-clean"
mkdir -p "$dash_named_clean_dir"
printf 'generated payload\n' >"$dash_named_clean_dir/-redis-windows.zip"
if ! (
  cd "$dash_named_clean_dir"
  clean_payload "-redis-windows.zip"
  [ ! -e "./-redis-windows.zip" ]
); then
  fail "clean_payload did not safely remove a relative output beginning with a dash"
fi
pass "clean_payload handles relative output names beginning with a dash"

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
helper_usage="$(bash "$HELPER" --help)"
case "$release_usage" in
  *"embeds Redis"*) ;;
  *) fail "local release usage does not disclose embedded Redis" ;;
esac
case "$release_usage" in
  *"network access"*) ;;
  *) fail "local release usage does not disclose the Windows network requirement" ;;
esac
for required_help_text in \
  "Node.js" \
  "pnpm" \
  "curl" \
  "Info-ZIP zip/unzip" \
  "tar with zstd support" \
  "awk" \
  "sha256sum or shasum" \
  "github.com" \
  "raw.githubusercontent.com" \
  "repo.msys2.org" \
  "www.gnu.org"
do
  case "$release_usage" in
    *"$required_help_text"*) ;;
    *) fail "local release --help does not mention $required_help_text" ;;
  esac
  case "$helper_usage" in
    *"$required_help_text"*) ;;
    *) fail "embedded Redis helper --help does not mention $required_help_text" ;;
  esac
done
pass "both embedded Redis release help texts disclose tools and network hosts"

# shellcheck source=build-release-local.sh
source "$LOCAL_RELEASE_SCRIPT"

declare -F require_windows_embedded_redis_commands >/dev/null ||
  fail "local release does not expose Windows embedded Redis command preflight"
windows_preflight_probe="$tmp_dir/windows-preflight-commands.txt"
(
  require_cmd() {
    printf '%s\n' "$1" >>"$windows_preflight_probe"
  }
  require_info_zip_unzip() {
    printf '%s\n' "Info-ZIP zip/unzip" >>"$windows_preflight_probe"
  }
  command_available() {
    [ "$1" = "sha256sum" ]
  }
  require_windows_embedded_redis_commands
)
for required_command in curl zip tar awk "Info-ZIP zip/unzip"; do
  grep -Fx -- "$required_command" "$windows_preflight_probe" >/dev/null ||
    fail "local Windows release preflight did not require $required_command"
done
pass "local Windows release preflight requires archive and text-processing tools"

audited_generated_dir="$tmp_dir/audited-generated"
mkdir -p "$audited_generated_dir"
printf '\n' >"$audited_generated_dir/.gitkeep"
cp "$first_zip" "$audited_generated_dir/redis-windows.zip"
declare -F verify_embedded_redis_generated_dir >/dev/null ||
  fail "local release does not expose generated-directory verification"
verify_embedded_redis_generated_dir "$audited_generated_dir/redis-windows.zip"

printf 'unexpected\n' >"$audited_generated_dir/sibling.txt"
assert_fails verify_embedded_redis_generated_dir "$audited_generated_dir/redis-windows.zip"
rm -f -- "$audited_generated_dir/sibling.txt"

ln -s missing-target "$audited_generated_dir/unexpected-link"
assert_fails verify_embedded_redis_generated_dir "$audited_generated_dir/redis-windows.zip"
rm -f -- "$audited_generated_dir/unexpected-link"

rm -f -- "$audited_generated_dir/.gitkeep"
ln -s "$first_zip" "$audited_generated_dir/.gitkeep"
assert_fails verify_embedded_redis_generated_dir "$audited_generated_dir/redis-windows.zip"
rm -f -- "$audited_generated_dir/.gitkeep"
printf '\n' >"$audited_generated_dir/.gitkeep"

rm -f -- "$audited_generated_dir/redis-windows.zip"
ln -s "$first_zip" "$audited_generated_dir/redis-windows.zip"
assert_fails verify_embedded_redis_generated_dir "$audited_generated_dir/redis-windows.zip"
pass "local release accepts only regular .gitkeep and redis-windows.zip entries in generated"

declare -F run_backend_build >/dev/null ||
  fail "local release does not expose the audited backend build boundary"
backend_audit_dir="$tmp_dir/backend-audit-generated"
mkdir -p "$backend_audit_dir" "$tmp_dir/fake-backend"
printf '\n' >"$backend_audit_dir/.gitkeep"
cp "$first_zip" "$backend_audit_dir/redis-windows.zip"

run_backend_build_probe() (
  local behavior="$1"
  local build_marker="$2"

  BACKEND_DIR="$tmp_dir/fake-backend"
  EMBEDDED_REDIS_OUTPUT="$backend_audit_dir/redis-windows.zip"
  bash() {
    printf 'called\n' >"$build_marker"
    if [ "$behavior" = "add-sibling" ]; then
      printf 'unexpected\n' >"$backend_audit_dir/build-sibling.txt"
    fi
  }
  run_backend_build windows-amd64 release windows_amd64
)

pre_build_marker="$tmp_dir/pre-build-called"
printf 'unexpected\n' >"$backend_audit_dir/pre-build-sibling.txt"
assert_fails run_backend_build_probe preserve "$pre_build_marker"
[ ! -e "$pre_build_marker" ] || fail "backend build ran before the immediate generated-directory audit"
rm -f -- "$backend_audit_dir/pre-build-sibling.txt"

valid_build_marker="$tmp_dir/valid-build-called"
run_backend_build_probe preserve "$valid_build_marker"
[ -f "$valid_build_marker" ] || fail "audited backend build did not run with a valid generated directory"

post_build_marker="$tmp_dir/post-build-called"
assert_fails run_backend_build_probe add-sibling "$post_build_marker"
[ -f "$post_build_marker" ] || fail "backend build did not run before the post-build audit"
[ -f "$backend_audit_dir/build-sibling.txt" ] || fail "backend build probe did not create its persistent sibling"
rm -f -- "$backend_audit_dir/build-sibling.txt"
pass "local release audits generated immediately before and after the backend build"

failing_clean_helper="$tmp_dir/failing-clean-helper.sh"
cat >"$failing_clean_helper" <<'HELPER'
#!/usr/bin/env bash
exit 23
HELPER
chmod +x "$failing_clean_helper"
cleanup_error="$tmp_dir/cleanup-error.txt"
set +e
(
  trap - EXIT
  EMBEDDED_REDIS_HELPER="$failing_clean_helper"
  cleanup_embedded_redis
) 2>"$cleanup_error"
cleanup_status=$?
set -e
[ "$cleanup_status" -ne 0 ] || fail "local release ignored embedded Redis cleanup failure after success"
grep -F "failed to clean generated embedded Redis payload" "$cleanup_error" >/dev/null ||
  fail "local release cleanup failure did not explain the error"
pass "local release fails when post-build embedded Redis cleanup fails"

embedded_payload="$tmp_dir/embedded-payload.zip"
printf 'embedded Redis payload fixture %s\000with binary data\n' "$tmp_dir" >"$embedded_payload"

amd64_pe="$tmp_dir/verifier-windows-amd64.exe"
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -buildvcs=false -o "$amd64_pe" "$EMBEDDED_REDIS_VERIFIER"

embedded_once_dir="$tmp_dir/embedded-once"
mkdir -p "$embedded_once_dir"
cp "$amd64_pe" "$embedded_once_dir/openlist.exe"
cat "$embedded_payload" >>"$embedded_once_dir/openlist.exe"
embedded_archive="$tmp_dir/embedded-once.zip"
(
  cd "$embedded_once_dir"
  zip -X -q "$embedded_archive" openlist.exe
)
verify_windows_release_archive "$embedded_archive" "$embedded_payload"

dll_patcher="$tmp_dir/mark-pe-as-dll.go"
cat >"$dll_patcher" <<'GO'
package main

import (
	"encoding/binary"
	"fmt"
	"os"
)

func main() {
	if len(os.Args) != 3 {
		panic("usage: mark-pe-as-dll <input> <output>")
	}
	data, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	if len(data) < 0x40 || data[0] != 'M' || data[1] != 'Z' {
		panic("input is missing its DOS header")
	}
	peOffset := int(binary.LittleEndian.Uint32(data[0x3c:0x40]))
	characteristicsOffset := peOffset + 4 + 18
	if characteristicsOffset+2 > len(data) {
		panic(fmt.Sprintf("COFF characteristics offset %d is out of range", characteristicsOffset))
	}
	characteristics := binary.LittleEndian.Uint16(data[characteristicsOffset : characteristicsOffset+2])
	binary.LittleEndian.PutUint16(data[characteristicsOffset:characteristicsOffset+2], characteristics|0x2000)
	if err := os.WriteFile(os.Args[2], data, 0644); err != nil {
		panic(err)
	}
}
GO
dll_pe_dir="$tmp_dir/dll-pe"
mkdir -p "$dll_pe_dir"
go run "$dll_patcher" "$amd64_pe" "$dll_pe_dir/openlist.exe"
cat "$embedded_payload" >>"$dll_pe_dir/openlist.exe"
dll_pe_archive="$tmp_dir/dll-pe.zip"
(
  cd "$dll_pe_dir"
  zip -X -q "$dll_pe_archive" openlist.exe
)
assert_fails verify_windows_release_archive "$dll_pe_archive" "$embedded_payload"

embedded_twice_dir="$tmp_dir/embedded-twice"
mkdir -p "$embedded_twice_dir"
cp "$amd64_pe" "$embedded_twice_dir/openlist.exe"
cat "$embedded_payload" "$embedded_payload" >>"$embedded_twice_dir/openlist.exe"
embedded_twice_archive="$tmp_dir/embedded-twice.zip"
(
  cd "$embedded_twice_dir"
  zip -X -q "$embedded_twice_archive" openlist.exe
)
assert_fails verify_windows_release_archive "$embedded_twice_archive" "$embedded_payload"

fake_pe_dir="$tmp_dir/fake-pe"
mkdir -p "$fake_pe_dir"
printf 'MZ is not enough to make a valid PE\n' >"$fake_pe_dir/openlist.exe"
cat "$embedded_payload" >>"$fake_pe_dir/openlist.exe"
fake_pe_archive="$tmp_dir/fake-pe.zip"
(
  cd "$fake_pe_dir"
  zip -X -q "$fake_pe_archive" openlist.exe
)
assert_fails verify_windows_release_archive "$fake_pe_archive" "$embedded_payload"

coff_object_dir="$tmp_dir/coff-object"
mkdir -p "$coff_object_dir"
{
  printf '\x64\x86'                 # Machine: AMD64.
  printf '\x00\x00'                 # NumberOfSections: 0.
  printf '\x00\x00\x00\x00'       # TimeDateStamp: 0.
  printf '\x00\x00\x00\x00'       # PointerToSymbolTable: 0.
  printf '\x00\x00\x00\x00'       # NumberOfSymbols: 0.
  printf '\x00\x00'                 # SizeOfOptionalHeader: 0.
  printf '\x00\x00'                 # Characteristics: 0.
} >"$coff_object_dir/openlist.exe"
[ "$(wc -c <"$coff_object_dir/openlist.exe" | tr -d '[:space:]')" = "20" ] ||
  fail "minimal AMD64 COFF fixture is not exactly 20 bytes"
cat "$embedded_payload" >>"$coff_object_dir/openlist.exe"
coff_object_archive="$tmp_dir/coff-object.zip"
(
  cd "$coff_object_dir"
  zip -X -q "$coff_object_archive" openlist.exe
)
assert_fails verify_windows_release_archive "$coff_object_archive" "$embedded_payload"

arm64_pe_dir="$tmp_dir/arm64-pe"
mkdir -p "$arm64_pe_dir"
CGO_ENABLED=0 GOOS=windows GOARCH=arm64 go build -buildvcs=false -o "$arm64_pe_dir/openlist.exe" "$EMBEDDED_REDIS_VERIFIER"
cat "$embedded_payload" >>"$arm64_pe_dir/openlist.exe"
arm64_pe_archive="$tmp_dir/arm64-pe.zip"
(
  cd "$arm64_pe_dir"
  zip -X -q "$arm64_pe_archive" openlist.exe
)
assert_fails verify_windows_release_archive "$arm64_pe_archive" "$embedded_payload"

missing_payload="$tmp_dir/missing-payload.zip"
printf 'different Redis payload fixture\n' >"$missing_payload"
assert_fails verify_windows_release_archive "$embedded_archive" "$missing_payload"

sparse_payload_helper="$tmp_dir/create-sparse-payload.go"
cat >"$sparse_payload_helper" <<'GO'
package main

import "os"

func main() {
	if len(os.Args) != 2 {
		panic("usage: create-sparse-payload <output>")
	}
	file, err := os.Create(os.Args[1])
	if err != nil {
		panic(err)
	}
	if err := file.Truncate((128 << 20) + 1); err != nil {
		file.Close()
		panic(err)
	}
	if err := file.Close(); err != nil {
		panic(err)
	}
}
GO
oversized_payload="$tmp_dir/oversized-payload.zip"
go run "$sparse_payload_helper" "$oversized_payload"
oversized_payload_error="$tmp_dir/oversized-payload-error.txt"
set +e
go run "$EMBEDDED_REDIS_VERIFIER" "$embedded_archive" "$oversized_payload" > /dev/null 2>"$oversized_payload_error"
oversized_payload_status=$?
set -e
[ "$oversized_payload_status" -ne 0 ] || fail "verifier accepted an oversized Redis payload"
grep -F "Redis payload exceeds 134217728-byte verification limit" "$oversized_payload_error" >/dev/null ||
  fail "verifier did not report the Redis payload size limit before scanning openlist.exe"

pass "local release accepts one payload in AMD64 PE and rejects DLL, duplicates, fake PE, COFF, ARM64, and mismatches"
pass "release verifier bounds the Redis payload before scanning openlist.exe"

for target in windows-amd64 amd64 all; do
  target_requires_windows_amd64 "$target" || fail "$target did not route through Windows AMD64 preparation"
done
for target in linux-amd64-musl darwin-amd64; do
  if target_requires_windows_amd64 "$target"; then
    fail "$target incorrectly routed through Windows AMD64 preparation"
  fi
done
pass "local release routing recognizes every target that emits Windows AMD64"

declare -F require_target_toolchain >/dev/null ||
  fail "local release does not expose target toolchain preflight"

run_target_toolchain_preflight() (
  local target="$1"
  local available_tools="$2"
  local docker_state="$3"
  local docker_marker="${4:-}"

  command_available() {
    case " $available_tools " in
      *" $1 "*) return 0 ;;
      *) return 1 ;;
    esac
  }
  docker_is_usable() {
    if [ -n "$docker_marker" ]; then
      printf 'called\n' >"$docker_marker"
    fi
    [ "$docker_state" = "up" ]
  }

  require_target_toolchain "$target"
)

linux_docker_marker="$tmp_dir/linux-docker-called"
run_target_toolchain_preflight linux-amd64-musl "" down "$linux_docker_marker"
[ ! -e "$linux_docker_marker" ] || fail "linux-amd64-musl preflight called Docker"

for target in windows-amd64 darwin-amd64 amd64; do
  zig_docker_marker="$tmp_dir/$target-zig-docker-called"
  run_target_toolchain_preflight "$target" "zig" down "$zig_docker_marker"
  [ ! -e "$zig_docker_marker" ] || fail "$target preflight called Docker even though zig was available"

  xgo_docker_marker="$tmp_dir/$target-xgo-docker-called"
  run_target_toolchain_preflight "$target" "xgo docker" up "$xgo_docker_marker"
  [ -f "$xgo_docker_marker" ] || fail "$target preflight did not verify Docker for xgo"
done

docker_only_marker="$tmp_dir/docker-only-called"
assert_fails run_target_toolchain_preflight windows-amd64 "docker" up "$docker_only_marker"
[ ! -e "$docker_only_marker" ] || fail "Windows preflight consulted Docker after detecting missing xgo"

missing_docker_marker="$tmp_dir/missing-docker-called"
assert_fails run_target_toolchain_preflight windows-amd64 "xgo" up "$missing_docker_marker"
[ ! -e "$missing_docker_marker" ] || fail "Windows preflight invoked a missing Docker command"

all_zig_marker="$tmp_dir/all-zig-docker-called"
assert_fails run_target_toolchain_preflight all "zig" up "$all_zig_marker"
[ ! -e "$all_zig_marker" ] || fail "all preflight consulted Docker without xgo"

all_docker_marker="$tmp_dir/all-docker-called"
run_target_toolchain_preflight all "xgo docker" up "$all_docker_marker"
[ -f "$all_docker_marker" ] || fail "all preflight did not verify Docker"
assert_fails run_target_toolchain_preflight all "xgo docker" down "$tmp_dir/all-docker-down-called"
pass "target toolchain preflight distinguishes Linux, zig, xgo with Docker, and full all builds"

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
