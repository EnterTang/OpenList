#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

REDIS_VERSION="7.2.14"
REDIS_ARCHIVE_URL="https://github.com/redis-windows/redis-windows/releases/download/${REDIS_VERSION}/Redis-${REDIS_VERSION}-Windows-x64-msys2.zip"
REDIS_ARCHIVE_SHA256="b31d0f867608017f0b0962624d55a4c569a745587ad4b08f7fe9eea59d6916c1"
REDIS_COPYING_URL="https://raw.githubusercontent.com/redis/redis/${REDIS_VERSION}/COPYING"
REDIS_COPYING_SHA256="97f0a15b7bbae580d2609dad2e11f1956ae167be296ab60f4691ab9c30ee9828"
REDIS_WINDOWS_LICENSE_URL="https://raw.githubusercontent.com/redis-windows/redis-windows/${REDIS_VERSION}/LICENSE"
REDIS_WINDOWS_LICENSE_SHA256="c71d239df91726fc519c6eb72d318ec65820627232b2f796219e87dcf35d0ab4"
MSYS2_RUNTIME_VERSION="3.6.9-1"
MSYS2_RUNTIME_PACKAGE_URL="https://repo.msys2.org/msys/x86_64/msys2-runtime-${MSYS2_RUNTIME_VERSION}-x86_64.pkg.tar.zst"
MSYS2_RUNTIME_PACKAGE_SHA256="b7cf3a7a423b78c2fb6752859075381b7fa52ffad2d8d66778f49c04dcf50312"
MSYS2_RUNTIME_SOURCE_URL="https://repo.msys2.org/msys/sources/msys2-runtime-${MSYS2_RUNTIME_VERSION}.src.tar.zst"
MSYS2_RUNTIME_LICENSE_SHA256="794433752103cf4bbb4a84a1bdb8fbc150abb1762704bb35fecc9f7f820be984"
LGPL3_LICENSE_URL="https://www.gnu.org/licenses/lgpl-3.0.txt"
LGPL3_LICENSE_SHA256="e3a994d82e644b03a792a930f574002658412f62407f5fee083f2555c5f23118"
OPENSSL_VERSION="3.6.2-1"
OPENSSL_LICENSE_URL="https://raw.githubusercontent.com/openssl/openssl/openssl-3.6.2/LICENSE.txt"
OPENSSL_LICENSE_SHA256="7d5450cb2d142651b8afa315b5f238efc805dad827d91ba367d8516bc9d49e7a"
OPENSSL_SOURCE_URL="https://repo.msys2.org/msys/sources/openssl-${OPENSSL_VERSION}.src.tar.zst"
OPENSSL_SOURCE_SHA256="c4f11272a8f4a101b9d704cc8ac71ae03d9ffc33785bfd676a25b5576e5a21ee"
REDIS_ARCHIVE_ROOT="Redis-${REDIS_VERSION}-Windows-x64-msys2"
EMBEDDED_REDIS_OUTPUT="${EMBEDDED_REDIS_OUTPUT:-$REPOSITORY_DIR/internal/embeddedredis/assets/generated/redis-windows.zip}"

PAYLOAD_FILES=(
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
)

REDIS_BINARY_FILES=(
  redis-server.exe
  msys-2.0.dll
  msys-crypto-3.dll
  msys-ssl-3.dll
)

usage() {
  cat <<'USAGE'
Prepare the pinned Redis for Windows payload embedded by OpenList releases.

Usage:
  scripts/prepare-embedded-redis.sh prepare
  scripts/prepare-embedded-redis.sh clean

Commands:
  prepare  Download, verify, and assemble the generated Redis payload.
  clean    Remove only the generated Redis payload archive.

Requirements:
  - curl
  - Info-ZIP zip/unzip
  - tar with zstd support
  - awk
  - sha256sum or shasum
  - Node.js and pnpm are additionally required by the full local release.

Network access:
  - github.com
  - raw.githubusercontent.com
  - repo.msys2.org
  - www.gnu.org
USAGE
}

die() {
  echo "error: $*" >&2
  return 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "$1 is required but was not found in PATH"
}

require_prepare_commands() {
  require_cmd curl
  require_cmd unzip
  require_cmd zip
  require_cmd tar
  require_cmd awk
  if ! command -v sha256sum >/dev/null 2>&1 && ! command -v shasum >/dev/null 2>&1; then
    die "sha256sum or shasum is required but neither was found in PATH"
  fi
}

sha256_file() {
  local file="$1"

  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$file" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$file" | awk '{print $1}'
  else
    die "sha256sum or shasum is required but neither was found in PATH"
  fi
}

verify_sha256() {
  local file="$1"
  local expected="$2"
  local actual

  [ -f "$file" ] || die "cannot verify missing file: $file"
  actual="$(sha256_file "$file")"
  if [ "$actual" != "$expected" ]; then
    die "SHA256 mismatch for $file: expected $expected, got $actual"
  fi
}

is_payload_name() {
  local candidate="$1"
  local expected

  for expected in "${PAYLOAD_FILES[@]}"; do
    if [ "$candidate" = "$expected" ]; then
      return 0
    fi
  done
  return 1
}

validate_payload_source() {
  local source_dir="$1"
  local name
  local path

  [ -d "$source_dir" ] || die "payload source directory does not exist: $source_dir"

  for name in "${PAYLOAD_FILES[@]}"; do
    path="$source_dir/$name"
    if [ ! -f "$path" ] || [ -L "$path" ] || [ ! -s "$path" ]; then
      die "required payload file is missing, empty, or not a regular file: $name"
      return 1
    fi
  done

  for path in "$source_dir"/* "$source_dir"/.[!.]* "$source_dir"/..?*; do
    if [ ! -e "$path" ] && [ ! -L "$path" ]; then
      continue
    fi
    name="${path##*/}"
    if ! is_payload_name "$name"; then
      die "unexpected payload source entry: $name"
      return 1
    fi
  done
}

assemble_payload() (
  set -e

  local source_dir="$1"
  local output="$2"
  local output_dir
  local staging_dir
  local temporary_output
  local name

  validate_payload_source "$source_dir" || return 1

  mkdir -p "$(dirname "$output")"
  output_dir="$(cd "$(dirname "$output")" && pwd)"
  output="$output_dir/$(basename "$output")"
  staging_dir="$(mktemp -d "${TMPDIR:-/tmp}/embedded-redis-payload.XXXXXX")"
  trap "rm -rf -- $(printf '%q' "$staging_dir")" EXIT
  temporary_output="$(mktemp "$output.tmp.XXXXXX")"
  trap "rm -rf -- $(printf '%q' "$staging_dir"); rm -f -- $(printf '%q' "$temporary_output")" EXIT
  rm -f "$temporary_output"

  for name in "${PAYLOAD_FILES[@]}"; do
    cp "$source_dir/$name" "$staging_dir/$name"
    chmod 0644 "$staging_dir/$name"
    TZ=UTC touch -t 200001010000 "$staging_dir/$name"
  done

  (
    cd "$staging_dir"
    TZ=UTC zip -X -q "$temporary_output" "${PAYLOAD_FILES[@]}"
  )

  mv -f "$temporary_output" "$output"
)

download_file() {
  local url="$1"
  local output="$2"

  curl -fL \
    --retry 5 \
    --retry-delay 2 \
    --connect-timeout 15 \
    --speed-limit 1024 \
    --speed-time 30 \
    --max-time 300 \
    --continue-at - \
    --output "$output" \
    "$url"
}

prepare_payload() (
  set -e

  local output="${1:-$EMBEDDED_REDIS_OUTPUT}"
  local work_dir
  local archive
  local msys2_runtime_package
  local source_dir
  local name
  local archive_path

  require_prepare_commands
  work_dir="$(mktemp -d "${TMPDIR:-/tmp}/prepare-embedded-redis.XXXXXX")"
  trap 'rm -rf "$work_dir"' EXIT
  archive="$work_dir/redis-windows.zip"
  msys2_runtime_package="$work_dir/msys2-runtime.pkg.tar.zst"
  source_dir="$work_dir/payload"
  mkdir -p "$source_dir"

  echo "==> Downloading Redis for Windows ${REDIS_VERSION}"
  download_file "$REDIS_ARCHIVE_URL" "$archive"
  verify_sha256 "$archive" "$REDIS_ARCHIVE_SHA256"

  for name in "${REDIS_BINARY_FILES[@]}"; do
    archive_path="$REDIS_ARCHIVE_ROOT/$name"
    unzip -j -q "$archive" "$archive_path" -d "$source_dir"
  done

  download_file "$REDIS_COPYING_URL" "$source_dir/COPYING.redis"
  verify_sha256 "$source_dir/COPYING.redis" "$REDIS_COPYING_SHA256"
  download_file "$REDIS_WINDOWS_LICENSE_URL" "$source_dir/LICENSE.redis-windows"
  verify_sha256 "$source_dir/LICENSE.redis-windows" "$REDIS_WINDOWS_LICENSE_SHA256"

  download_file "$MSYS2_RUNTIME_PACKAGE_URL" "$msys2_runtime_package"
  verify_sha256 "$msys2_runtime_package" "$MSYS2_RUNTIME_PACKAGE_SHA256"
  tar -xOf "$msys2_runtime_package" usr/share/doc/Cygwin/CYGWIN_LICENSE >"$source_dir/LICENSE.msys2-runtime"
  verify_sha256 "$source_dir/LICENSE.msys2-runtime" "$MSYS2_RUNTIME_LICENSE_SHA256"
  download_file "$LGPL3_LICENSE_URL" "$source_dir/LICENSE.LGPL-3.0"
  verify_sha256 "$source_dir/LICENSE.LGPL-3.0" "$LGPL3_LICENSE_SHA256"
  download_file "$OPENSSL_LICENSE_URL" "$source_dir/LICENSE.openssl"
  verify_sha256 "$source_dir/LICENSE.openssl" "$OPENSSL_LICENSE_SHA256"

  cat >"$source_dir/THIRD_PARTY_NOTICES.txt" <<NOTICES
OpenList embedded Redis third-party notices

Redis ${REDIS_VERSION}
  Component: redis-server.exe
  License: Redis BSD-3-Clause terms in COPYING.redis
  Source: https://github.com/redis/redis/tree/${REDIS_VERSION}

Redis for Windows ${REDIS_VERSION}
  Component: Windows/MSYS2 Redis port
  License: Apache-2.0 terms in LICENSE.redis-windows
  Source: https://github.com/redis-windows/redis-windows/tree/${REDIS_VERSION}

MSYS2 runtime ${MSYS2_RUNTIME_VERSION}
  Component: msys-2.0.dll
  License: LGPL-3.0-or-later with the Cygwin linking exception; see
           LICENSE.LGPL-3.0 and LICENSE.msys2-runtime
  Binary package: ${MSYS2_RUNTIME_PACKAGE_URL}
  Binary SHA-256: ${MSYS2_RUNTIME_PACKAGE_SHA256}
  Corresponding source package: ${MSYS2_RUNTIME_SOURCE_URL}

OpenSSL ${OPENSSL_VERSION}
  Components: msys-crypto-3.dll, msys-ssl-3.dll
  License: Apache-2.0 terms in LICENSE.openssl
  Corresponding source package: ${OPENSSL_SOURCE_URL}
  Source SHA-256: ${OPENSSL_SOURCE_SHA256}

Distributors must keep the corresponding source packages available for the
period and by the method required by the applicable licenses. The fixed URLs
above identify the exact MSYS2 package recipes and upstream sources used by
this payload; mirror them with any public binary distribution.
NOTICES

  assemble_payload "$source_dir" "$output"
  echo "==> Prepared embedded Redis payload: $output"
  echo "    SHA256: $(sha256_file "$output")"
)

clean_payload() {
  local output="${1:-$EMBEDDED_REDIS_OUTPUT}"

  rm -f -- "$output"
}

main() {
  if [ "$#" -ne 1 ]; then
    usage >&2
    return 2
  fi

  case "$1" in
    prepare) prepare_payload ;;
    clean) clean_payload ;;
    -h|--help) usage ;;
    *)
      usage >&2
      die "unknown command: $1"
      return 2
      ;;
  esac
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
