#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
EMBEDDED_REDIS_HELPER="$SCRIPT_DIR/prepare-embedded-redis.sh"
EMBEDDED_REDIS_VERIFIER="$SCRIPT_DIR/verify-embedded-redis-payload.go"
EMBEDDED_REDIS_OUTPUT="${EMBEDDED_REDIS_OUTPUT:-$BACKEND_DIR/internal/embeddedredis/assets/generated/redis-windows.zip}"
export EMBEDDED_REDIS_OUTPUT

usage() {
  cat <<'USAGE'
Build OpenList release artifacts with the official build.sh pipeline, embedding a
local OpenList-Frontend checkout.

Usage:
  scripts/build-release-local.sh [options]

Options:
  --frontend-dir <path>   Local OpenList-Frontend path.
                          Defaults to ../OpenList-Frontend.
  --backend-mode <mode>   dev | beta | release. Defaults to release.
  --target <name>         windows-amd64 | linux-amd64-musl | darwin-amd64 | amd64 | all
                          Defaults to windows-amd64.
  --lite                  Build lite frontend/backend artifacts.
  --skip-frontend-build   Reuse existing frontend dist/ without rebuilding.
  --skip-i18n             Skip Crowdin i18n download (default for local builds).
  --with-crowdin-i18n     Run Crowdin i18n:release (needs CROWDIN_PROJECT_ID and token).
  --no-install            Skip pnpm install before frontend build.
  -h, --help              Show this help.

Environment:
  FRONTEND_DIR            Same as --frontend-dir.
  FRONTEND_VERSION        Override embedded frontend version metadata.
  GITHUB_TOKEN            Optional, only needed when downloading frontend i18n.

The Windows artifact embeds Redis from pinned upstream packages and requires
network access to:
  - github.com
  - raw.githubusercontent.com
  - repo.msys2.org
  - www.gnu.org

Output:
  build/compress/openlist-windows-amd64.zip          (--target windows-amd64, amd64, or all)
  build/compress/openlist-linux-musl-amd64.tar.gz    (--target linux-amd64-musl)
  build/compress/openlist-darwin-amd64.tar.gz        (--target darwin-amd64)
  or all official release archives when --target all

Requirements:
  - Go 1.26.4
  - Node.js and pnpm
  - curl
  - Info-ZIP zip/unzip
  - tar with zstd support
  - awk
  - sha256sum or shasum
  - windows-amd64, darwin-amd64, and amd64 need zig or xgo + usable Docker.
  - all needs xgo + usable Docker:
      go install github.com/crazy-max/xgo@latest
  - linux-amd64-musl only needs Go + downloaded musl toolchain (no Docker)

Examples:
  scripts/build-release-local.sh \
    --frontend-dir /Users/entertang/Github/OpenList-Frontend \
    --target windows-amd64

  scripts/build-release-local.sh \
    --frontend-dir /Users/entertang/Github/OpenList-Frontend \
    --target linux-amd64-musl \
    --skip-frontend-build
USAGE
}

die() {
  echo "error: $*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "$1 is required but was not found in PATH"
}

command_available() {
  command -v "$1" >/dev/null 2>&1
}

docker_is_usable() {
  docker info >/dev/null 2>&1
}

require_target_toolchain() {
  local target="$1"

  case "$target" in
    linux-amd64-musl)
      return 0
      ;;
    all)
      command_available xgo ||
        die "xgo is required for target all; run: go install github.com/crazy-max/xgo@latest"
      command_available docker ||
        die "Docker is required for target all but was not found in PATH"
      docker_is_usable || die "Docker is required for target all but is not available"
      ;;
    windows-amd64|darwin-amd64|amd64)
      if command_available zig; then
        return 0
      fi
      command_available xgo ||
        die "zig or xgo is required for target $target; install zig or run: go install github.com/crazy-max/xgo@latest"
      command_available docker ||
        die "Docker is required when target $target uses xgo but was not found in PATH"
      docker_is_usable || die "Docker is required when target $target uses xgo but is not available"
      ;;
    *)
      die "unsupported toolchain target: $target"
      ;;
  esac
}

target_requires_windows_amd64() {
  case "$1" in
    windows-amd64|amd64|all) return 0 ;;
    *) return 1 ;;
  esac
}

verify_embedded_redis_generated_dir() (
  set -e

  local output="${1:-$EMBEDDED_REDIS_OUTPUT}"
  local generated_dir
  local output_name
  local entry
  local entry_name
  local entries

  generated_dir="$(dirname "$output")"
  output_name="$(basename "$output")"
  [ -d "$generated_dir" ] && [ ! -L "$generated_dir" ] ||
    die "embedded Redis generated directory is missing or is a symlink: $generated_dir"
  [ -f "$generated_dir/.gitkeep" ] && [ ! -L "$generated_dir/.gitkeep" ] ||
    die "embedded Redis generated .gitkeep must be a regular non-symlink file"
  [ -f "$output" ] && [ ! -L "$output" ] ||
    die "embedded Redis output must be a regular non-symlink file: $output"

  shopt -s dotglob nullglob
  entries=("$generated_dir"/*)
  [ "${#entries[@]}" -eq 2 ] ||
    die "embedded Redis generated directory must contain only .gitkeep and $output_name"
  for entry in "${entries[@]}"; do
    entry_name="${entry##*/}"
    case "$entry_name" in
      .gitkeep|"$output_name") ;;
      *) die "unexpected embedded Redis generated entry: $entry_name" ;;
    esac
  done
)

cleanup_embedded_redis() {
  local original_status=$?
  local cleanup_status=0
  local final_status="$original_status"

  trap - EXIT
  if bash "$EMBEDDED_REDIS_HELPER" clean; then
    cleanup_status=0
  else
    cleanup_status=$?
    echo "error: failed to clean generated embedded Redis payload" >&2
  fi
  if [ "$original_status" -eq 0 ] && [ "$cleanup_status" -ne 0 ]; then
    final_status="$cleanup_status"
  fi
  exit "$final_status"
}

verify_windows_release_archive() {
  local archive="$1"
  local payload="${2:-}"
  local entries
  local listing
  local regular_entry_count

  [ -f "$archive" ] || die "expected Windows release archive was not created: $archive"
  entries="$(unzip -Z1 "$archive")" || die "failed to list Windows release archive: $archive"
  if [ "$entries" != "openlist.exe" ]; then
    die "Windows release archive must contain exactly one entry named openlist.exe: $archive"
  fi

  listing="$(unzip -Z -l "$archive")" || die "failed to inspect Windows release archive: $archive"
  regular_entry_count="$(printf '%s\n' "$listing" | awk '$NF == "openlist.exe" && substr($1, 1, 1) == "-" { count++ } END { print count + 0 }')"
  [ "$regular_entry_count" -eq 1 ] ||
    die "Windows release archive entry openlist.exe is not a regular file: $archive"

  [ -n "$payload" ] || die "embedded Redis payload path is required for Windows release verification"
  [ -f "$payload" ] || die "embedded Redis payload was not found for release verification: $payload"
  [ -f "$EMBEDDED_REDIS_VERIFIER" ] || die "embedded Redis verifier was not found: $EMBEDDED_REDIS_VERIFIER"
  require_cmd go
  go run "$EMBEDDED_REDIS_VERIFIER" "$archive" "$payload" ||
    die "Windows openlist.exe does not contain the exact embedded Redis payload: $archive"
}

require_info_zip_unzip() (
  set -e

  local probe_dir
  local probe_archive
  local entries
  local listing
  local regular_entry_count
  local cleanup

  require_cmd unzip
  require_cmd zip

  probe_dir="$(mktemp -d "${TMPDIR:-/tmp}/openlist-unzip-probe.XXXXXX")"
  printf -v cleanup 'rm -rf %q' "$probe_dir"
  trap "$cleanup" EXIT
  probe_archive="$probe_dir/probe.zip"
  printf 'probe\n' >"$probe_dir/probe.txt"
  (
    cd "$probe_dir"
    zip -X -q "$probe_archive" probe.txt
  )

  if ! entries="$(unzip -Z1 "$probe_archive" 2>/dev/null)" || [ "$entries" != "probe.txt" ]; then
    die "unzip must support Info-ZIP-style -Z1 and -Z -l modes required to verify Windows release archives"
  fi

  if ! listing="$(unzip -Z -l "$probe_archive" 2>/dev/null)"; then
    die "unzip must support Info-ZIP-style -Z1 and -Z -l modes required to verify Windows release archives"
  fi
  regular_entry_count="$(printf '%s\n' "$listing" | awk '$NF == "probe.txt" && substr($1, 1, 1) == "-" { count++ } END { print count + 0 }')"
  [ "$regular_entry_count" -eq 1 ] ||
    die "unzip must support Info-ZIP-style -Z1 and -Z -l modes required to verify Windows release archives"
)

require_windows_embedded_redis_commands() {
  require_cmd curl
  require_cmd zip
  require_cmd tar
  require_cmd awk
  require_info_zip_unzip
  if ! command_available sha256sum && ! command_available shasum; then
    die "sha256sum or shasum is required for Windows embedded Redis builds"
  fi
}

run_backend_build() {
  local target="$1"
  shift

  if target_requires_windows_amd64 "$target"; then
    verify_embedded_redis_generated_dir "$EMBEDDED_REDIS_OUTPUT" || return 1
  fi
  (
    cd "$BACKEND_DIR"
    bash build.sh "$@"
  ) || return 1
  if target_requires_windows_amd64 "$target"; then
    verify_embedded_redis_generated_dir "$EMBEDDED_REDIS_OUTPUT" || return 1
  fi
}

if [[ "${BASH_SOURCE[0]}" != "$0" ]]; then
  return 0
fi

FRONTEND_DIR="${FRONTEND_DIR:-"$BACKEND_DIR/../OpenList-Frontend"}"
BACKEND_MODE="release"
TARGET="windows-amd64"
use_lite_build="false"
SKIP_FRONTEND_BUILD="false"
SKIP_I18N="true"
RUN_PNPM_INSTALL="true"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --frontend-dir)
      [ "$#" -ge 2 ] || die "--frontend-dir requires a value"
      FRONTEND_DIR="$2"
      shift 2
      ;;
    --backend-mode)
      [ "$#" -ge 2 ] || die "--backend-mode requires a value"
      BACKEND_MODE="$2"
      shift 2
      ;;
    --target)
      [ "$#" -ge 2 ] || die "--target requires a value"
      TARGET="$2"
      shift 2
      ;;
    --lite)
      use_lite_build="true"
      shift
      ;;
    --skip-frontend-build)
      SKIP_FRONTEND_BUILD="true"
      shift
      ;;
    --skip-i18n)
      SKIP_I18N="true"
      shift
      ;;
    --with-crowdin-i18n)
      SKIP_I18N="false"
      shift
      ;;
    --no-install)
      RUN_PNPM_INSTALL="false"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown option: $1"
      ;;
  esac
done

case "$BACKEND_MODE" in
  dev|beta|release) ;;
  *) die "invalid backend mode: $BACKEND_MODE" ;;
esac

case "$TARGET" in
  windows-amd64) BUILD_PLATFORM="windows_amd64" ;;
  linux-amd64-musl) BUILD_PLATFORM="linux_amd64_musl" ;;
  darwin-amd64) BUILD_PLATFORM="darwin_amd64" ;;
  amd64) BUILD_PLATFORM="amd64" ;;
  all) BUILD_PLATFORM="" ;;
  *) die "invalid target: $TARGET" ;;
esac

require_target_toolchain "$TARGET"
require_cmd go
require_cmd pnpm
require_cmd node
export GOTOOLCHAIN="${GOTOOLCHAIN:-go1.26.4}"
export GOSUMDB="${GOSUMDB:-sum.golang.org}"

FRONTEND_DIR="$(cd "$FRONTEND_DIR" && pwd)" || die "frontend dir does not exist: $FRONTEND_DIR"
[ -f "$FRONTEND_DIR/build.sh" ] || die "frontend build.sh not found: $FRONTEND_DIR/build.sh"
[ -f "$FRONTEND_DIR/package.json" ] || die "frontend package.json not found: $FRONTEND_DIR"

export FRONTEND_DIR
export FRONTEND_VERSION="${FRONTEND_VERSION:-$(node -p "require('$FRONTEND_DIR/package.json').version")}"

if target_requires_windows_amd64 "$TARGET"; then
  [ -f "$EMBEDDED_REDIS_HELPER" ] || die "embedded Redis helper not found: $EMBEDDED_REDIS_HELPER"
  require_windows_embedded_redis_commands

  trap cleanup_embedded_redis EXIT
  echo "==> Preparing pinned embedded Redis payload"
  bash "$EMBEDDED_REDIS_HELPER" prepare
  verify_embedded_redis_generated_dir "$EMBEDDED_REDIS_OUTPUT"
fi

if [ "$SKIP_FRONTEND_BUILD" != "true" ]; then
  echo "==> Building local frontend in $FRONTEND_DIR"
  if [ "$use_lite_build" = "true" ]; then
    frontend_build_cmd=(pnpm build:lite)
  else
    frontend_build_cmd=(pnpm build)
  fi
  if [ "$SKIP_I18N" = "true" ]; then
    echo "==> Skipping Crowdin i18n (using committed src/lang files)"
    if [ "$RUN_PNPM_INSTALL" = "false" ]; then
      (cd "$FRONTEND_DIR" && "${frontend_build_cmd[@]}")
    else
      (cd "$FRONTEND_DIR" && pnpm install && "${frontend_build_cmd[@]}")
    fi
  else
    frontend_args=(--dev --compress)
    if [ "$use_lite_build" = "true" ]; then
      frontend_args+=(--lite)
    fi
    (cd "$FRONTEND_DIR" && bash ./build.sh "${frontend_args[@]}")
  fi
fi

[ -d "$FRONTEND_DIR/dist" ] || die "frontend dist not found: $FRONTEND_DIR/dist"

echo "==> Embedding frontend $FRONTEND_VERSION from $FRONTEND_DIR/dist"
lite_suffix=""
if [ "$use_lite_build" = "true" ]; then
  lite_suffix=" lite"
fi
echo "==> Running official backend build: bash build.sh $BACKEND_MODE ${BUILD_PLATFORM:+$BUILD_PLATFORM}$lite_suffix"

backend_args=("$BACKEND_MODE")
if [ -n "$BUILD_PLATFORM" ]; then
  backend_args+=("$BUILD_PLATFORM")
fi
if [ "$use_lite_build" = "true" ]; then
  backend_args+=("lite")
fi

run_backend_build "$TARGET" "${backend_args[@]}"

if target_requires_windows_amd64 "$TARGET"; then
  windows_archive="$BACKEND_DIR/build/compress/openlist-windows-amd64.zip"
  if [ "$use_lite_build" = "true" ]; then
    windows_archive="$BACKEND_DIR/build/compress/openlist-windows-amd64-lite.zip"
  fi
  verify_windows_release_archive "$windows_archive" "$EMBEDDED_REDIS_OUTPUT"
fi

echo
echo "==> Done. Release artifacts:"
find "$BACKEND_DIR/build/compress" -maxdepth 1 -type f 2>/dev/null | sort || true
