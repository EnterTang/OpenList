#!/usr/bin/env bash
set -euo pipefail

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
  The Windows artifact embeds Redis from a pinned distribution and requires
  network access to GitHub plus curl, unzip, zip, and sha256sum or shasum.

Output:
  build/compress/openlist-windows-amd64.zip          (--target windows-amd64, amd64, or all)
  build/compress/openlist-linux-musl-amd64.tar.gz    (--target linux-amd64-musl)
  build/compress/openlist-darwin-amd64.tar.gz        (--target darwin-amd64)
  or all official release archives when --target all

Requirements:
  - Go 1.26.4
  - Docker + xgo for Windows/macOS targets:
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

target_requires_windows_amd64() {
  case "$1" in
    windows-amd64|amd64|all) return 0 ;;
    *) return 1 ;;
  esac
}

cleanup_embedded_redis() {
  local original_status=$?

  trap - EXIT
  if ! bash "$EMBEDDED_REDIS_HELPER" clean; then
    echo "warning: failed to clean generated embedded Redis payload" >&2
  fi
  exit "$original_status"
}

verify_windows_release_archive() {
  local archive="$1"
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

if [[ "${BASH_SOURCE[0]}" != "$0" ]]; then
  return 0
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
EMBEDDED_REDIS_HELPER="$SCRIPT_DIR/prepare-embedded-redis.sh"

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

if [ "$BUILD_PLATFORM" != "linux_amd64_musl" ] && [ "$BUILD_PLATFORM" != "windows_amd64" ] && [ "$BUILD_PLATFORM" != "darwin_amd64" ] && [ "$BUILD_PLATFORM" != "amd64" ]; then
  require_cmd docker
  command -v xgo >/dev/null 2>&1 || die "xgo is required for target $TARGET; run: go install github.com/crazy-max/xgo@latest"
elif ! command -v zig >/dev/null 2>&1 && ! docker info >/dev/null 2>&1; then
  die "install zig (brew install zig) or start Docker for cross-compilation"
fi
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
  require_cmd curl
  require_cmd zip
  require_info_zip_unzip
  if ! command -v sha256sum >/dev/null 2>&1 && ! command -v shasum >/dev/null 2>&1; then
    die "sha256sum or shasum is required for Windows embedded Redis builds"
  fi

  trap cleanup_embedded_redis EXIT
  echo "==> Preparing pinned embedded Redis payload"
  bash "$EMBEDDED_REDIS_HELPER" prepare
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

(
  cd "$BACKEND_DIR"
  bash build.sh "${backend_args[@]}"
)

if target_requires_windows_amd64 "$TARGET"; then
  windows_archive="$BACKEND_DIR/build/compress/openlist-windows-amd64.zip"
  if [ "$use_lite_build" = "true" ]; then
    windows_archive="$BACKEND_DIR/build/compress/openlist-windows-amd64-lite.zip"
  fi
  verify_windows_release_archive "$windows_archive"
fi

echo
echo "==> Done. Release artifacts:"
find "$BACKEND_DIR/build/compress" -maxdepth 1 -type f 2>/dev/null | sort || true
