#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Build the customized OpenList frontend, embed it into the backend, build a Docker
image, and push it to DockerHub.

Usage:
  scripts/build-and-push-dockerhub.sh <dockerhub-user/repo[:tag]> [options]

Options:
  --frontend-dir <path>    Path to OpenList-Frontend. Defaults to ../OpenList-Frontend.
  --platforms <list>       Buildx platforms. Defaults to linux/amd64,linux/arm64.
  --base-image-tag <tag>   OpenList base image tag: base, aria2, ffmpeg, aio. Defaults to base.
  --builder <name>         Docker buildx builder name. Defaults to openlist-etf-builder.
  --no-install             Skip pnpm install before frontend build.
  --skip-i18n              Skip fetching/generating frontend i18n assets before frontend build.
  --no-push                Build locally without pushing. Only supports a single platform.
  --no-cache               Pass --no-cache to docker buildx build.
  -h, --help               Show this help.

Environment:
  DOCKERHUB_USERNAME       Optional username for docker login.
  DOCKERHUB_TOKEN          Optional access token/password for docker login.
  GO_VERSION               Go Docker image version. Defaults to 1.26.4.
  BASE_IMAGE               Runtime base image. Defaults to openlistteam/openlist-base-image.
  INSTALL_ARIA2            Runtime aria2 switch passed to image build. Defaults to false.
  VERSION                  Backend version metadata. Defaults to latest git tag or dev.
  WEB_VERSION              Frontend version metadata. Defaults to frontend package version.
  GIT_COMMIT               Commit metadata. Defaults to current short git commit.
  FRONTEND_REPO            Frontend release repo. Defaults to OpenListTeam/OpenList-Frontend.
  FRONTEND_I18N_TAG        Frontend release tag used for i18n.tar.gz. Defaults to latest release.
  FRONTEND_I18N_URL        Override i18n.tar.gz download URL.
  GO_BUILD_PARALLELISM     Go package build parallelism inside Docker. Defaults to 1.
  GO_MAX_PROCS             GOMAXPROCS for Go compiler processes inside Docker. Defaults to 1.

Examples:
  scripts/build-and-push-dockerhub.sh tangente/openlist-etf:latest
  DOCKERHUB_USERNAME=tangente DOCKERHUB_TOKEN=xxx \
    scripts/build-and-push-dockerhub.sh tangente/openlist-etf:v1
  scripts/build-and-push-dockerhub.sh tangente/openlist-etf:latest \
    --platforms linux/amd64 --base-image-tag ffmpeg
USAGE
}

die() {
  echo "error: $*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "$1 is required but was not found in PATH"
}

install_pnpm() {
  local version="${1:-}"

  if command -v pnpm >/dev/null 2>&1; then
    return 0
  fi

  echo "==> pnpm not found, installing"

  if command -v corepack >/dev/null 2>&1; then
    corepack enable >/dev/null 2>&1 || true
    if [ -n "$version" ]; then
      corepack prepare "pnpm@$version" --activate
    else
      corepack prepare pnpm@latest --activate
    fi
    command -v pnpm >/dev/null 2>&1 && return 0
  fi

  if command -v npm >/dev/null 2>&1; then
    if [ -n "$version" ]; then
      npm install -g "pnpm@$version"
    else
      npm install -g pnpm
    fi
    command -v pnpm >/dev/null 2>&1 && return 0
  fi

  echo "==> Falling back to standalone pnpm installer"
  if ! (ldconfig -p 2>/dev/null | grep -q 'libatomic\.so\.1') \
    && ! ls /usr/lib/*/libatomic.so.1 >/dev/null 2>&1 \
    && ! ls /lib/*/libatomic.so.1 >/dev/null 2>&1; then
    die "libatomic.so.1 is required for standalone pnpm; install it with: apt install -y libatomic1 (or use: npm install -g pnpm@${version:-latest})"
  fi

  curl -fsSL https://get.pnpm.io/install.sh | sh -
  export PATH="$HOME/.local/share/pnpm:$PATH"
  command -v pnpm >/dev/null 2>&1 || die "failed to install pnpm; try: npm install -g pnpm@${version:-latest}"
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

IMAGE="${DOCKER_IMAGE:-}"
FRONTEND_DIR="${FRONTEND_DIR:-"$BACKEND_DIR/../OpenList-Frontend"}"
PLATFORMS="${PLATFORMS:-linux/amd64,linux/arm64}"
BASE_IMAGE_TAG="${BASE_IMAGE_TAG:-base}"
BASE_IMAGE="${BASE_IMAGE:-openlistteam/openlist-base-image}"
BUILDER_NAME="${BUILDER_NAME:-openlist-etf-builder}"
GO_VERSION="${GO_VERSION:-1.26.4}"
BUILD_TMP_ROOT="${BUILD_TMP_ROOT:-"$BACKEND_DIR/.tmp-build"}"
INSTALL_ARIA2="${INSTALL_ARIA2:-false}"
FRONTEND_REPO="${FRONTEND_REPO:-OpenListTeam/OpenList-Frontend}"
GO_BUILD_PARALLELISM="${GO_BUILD_PARALLELISM:-1}"
GO_MAX_PROCS="${GO_MAX_PROCS:-1}"
RUN_PNPM_INSTALL="true"
FETCH_I18N="true"
PUSH="true"
NO_CACHE="false"

if [ "$#" -gt 0 ] && [[ "$1" != -* ]]; then
  IMAGE="$1"
  shift
fi

while [ "$#" -gt 0 ]; do
  case "$1" in
    --frontend-dir)
      [ "$#" -ge 2 ] || die "--frontend-dir requires a value"
      FRONTEND_DIR="$2"
      shift 2
      ;;
    --platforms)
      [ "$#" -ge 2 ] || die "--platforms requires a value"
      PLATFORMS="$2"
      shift 2
      ;;
    --base-image-tag)
      [ "$#" -ge 2 ] || die "--base-image-tag requires a value"
      BASE_IMAGE_TAG="$2"
      shift 2
      ;;
    --builder)
      [ "$#" -ge 2 ] || die "--builder requires a value"
      BUILDER_NAME="$2"
      shift 2
      ;;
    --no-install)
      RUN_PNPM_INSTALL="false"
      shift
      ;;
    --skip-i18n)
      FETCH_I18N="false"
      shift
      ;;
    --no-push)
      PUSH="false"
      shift
      ;;
    --no-cache)
      NO_CACHE="true"
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

[ -n "$IMAGE" ] || { usage; die "Docker image is required, for example tangente/openlist-etf:latest"; }
[[ "$IMAGE" == */* ]] || die "DockerHub image should include a namespace, for example tangente/openlist-etf:latest"

require_cmd docker
require_cmd rsync
require_cmd curl
require_cmd node

docker buildx version >/dev/null 2>&1 || die "docker buildx is required"

FRONTEND_DIR="$(cd "$FRONTEND_DIR" && pwd)" || die "frontend dir does not exist: $FRONTEND_DIR"
[ -f "$FRONTEND_DIR/package.json" ] || die "frontend package.json not found: $FRONTEND_DIR"
mkdir -p "$BUILD_TMP_ROOT"

FRONTEND_PACKAGE_MANAGER="$(cd "$FRONTEND_DIR" && node -p "require('./package.json').packageManager || ''" 2>/dev/null || true)"
REQUIRED_PNPM_VERSION=""
if [[ "$FRONTEND_PACKAGE_MANAGER" == pnpm@* ]]; then
  REQUIRED_PNPM_VERSION="${FRONTEND_PACKAGE_MANAGER#pnpm@}"
fi
install_pnpm "$REQUIRED_PNPM_VERSION"

# Determine pnpm command - try PATH first, then use full path if just installed
if command -v pnpm >/dev/null 2>&1; then
  FRONTEND_PNPM=(pnpm)
else
  FRONTEND_PNPM=("$HOME/.local/share/pnpm/pnpm")
fi
if [[ "$FRONTEND_PACKAGE_MANAGER" == pnpm@* ]]; then
  REQUIRED_PNPM_VERSION="${FRONTEND_PACKAGE_MANAGER#pnpm@}"
  CURRENT_PNPM_VERSION="$(pnpm --version 2>/dev/null || true)"
  if [ "$CURRENT_PNPM_VERSION" != "$REQUIRED_PNPM_VERSION" ]; then
    echo "==> Using frontend package manager pnpm@$REQUIRED_PNPM_VERSION (current pnpm: ${CURRENT_PNPM_VERSION:-not found})"
    FRONTEND_PNPM=(pnpm dlx "pnpm@$REQUIRED_PNPM_VERSION")
  fi
fi

run_frontend_pnpm() {
  (cd "$FRONTEND_DIR" && "${FRONTEND_PNPM[@]}" "$@")
}

VERSION="${VERSION:-$(cd "$BACKEND_DIR" && git describe --abbrev=0 --tags 2>/dev/null || echo dev)}"
WEB_VERSION="${WEB_VERSION:-$(cd "$FRONTEND_DIR" && node -p "require('./package.json').version" 2>/dev/null || echo custom)}"
GIT_COMMIT="${GIT_COMMIT:-$(cd "$BACKEND_DIR" && git rev-parse --short HEAD 2>/dev/null || echo unknown)}"
BUILT_AT="${BUILT_AT:-$(date -u +'%Y-%m-%dT%H:%M:%SZ')}"

resolve_frontend_i18n_tag() {
  if [ -n "${FRONTEND_I18N_TAG:-}" ]; then
    printf '%s\n' "$FRONTEND_I18N_TAG"
    return
  fi

  latest_url="$(curl -fsSLI --max-time 20 -o /dev/null -w '%{url_effective}' \
    "https://github.com/$FRONTEND_REPO/releases/latest" 2>/dev/null || true)"
  latest_tag="${latest_url##*/}"
  if [ -n "$latest_tag" ] && [ "$latest_tag" != "latest" ]; then
    printf '%s\n' "$latest_tag"
  else
    printf '%s\n' "rolling"
  fi
}

resolve_frontend_i18n_url() {
  local tag="$1"
  local assets_html asset_href

  if [ -n "${FRONTEND_I18N_URL:-}" ]; then
    printf '%s\n' "$FRONTEND_I18N_URL"
    return
  fi

  assets_html="$(curl -fsSL --max-time 20 \
    "https://github.com/$FRONTEND_REPO/releases/expanded_assets/$tag" 2>/dev/null || true)"
  [ -n "$assets_html" ] || return 1

  asset_href="$(printf '%s' "$assets_html" |
    grep -o 'href="[^"]*i18n\.tar\.gz"' |
    head -n 1 |
    sed 's/^href="//;s/"$//')"
  [ -n "$asset_href" ] || return 1

  case "$asset_href" in
    http*) printf '%s\n' "$asset_href" ;;
    *) printf '%s\n' "https://github.com$asset_href" ;;
  esac
}

prepare_frontend_i18n() {
  local tag url tmp_i18n

  if [ "$FETCH_I18N" != "true" ]; then
    echo "==> Skipping frontend i18n preparation"
    return
  fi

  [ -f "$FRONTEND_DIR/scripts/i18n.mjs" ] || die "frontend i18n script not found: $FRONTEND_DIR/scripts/i18n.mjs"

  tag="$(resolve_frontend_i18n_tag)"
  url="$(resolve_frontend_i18n_url "$tag")" || true

  if [ -n "$url" ]; then
    tmp_i18n="$(mktemp "${TMPDIR:-/tmp}/openlist-i18n.XXXXXX.tar.gz")"
    echo "==> Fetching frontend i18n assets from $FRONTEND_REPO@$tag"
    if curl -fsSL --retry 3 "$url" -o "$tmp_i18n" && tar -xzf "$tmp_i18n" -C "$FRONTEND_DIR/src/lang"; then
      rm -f "$tmp_i18n"
    else
      rm -f "$tmp_i18n"
      echo "warning: failed to fetch frontend i18n.tar.gz for $FRONTEND_REPO@$tag; using local i18n generation" >&2
    fi
  else
    echo "warning: failed to resolve frontend i18n.tar.gz for $FRONTEND_REPO@$tag; using local i18n generation" >&2
  fi

  echo "==> Preparing frontend i18n entries"
  (cd "$FRONTEND_DIR" && node scripts/i18n.mjs)
}

echo "==> Backend:  $BACKEND_DIR"
echo "==> Frontend: $FRONTEND_DIR"
echo "==> Image:    $IMAGE"
echo "==> Platforms: $PLATFORMS"
echo "==> Version:  $VERSION / frontend $WEB_VERSION / commit $GIT_COMMIT"

if [ "$RUN_PNPM_INSTALL" = "true" ]; then
  echo "==> Installing frontend dependencies"
  CI=true run_frontend_pnpm install --frozen-lockfile
fi

prepare_frontend_i18n

echo "==> Building frontend"
run_frontend_pnpm run build

TMP_DOCKERFILE=""
BUILD_CONTEXT=""

cleanup() {
  [ -z "$TMP_DOCKERFILE" ] || rm -f "$TMP_DOCKERFILE"
  [ -z "$BUILD_CONTEXT" ] || rm -rf "$BUILD_CONTEXT"
}
trap cleanup EXIT

TMP_DOCKERFILE="$(mktemp "$BUILD_TMP_ROOT/openlist-dockerfile.XXXXXX")"
BUILD_CONTEXT="$(mktemp -d "$BUILD_TMP_ROOT/openlist-context.XXXXXX")"

echo "==> Preparing temporary Docker build context"
rsync -a --delete \
  --exclude='/.git' \
  --exclude='/.github' \
  --exclude='/.idea' \
  --exclude='/.omx' \
  --exclude='/.tmp-build' \
  --exclude='/.vscode' \
  --exclude='/bin' \
  --exclude='/build' \
  --exclude='/data' \
  --exclude='/daemon' \
  --exclude='/dist' \
  --exclude='/log' \
  --exclude='/tmp' \
  --exclude='/openlist' \
  --exclude='/openlist-*' \
  --exclude='/public/dist' \
  --exclude='*.db' \
  --exclude='*.test' \
  --exclude='*.out' \
  --exclude='node_modules' \
  "$BACKEND_DIR/" "$BUILD_CONTEXT/"

mkdir -p "$BUILD_CONTEXT/public/dist"
cp -R "$FRONTEND_DIR/dist/." "$BUILD_CONTEXT/public/dist/"

cat > "$TMP_DOCKERFILE" <<'DOCKERFILE'
# syntax=docker/dockerfile:1.7
ARG GO_VERSION=1.26.4
ARG BASE_IMAGE=openlistteam/openlist-base-image
ARG BASE_IMAGE_TAG=base

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS builder
WORKDIR /src
RUN apk add --no-cache ca-certificates tzdata
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS
ARG TARGETARCH
ARG BUILT_AT=unknown
ARG GIT_COMMIT=unknown
ARG VERSION=dev
ARG WEB_VERSION=custom
ARG GO_BUILD_PARALLELISM=1
ARG GO_MAX_PROCS=1
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} GOMAXPROCS=${GO_MAX_PROCS} \
    go build -p=${GO_BUILD_PARALLELISM} -tags=jsoniter \
    -ldflags="-s -w \
    -X github.com/OpenListTeam/OpenList/v4/internal/conf.BuiltAt=${BUILT_AT} \
    -X github.com/OpenListTeam/OpenList/v4/internal/conf.GitCommit=${GIT_COMMIT} \
    -X github.com/OpenListTeam/OpenList/v4/internal/conf.Version=${VERSION} \
    -X github.com/OpenListTeam/OpenList/v4/internal/conf.WebVersion=${WEB_VERSION}" \
    -o /out/openlist .

FROM ${BASE_IMAGE}:${BASE_IMAGE_TAG}
LABEL maintainer="OpenList"
ARG INSTALL_ARIA2=false
ARG USER=openlist
ARG UID=1001
ARG GID=1001

WORKDIR /opt/openlist/

RUN addgroup -g ${GID} ${USER} && \
    adduser -D -u ${UID} -G ${USER} ${USER} && \
    mkdir -p /opt/openlist/data

COPY --from=builder --chmod=755 --chown=${UID}:${GID} /out/openlist ./
COPY --chmod=755 --chown=${UID}:${GID} entrypoint.sh /entrypoint.sh

USER ${USER}
RUN /entrypoint.sh version

ENV UMASK=022 RUN_ARIA2=${INSTALL_ARIA2}
VOLUME /opt/openlist/data/
EXPOSE 5244 5245
CMD [ "/entrypoint.sh" ]
DOCKERFILE

if [ -n "${DOCKERHUB_USERNAME:-}" ] && [ -n "${DOCKERHUB_TOKEN:-}" ]; then
  echo "==> Logging in to DockerHub as $DOCKERHUB_USERNAME"
  printf '%s' "$DOCKERHUB_TOKEN" | docker login -u "$DOCKERHUB_USERNAME" --password-stdin
fi

if ! docker buildx inspect "$BUILDER_NAME" >/dev/null 2>&1; then
  echo "==> Creating buildx builder: $BUILDER_NAME"
  docker buildx create --name "$BUILDER_NAME" --use >/dev/null
else
  docker buildx use "$BUILDER_NAME"
fi
docker buildx inspect --bootstrap >/dev/null

docker_platform_tag() {
  local image="$1"
  local platform="$2"
  local suffix
  suffix="${platform//\//-}"
  suffix="${suffix//,/-}"

  if [[ "${image##*/}" == *:* ]]; then
    printf '%s:%s-%s\n' "${image%:*}" "${image##*:}" "$suffix"
  else
    printf '%s:%s\n' "$image" "$suffix"
  fi
}

BASE_BUILD_ARGS=(
  buildx build
  --file "$TMP_DOCKERFILE"
  --build-arg "GO_VERSION=$GO_VERSION"
  --build-arg "BASE_IMAGE=$BASE_IMAGE"
  --build-arg "BASE_IMAGE_TAG=$BASE_IMAGE_TAG"
  --build-arg "INSTALL_ARIA2=$INSTALL_ARIA2"
  --build-arg "BUILT_AT=$BUILT_AT"
  --build-arg "GIT_COMMIT=$GIT_COMMIT"
  --build-arg "VERSION=$VERSION"
  --build-arg "WEB_VERSION=$WEB_VERSION"
  --build-arg "GO_BUILD_PARALLELISM=$GO_BUILD_PARALLELISM"
  --build-arg "GO_MAX_PROCS=$GO_MAX_PROCS"
)

if [ "$NO_CACHE" = "true" ]; then
  BASE_BUILD_ARGS+=(--no-cache)
fi

if [ "$PUSH" = "true" ]; then
  if [[ "$PLATFORMS" == *,* ]]; then
    IFS=',' read -r -a PLATFORM_LIST <<< "$PLATFORMS"
    PLATFORM_TAGS=()
    for platform in "${PLATFORM_LIST[@]}"; do
      platform="${platform//[[:space:]]/}"
      [ -n "$platform" ] || continue
      platform_tag="$(docker_platform_tag "$IMAGE" "$platform")"
      PLATFORM_TAGS+=("$platform_tag")
      echo "==> Building and pushing Docker image for $platform as $platform_tag"
      docker "${BASE_BUILD_ARGS[@]}" \
        --tag "$platform_tag" \
        --platform "$platform" \
        --push \
        "$BUILD_CONTEXT"
    done
    [ "${#PLATFORM_TAGS[@]}" -gt 0 ] || die "no valid platform found in --platforms $PLATFORMS"
    echo "==> Creating multi-platform manifest: $IMAGE"
    docker buildx imagetools create -t "$IMAGE" "${PLATFORM_TAGS[@]}"
  else
    echo "==> Building and pushing Docker image"
    docker "${BASE_BUILD_ARGS[@]}" \
      --tag "$IMAGE" \
      --platform "$PLATFORMS" \
      --push \
      "$BUILD_CONTEXT"
  fi
else
  [[ "$PLATFORMS" != *,* ]] || die "--no-push can only be used with one platform, for example --platforms linux/amd64"
  echo "==> Building Docker image"
  docker "${BASE_BUILD_ARGS[@]}" \
    --tag "$IMAGE" \
    --platform "$PLATFORMS" \
    --load \
    "$BUILD_CONTEXT"
fi

echo "==> Done"
if [ "$PUSH" = "true" ]; then
  echo "Pushed: $IMAGE"
else
  echo "Built locally: $IMAGE"
fi
echo
echo "Deploy with:"
echo "  OPENLIST_IMAGE=$IMAGE docker compose -f deploy/docker-compose.dockerhub.yml up -d"
