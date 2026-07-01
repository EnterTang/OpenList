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
  --no-push                Build locally without pushing. Only supports a single platform.
  --no-cache               Pass --no-cache to docker buildx build.
  -h, --help               Show this help.

Environment:
  DOCKERHUB_USERNAME       Optional username for docker login.
  DOCKERHUB_TOKEN          Optional access token/password for docker login.
  GO_VERSION               Go Docker image version. Defaults to 1.24.
  BASE_IMAGE               Runtime base image. Defaults to openlistteam/openlist-base-image.
  INSTALL_ARIA2            Runtime aria2 switch passed to image build. Defaults to false.
  VERSION                  Backend version metadata. Defaults to latest git tag or dev.
  WEB_VERSION              Frontend version metadata. Defaults to frontend package version.
  GIT_COMMIT               Commit metadata. Defaults to current short git commit.

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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

IMAGE="${DOCKER_IMAGE:-}"
FRONTEND_DIR="${FRONTEND_DIR:-"$BACKEND_DIR/../OpenList-Frontend"}"
PLATFORMS="${PLATFORMS:-linux/amd64,linux/arm64}"
BASE_IMAGE_TAG="${BASE_IMAGE_TAG:-base}"
BASE_IMAGE="${BASE_IMAGE:-openlistteam/openlist-base-image}"
BUILDER_NAME="${BUILDER_NAME:-openlist-etf-builder}"
GO_VERSION="${GO_VERSION:-1.24}"
INSTALL_ARIA2="${INSTALL_ARIA2:-false}"
RUN_PNPM_INSTALL="true"
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
require_cmd pnpm
require_cmd rsync

docker buildx version >/dev/null 2>&1 || die "docker buildx is required"

FRONTEND_DIR="$(cd "$FRONTEND_DIR" && pwd)" || die "frontend dir does not exist: $FRONTEND_DIR"
[ -f "$FRONTEND_DIR/package.json" ] || die "frontend package.json not found: $FRONTEND_DIR"

VERSION="${VERSION:-$(cd "$BACKEND_DIR" && git describe --abbrev=0 --tags 2>/dev/null || echo dev)}"
WEB_VERSION="${WEB_VERSION:-$(cd "$FRONTEND_DIR" && node -p "require('./package.json').version" 2>/dev/null || echo custom)}"
GIT_COMMIT="${GIT_COMMIT:-$(cd "$BACKEND_DIR" && git rev-parse --short HEAD 2>/dev/null || echo unknown)}"
BUILT_AT="${BUILT_AT:-$(date -u +'%Y-%m-%dT%H:%M:%SZ')}"

echo "==> Backend:  $BACKEND_DIR"
echo "==> Frontend: $FRONTEND_DIR"
echo "==> Image:    $IMAGE"
echo "==> Platforms: $PLATFORMS"
echo "==> Version:  $VERSION / frontend $WEB_VERSION / commit $GIT_COMMIT"

if [ "$RUN_PNPM_INSTALL" = "true" ]; then
  echo "==> Installing frontend dependencies"
  (cd "$FRONTEND_DIR" && pnpm install --frozen-lockfile)
fi

echo "==> Building frontend"
(cd "$FRONTEND_DIR" && pnpm run build)

TMP_DOCKERFILE=""
BUILD_CONTEXT=""

cleanup() {
  [ -z "$TMP_DOCKERFILE" ] || rm -f "$TMP_DOCKERFILE"
  [ -z "$BUILD_CONTEXT" ] || rm -rf "$BUILD_CONTEXT"
}
trap cleanup EXIT

TMP_DOCKERFILE="$(mktemp "${TMPDIR:-/tmp}/openlist-dockerfile.XXXXXX")"
BUILD_CONTEXT="$(mktemp -d "${TMPDIR:-/tmp}/openlist-context.XXXXXX")"

echo "==> Preparing temporary Docker build context"
rsync -a --delete \
  --exclude='/.git' \
  --exclude='/.github' \
  --exclude='/.idea' \
  --exclude='/.omx' \
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
ARG GO_VERSION=1.24
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
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -tags=jsoniter \
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

BUILD_ARGS=(
  buildx build
  --file "$TMP_DOCKERFILE"
  --tag "$IMAGE"
  --platform "$PLATFORMS"
  --build-arg "GO_VERSION=$GO_VERSION"
  --build-arg "BASE_IMAGE=$BASE_IMAGE"
  --build-arg "BASE_IMAGE_TAG=$BASE_IMAGE_TAG"
  --build-arg "INSTALL_ARIA2=$INSTALL_ARIA2"
  --build-arg "BUILT_AT=$BUILT_AT"
  --build-arg "GIT_COMMIT=$GIT_COMMIT"
  --build-arg "VERSION=$VERSION"
  --build-arg "WEB_VERSION=$WEB_VERSION"
)

if [ "$NO_CACHE" = "true" ]; then
  BUILD_ARGS+=(--no-cache)
fi

if [ "$PUSH" = "true" ]; then
  BUILD_ARGS+=(--push)
else
  [[ "$PLATFORMS" != *,* ]] || die "--no-push can only be used with one platform, for example --platforms linux/amd64"
  BUILD_ARGS+=(--load)
fi

BUILD_ARGS+=("$BUILD_CONTEXT")

echo "==> Building Docker image"
docker "${BUILD_ARGS[@]}"

echo "==> Done"
if [ "$PUSH" = "true" ]; then
  echo "Pushed: $IMAGE"
else
  echo "Built locally: $IMAGE"
fi
echo
echo "Deploy with:"
echo "  OPENLIST_IMAGE=$IMAGE docker compose -f deploy/docker-compose.dockerhub.yml up -d"
