#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${CONTEXTSWARM_MINI_IMAGE:-contextswarm-iclr-mini:latest}"
PI_VERSION="${CONTEXTSWARM_MINI_PI_VERSION:-0.84.3}"
CODEX_VERSION="${CONTEXTSWARM_MINI_CODEX_VERSION:-0.150.1}"
cd "${ROOT_DIR}"

# A paper-facing image must be an exact Git snapshot.  Refuse an untracked or
# modified source tree so the revision label cannot describe different bytes.
if [[ -n "$(git status --porcelain=v1 --untracked-files=all)" ]]; then
  echo "refusing to build a formal image from a dirty worktree" >&2
  exit 2
fi
SOURCE_COMMIT="$(git rev-parse --verify HEAD)"
case "${SOURCE_COMMIT}" in
  (????????????????????????????????????????)
    ;;
  (*)
    echo "unable to resolve a full source commit" >&2
    exit 2
    ;;
esac

# Build from Git's exact tracked tree, not from the ambient directory.  This
# also excludes ignored operator files that a broad Docker COPY could otherwise
# pick up despite a clean `git status`.
BUILD_CONTEXT="$(mktemp -d "${TMPDIR:-/tmp}/contextswarm-image.XXXXXX")"
cleanup() {
  rm -rf -- "${BUILD_CONTEXT}"
}
trap cleanup EXIT
git archive --format=tar "${SOURCE_COMMIT}" | tar -xf - -C "${BUILD_CONTEXT}"

docker build \
  --build-arg "PI_VERSION=${PI_VERSION}" \
  --build-arg "CODEX_VERSION=${CODEX_VERSION}" \
  --build-arg "CONTEXTSWARM_SOURCE_COMMIT=${SOURCE_COMMIT}" \
  --label "org.opencontainers.image.revision=${SOURCE_COMMIT}" \
  -t "${IMAGE}" "${BUILD_CONTEXT}"
