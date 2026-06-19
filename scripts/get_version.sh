#!/usr/bin/env bash
# scripts/get_version.sh - Infers project version from git tags and status.
set -euo pipefail

# Ensure we are in a git repository. If not, default to v0.0.0.
if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "v0.0.0"
    exit 0
fi

# Get the latest tag reachable from HEAD.
LATEST_TAG=$(git describe --tags --abbrev=0 2>/dev/null || true)

# Check if the working tree is dirty.
if [ -n "$(git status --porcelain 2>/dev/null)" ]; then
    DIRTY=true
else
    DIRTY=false
fi

# Check if HEAD is the tagged commit.
IS_TAGGED=false
if [ -n "$LATEST_TAG" ]; then
    TAG_COMMIT=$(git rev-parse --verify "${LATEST_TAG}^{commit}" 2>/dev/null || true)
    HEAD_COMMIT=$(git rev-parse --verify HEAD 2>/dev/null || true)
    if [ "$TAG_COMMIT" = "$HEAD_COMMIT" ]; then
        IS_TAGGED=true
    fi
fi

# Determine base version.
if [ -n "$LATEST_TAG" ]; then
    BASE_VERSION="$LATEST_TAG"
else
    BASE_VERSION="v0.0.0"
fi

# Determine final version.
if [ "$IS_TAGGED" = true ] && [ "$DIRTY" = false ]; then
    echo "$BASE_VERSION"
else
    SHORT_SHA=$(git rev-parse --short=10 HEAD 2>/dev/null || echo "")
    if [ -n "$SHORT_SHA" ]; then
        echo "${BASE_VERSION}-${SHORT_SHA}"
    else
        echo "$BASE_VERSION"
    fi
fi
