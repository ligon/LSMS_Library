#!/usr/bin/env bash
set -euo pipefail
: "${1:?final master SHA required}"
: "${2:?exact automatic master CI run ID required}"
RELEASE_PREP=/local/job40255479/control/release-v016/verification-prep-seed1519
export RELEASE_REPO=/local/job40255479/worktrees/release-v016-master
export RELEASE_EXPECTED_SHA="$1"
export RELEASE_CI_RUN_ID="$2"
export RELEASE_OUT=/local/job40255479/control/release-v016/make-release-final
export RELEASE_COPY_MANIFEST="$RELEASE_PREP/copy-manifest-private.json"
export RELEASE_CI_WAIT_SECONDS=10800
export PYTHONPATH="$RELEASE_REPO:$RELEASE_PREP"
export LSMS_COUNTRIES_ROOT="$RELEASE_REPO/lsms_library/countries"
export LSMS_DATA_DIR=/local/job40255479/release-v016-seed-data
export LSMS_EXPECTED_ROOT="$RELEASE_REPO"
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
export LSMS_BUILD_WORKERS=1 LSMS_MAKE_JOBS=1 MAKEFLAGS=-j1
export PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1
unset LSMS_NO_CACHE LSMS_SKIP_AUTH PYTEST_ADDOPTS PYTEST_PLUGINS LSMS_ALLOW_INTREE_SOURCES PYTHONOPTIMIZE PYTHONSAFEPATH
cd "$RELEASE_REPO"
exec taskset -c 0-7,20-27 make release v=0.16.0 \
 POETRY="$RELEASE_PREP/poetry-pytest-only" \
 PYTEST_ARGS='-n 16 --dist=loadfile --timeout=600 --timeout-method=thread --maxfail=1 --tb=short -ra --durations=25'
