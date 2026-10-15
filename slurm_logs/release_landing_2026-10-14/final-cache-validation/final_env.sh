#!/bin/bash
# Source only after the coordinator supplies the final development SHA.
set -eu
: "${LSMS_FINAL_SHA:?Set LSMS_FINAL_SHA to the coordinator-provided final 40-character SHA}"
export LSMS_FINAL_WORKTREE=/local/job39919092/release-work/runtime/final-candidate
export PYTHONPATH="$LSMS_FINAL_WORKTREE"
export LSMS_COUNTRIES_ROOT="$LSMS_FINAL_WORKTREE/lsms_library/countries"
export LSMS_DATA_DIR=/local/job39919092/release-work/runtime/data-final-promotion
export LSMS_SKIP_AUTH=1 OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
export LSMS_BUILD_WORKERS=1 LSMS_MAKE_JOBS=1
export PATH="/local/job39919092/venv_img/bin:$PATH"
unset PYTHONSAFEPATH LSMS_NO_CACHE LSMS_BUILD_BACKEND
cd "$LSMS_FINAL_WORKTREE"
test "$(git rev-parse HEAD)" = "$LSMS_FINAL_SHA"
test -z "$(git status --short)"
