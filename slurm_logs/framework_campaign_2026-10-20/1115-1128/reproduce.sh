#!/usr/bin/env bash
# Run in this existing frozen worktree; all generated data stays private.
set -eu
TASK_ROOT=/local/job40255479/worktrees/1115-1128-provenance
BASE_ROOT=/local/job40255479/worktrees/framework-integration
IMAGE_PY=/local/job40255479/venv_img/bin/python
REPORT_DIR="$TASK_ROOT/slurm_logs/framework_campaign_2026-10-20/1115-1128"
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
cd "$TASK_ROOT"
export PYTHONPATH="$TASK_ROOT" LSMS_COUNTRIES_ROOT="$TASK_ROOT/lsms_library/countries"
export LSMS_DATA_DIR=/local/job40255479/1115-1128-reproduce-private
"$IMAGE_PY" - <<'PY'
from pathlib import Path
import lsms_library
from lsms_library.paths import countries_root, data_root
root=Path.cwd().resolve()
assert Path(lsms_library.__file__).resolve()==root/'lsms_library/__init__.py'
assert countries_root().resolve()==root/'lsms_library/countries'
assert data_root().resolve()==Path('/local/job40255479/1115-1128-reproduce-private')
PY
taskset -c 12-15 "$IMAGE_PY" -m pytest -q -n 4 --dist=loadfile \
 tests/test_wave_provenance.py tests/test_acquisition_source_validation.py \
 tests/test_capability.py tests/test_catalog.py tests/test_data_access.py \
 tests/test_provenance.py tests/test_repository_discovery.py tests/test_build_transform_hash.py \
 > "$REPORT_DIR/reproduced-related.log" 2>&1
taskset -c 12-15 "$IMAGE_PY" -m pytest -q \
 tests/test_gh831_intree_source_policy.py tests/test_gh831_no_runtime_writes_into_config_tree.py \
 > "$REPORT_DIR/reproduced-policy.log" 2>&1
taskset -c 12 "$IMAGE_PY" "$REPORT_DIR/source_census.py" --out "$REPORT_DIR/reproduced-census.json"
# Hash probes require separate empty roots; they create no parquet.
export LSMS_DATA_DIR=/local/job40255479/1115-1128-hash-reproduce-patched
taskset -c 12 "$IMAGE_PY" "$REPORT_DIR/hash_probe.py" --out "$REPORT_DIR/reproduced-hash-patched.json"
cd "$BASE_ROOT"
export PYTHONPATH="$BASE_ROOT" LSMS_COUNTRIES_ROOT="$BASE_ROOT/lsms_library/countries"
export LSMS_DATA_DIR=/local/job40255479/1115-1128-hash-reproduce-base
taskset -c 12 "$IMAGE_PY" "$REPORT_DIR/hash_probe.py" --out "$REPORT_DIR/reproduced-hash-base.json"
# Negative controls intentionally exit nonzero; check retained summaries.
export LSMS_DATA_DIR=/local/job40255479/1115-1128-negative-reproduce-base
if taskset -c 12 "$IMAGE_PY" "$REPORT_DIR/negative_controls.py" base > "$REPORT_DIR/reproduced-negative-base.log" 2>&1; then
  echo 'Unexpected baseline negative-control pass' >&2; exit 1
fi
cd "$TASK_ROOT"
export PYTHONPATH="$TASK_ROOT" LSMS_COUNTRIES_ROOT="$TASK_ROOT/lsms_library/countries"
export LSMS_DATA_DIR=/local/job40255479/1115-1128-negative-reproduce-prior
if taskset -c 12 "$IMAGE_PY" "$REPORT_DIR/negative_controls.py" prior-functions > "$REPORT_DIR/reproduced-negative-prior.log" 2>&1; then
  echo 'Unexpected prior-function negative-control pass' >&2; exit 1
fi
