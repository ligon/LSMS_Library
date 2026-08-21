#!/usr/bin/env bash
# GH #445: definitive baseline validation.  conftest purges the Uganda cache at
# session start in DEFAULT mode (conftest.py:96-109); --no-purge disables that,
# so the invariance test reads the cold parquets we just built instead of
# skipping on an empty cache.
set -uo pipefail
cd /global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
PY=.venv/bin/python

.venv/bin/lsms-library cache clear --country Uganda >/dev/null 2>&1
echo "### cold build from empty ###"
$PY slurm_logs/2026-06-14_issue445/cold_rebuild_uganda.py 2>&1 | grep -E "^DONE"

echo "### invariance test, --no-purge, SERIAL (must NOT skip) ###"
$PY -m pytest tests/test_uganda_invariance.py --no-purge -p no:cacheprovider -q -rs 2>&1 \
  | grep -vE "RuntimeWarning|warnings.warn|categorical mapping|no _/ directory|Canonical index|collapsed via|UserWarning" \
  | tail -20
