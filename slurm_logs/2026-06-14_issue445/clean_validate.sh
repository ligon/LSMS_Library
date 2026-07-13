#!/usr/bin/env bash
# GH #445: CLEAN baseline validation -- clear, ONE complete cold build from
# empty, assert all 53 baseline parquets present, then run the invariance test
# SERIALLY so every case actually executes (no skips, no parallel cache races).
set -uo pipefail
cd /global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
PY=.venv/bin/python

echo "### clear Uganda cache ###"
.venv/bin/lsms-library cache clear --country Uganda >/dev/null 2>&1
echo "### cold build (from empty) ###"
$PY slurm_logs/2026-06-14_issue445/cold_rebuild_uganda.py 2>&1 | grep -E "^(OK|FAIL|DONE)"

echo "### assert 53/53 baseline parquets present ###"
$PY - <<'EOF'
import json, pathlib
from lsms_library.paths import data_root
r = pathlib.Path(data_root('Uganda'))
B = json.load(open('tests/fixtures/uganda_baseline.json'))
miss = [k for k in B if not (r/k).exists()]
print(f"present {len(B)-len(miss)}/{len(B)}")
if miss:
    print("MISSING:", *miss, sep="\n  ")
    raise SystemExit(2)
EOF
[ $? -ne 0 ] && { echo "ABORT: incomplete cache"; exit 2; }

echo "### invariance test, SERIAL ###"
$PY -m pytest tests/test_uganda_invariance.py -p no:cacheprovider -q 2>&1 \
  | grep -vE "RuntimeWarning|warnings.warn|categorical mapping|no _/ directory|Canonical index|collapsed via|UserWarning" \
  | tail -15
