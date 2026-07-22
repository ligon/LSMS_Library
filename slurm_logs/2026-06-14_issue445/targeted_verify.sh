#!/usr/bin/env bash
# GH #445 targeted SERIAL verification: deterministic proof the 5 fixed items
# pass, isolated from the parallel-cold cache-race flakiness (#330).
set -uo pipefail
cd /global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
PY=.venv/bin/python

echo "### 1. repopulate full Uganda cold cache (13 baseline tables) ###"
$PY slurm_logs/2026-06-14_issue445/cold_rebuild_uganda.py 2>&1 | grep -E "^(OK|FAIL|DONE)"

echo
echo "### 2. targeted tests, SERIAL (-p no:cacheprovider, no xdist) ###"
$PY -m pytest \
  tests/test_uganda_invariance.py \
  "tests/test_sample.py::TestSample::test_covers_all_waves[Albania]" \
  "tests/test_uganda_api_vs_replication.py::test_api_matches_replication[interview_date]" \
  "tests/test_uganda_api_vs_replication.py::test_api_matches_replication[food_quantities]" \
  -p no:cacheprovider -q -rxX 2>&1 | grep -vE "RuntimeWarning|warnings.warn|categorical mapping|no _/ directory|Canonical index|collapsed via|UserWarning" | tail -25
