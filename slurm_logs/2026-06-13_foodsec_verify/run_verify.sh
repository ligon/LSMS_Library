#!/usr/bin/env bash
# Food-security verification sweep driver.  Runs on the interactive node.
# 1. Physically clear caches for every touched country (defeats the
#    script-path L2-wave parquet shadow trap that LSMS_NO_CACHE=1 misses).
# 2. Cold-cache build + sanity-check every food-security (country,feature).
# 3. Structural pytest (schema consistency + feature surface) cold-cache.
set -uo pipefail
cd /global/scratch/fsa/fc_jevons/ligon/mirrors/LSMS_Library
PY=.venv/bin/python
SESS=slurm_logs/2026-06-13_foodsec_verify
export VERIFY_WORKERS=12

echo "########## VERIFY START $(date +%H:%M:%S) ##########"

# --- distinct touched countries, from the same discovery the verifier uses
COUNTRIES=$($PY - <<'PY'
import sys; sys.path.insert(0, 'slurm_logs/2026-06-13_foodsec_verify')
from verify_foodsec import discover_targets
print(' '.join(sorted({c for c, _ in discover_targets()})))
PY
)
echo "Touched countries: $COUNTRIES"

# --- 1. cold the caches (one CLI call, repeatable --country)
CLEAR_ARGS=""
for c in $COUNTRIES; do CLEAR_ARGS="$CLEAR_ARGS --country $c"; done
echo "########## CACHE CLEAR $(date +%H:%M:%S) ##########"
.venv/bin/lsms-library cache clear $CLEAR_ARGS 2>&1 | tail -5

# --- 2. cold build + sanity-check
echo "########## BUILD VERIFY $(date +%H:%M:%S) ##########"
$PY $SESS/verify_foodsec.py
BUILD_RC=$?
echo "build verify exit=$BUILD_RC"

# --- 3. structural pytest, cold cache (--rebuild => LSMS_NO_CACHE=1)
echo "########## PYTEST $(date +%H:%M:%S) ##########"
$PY -m pytest tests/test_schema_consistency.py tests/test_feature.py \
    --rebuild -q -p no:cacheprovider 2>&1 | tail -40
PYTEST_RC=${PIPESTATUS[0]}
echo "pytest exit=$PYTEST_RC"

echo "########## VERIFY DONE $(date +%H:%M:%S) build_rc=$BUILD_RC pytest_rc=$PYTEST_RC ##########"
exit $(( BUILD_RC || PYTEST_RC ))
