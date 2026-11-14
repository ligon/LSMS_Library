#!/bin/bash
# Red team GH #1559 job B: hash census base vs cand, probe edits, tests incl. pin probes.
# sbatch ... jobB.sh CAND BASE
set -uo pipefail
CAND=$1; BASE=$2
RT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/cropkg-redteam
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
source $BATCH/tools/burst-env.sh cropkg-redteam "$CAND" "$BASE" || exit 9
J=$SLURM_JOB_ID; JL=/local/job$J
run_c() { ( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" "$@" ); }
run_b() { ( cd "$SRC_2" && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 "$PY" "$@" ); }
echo "=== census $(date)"
run_c $BATCH/tools/hash_census.py $OUT/B-census-cand-$J.json
run_b $BATCH/tools/hash_census.py $OUT/B-census-base-$J.json
$PY $BATCH/tools/hashdiff.py $OUT/B-census-base-$J.json $OUT/B-census-cand-$J.json $OUT/B-hashdiff-$J.json
for P in pA pB pAB pF pC pD pE pPos; do
  PD=$JL/probe-$P; rm -rf "$PD"
  git clone --quiet --shared --no-checkout "$SRC_1" "$PD" && git -C "$PD" checkout --quiet --detach "$CAND"
  "$PY" -I "$RT/scripts/rt_probe_edit.py" "$PD" "$P" || { echo "probe $P edit failed"; continue; }
  git -C "$PD" diff --stat
  ( cd "$PD" && PYTHONPATH=$PD LSMS_DATA_DIR=$DATA_1 "$PY" $BATCH/tools/hash_census.py $OUT/B-census-$P-$J.json )
  "$PY" $BATCH/tools/hashdiff.py $OUT/B-census-cand-$J.json $OUT/B-census-$P-$J.json $OUT/B-hashdiff-$P-$J.json
  if [ "$P" = pA ] || [ "$P" = pB ]; then
    ( cd "$PD" && PYTHONPATH=$PD LSMS_DATA_DIR=$DATA_1 "$PY" -m pytest -q -p no:cacheprovider \
        tests/test_gh1559_survey_kg_price_screen.py -k "excluded or exclusion or fingerprint" \
        > $OUT/B-pytest-pin-$P-$J.txt 2>&1 ); echo "pin test $P exit $?"; tail -5 $OUT/B-pytest-pin-$P-$J.txt
  fi
done
echo "=== tests $(date)"
TESTS="tests/test_gh1559_survey_kg_price_screen.py tests/test_build_transform_hash.py tests/test_quantity_screen.py tests/test_crop_survey_kg.py tests/test_gh1493_near_zero_screen.py tests/test_gh1569_sold_diagnostics.py tests/test_gh1482_1485_quantity_diagnostics.py tests/test_niger_crop_sold_kg.py tests/test_mali_crop_sold_kg.py tests/test_add_market_index_keeps_unlabelled.py"
( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" -m pytest -n "$SLURM_CPUS_ON_NODE" --dist=loadfile -q -rs -p no:cacheprovider $TESTS > $OUT/B-pytest-$J.txt 2>&1 ); echo "pytest exit $?"; tail -8 $OUT/B-pytest-$J.txt
echo "issues log lines: $(wc -l < "$LSMS_ISSUES_LOG" 2>/dev/null || echo 0)"
for s in "$SRC_1" "$SRC_2"; do echo "in-tree parquet $s: $(find "$s/lsms_library/countries" -name '*.parquet' | wc -l)"; done
( cd "$SRC_1" && bin/savio_venv.sh umount ) || true
echo "done $(date)"
