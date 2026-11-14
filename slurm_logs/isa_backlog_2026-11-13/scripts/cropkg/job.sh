#!/bin/bash
# GH #1559 cropkg stream.  Usage:
#   sbatch -J NAME job.sh CAND_SHA BASE_SHA STEP [STEP...]
#   STEP in: build compare analysis part2 tests census probe
# Restartable: each step rebuilds what it needs in node-local private roots.
set -uo pipefail
CAND=$1; BASE=$2; shift 2
REP=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/cropkg
PRIOR=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/sold-kg-1560
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh cropkg "$CAND" "$BASE" || exit 9
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
JOBLOCAL=/local/job${SLURM_JOB_ID}
FR_C=$JOBLOCAL/private/frames-cand; FR_B=$JOBLOCAL/private/frames-base
mkdir -p "$FR_C" "$FR_B"
J=${SLURM_JOB_ID}
run_c() { ( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" "$@" ); }
run_b() { ( cd "$SRC_2" && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 "$PY" "$@" ); }
for step in "$@"; do
  echo "=== step $step $(date)"
  case $step in
    quick)
      ( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" -m pytest -q -p no:cacheprovider -m "not requires_s3" tests/test_gh1559_survey_kg_price_screen.py > "$OUT/pytest-quick-$J.txt" 2>&1 ); echo "quick exit $?"; tail -30 "$OUT/pytest-quick-$J.txt" ;;
    build)
      run_c "$REP/scripts/build.py" "$FR_C" cand "$OUT/build-cand-$J.json"
      run_b "$REP/scripts/build.py" "$FR_B" base "$OUT/build-base-$J.json" ;;
    compare)
      run_c "$REP/scripts/compare.py" "$FR_B" "$FR_C" "$OUT/compare-$J.json" ;;
    analysis)
      run_c "$REP/scripts/analysis.py" "$FR_C" "$OUT/analysis-$J.json" ;;
    analysis3)
      run_c "$REP/scripts/analysis3.py" "$FR_C" "$OUT/analysis3-$J.json" ;;
    part2)
      run_c "$PRIOR/scripts/part2.py" "$FR_C" "$OUT/part2-repro-$J" ;;
    tests)
      TESTS=$( cd "$SRC_1" && { ls tests/test_*niger*.py | grep -v nigeria; ls tests/test_*mali*.py; \
               ls tests/test_gh1559_survey_kg_price_screen.py tests/test_quantity_screen.py \
                  tests/test_build_transform_hash.py tests/test_crop_survey_kg.py \
                  tests/test_gh1569_sold_diagnostics.py tests/test_gh1482_1485_quantity_diagnostics.py \
                  tests/test_gh1493_near_zero_screen.py tests/test_crop_kg_factor.py \
                  tests/test_schema_consistency.py tests/test_null_read_guard.py \
                  tests/test_null_index_guard.py tests/test_failure_is_not_absence.py \
                  tests/test_derivations.py tests/test_quantity_kg.py \
                  tests/test_median_price_valuation.py tests/test_gh1528_sold_condition.py \
                  tests/test_ehcvm_crop_production_condition.py tests/test_gh1475_country_crop_transforms.py \
                  tests/test_gh1460_crop_level.py tests/test_feature_attrs_carriers.py; } 2>/dev/null | sort -u )
      echo "$TESTS" > "$OUT/pytest-$J.files"
      ( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" -m pytest -n "${SLURM_CPUS_ON_NODE}" \
          --dist=loadfile -q -rs -p no:cacheprovider $TESTS > "$OUT/pytest-$J.txt" 2>&1 )
      echo "pytest exit $?"; tail -5 "$OUT/pytest-$J.txt" ;;
    census)
      run_c "$BATCH/tools/hash_census.py" "$OUT/census-cand-$J.json"
      run_b "$BATCH/tools/hash_census.py" "$OUT/census-base-$J.json"
      "$PY" "$BATCH/tools/hashdiff.py" "$OUT/census-base-$J.json" "$OUT/census-cand-$J.json" "$OUT/hashdiff-$J.json" ;;
    probe)
      for P in ${PROBES:-p1 p2 p3 p4}; do
        PD=$JOBLOCAL/probe-$P
        rm -rf "$PD"; git clone --quiet --shared --no-checkout "$SRC_1" "$PD"
        git -C "$PD" checkout --quiet --detach "$CAND"
        "$PY" "$REP/scripts/probe_edit.py" "$PD" "$P" || { echo "probe edit $P failed"; continue; }
        git -C "$PD" diff --stat
        ( cd "$PD" && PYTHONPATH=$PD LSMS_DATA_DIR=$DATA_1 "$PY" "$BATCH/tools/hash_census.py" "$OUT/census-probe-$P-$J.json" )
        "$PY" "$BATCH/tools/hashdiff.py" "$OUT/census-cand-$J.json" "$OUT/census-probe-$P-$J.json" "$OUT/hashdiff-probe-$P-$J.json"
      done ;;
    *) echo "unknown step $step" ;;
  esac
  echo "=== step $step exit $?"
done
echo "issues log lines: $(wc -l < "$LSMS_ISSUES_LOG" 2>/dev/null || echo 0)"
for s in "$SRC_1" "$SRC_2"; do echo "in-tree parquet $s: $(find "$s/lsms_library/countries" -name '*.parquet' | wc -l)"; done
( cd "$SRC_1" && bin/savio_venv.sh umount ) || true
echo done
