#!/bin/bash
# Red team GH #1559 job A: cold builds base/cand (Niger, Mali), report-only, strict,
# independent estimator, variants, cost, faults, Feature, memo numbers.
# sbatch ... jobA.sh CAND BASE
set -uo pipefail
CAND=$1; BASE=$2
RT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/cropkg-redteam
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh cropkg-redteam "$CAND" "$BASE" || exit 9
J=$SLURM_JOB_ID; JL=/local/job$J
FR_C=$JL/private/frames-cand; FR_B=$JL/private/frames-base; mkdir -p "$FR_C" "$FR_B"
run_c() { ( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" "$@" ); }
run_b() { ( cd "$SRC_2" && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 "$PY" "$@" ); }
echo "=== build cand $(date)"; run_c "$RT/scripts/rt_build.py" "$FR_C" cand "$OUT/A-build-cand-$J.json"; echo "exit $?"
echo "=== build base $(date)"; run_b "$RT/scripts/rt_build.py" "$FR_B" base "$OUT/A-build-base-$J.json"; echo "exit $?"
echo "=== compare $(date)"; run_c "$RT/scripts/rt_compare.py" "$FR_B" "$FR_C" "$OUT/A-build-base-$J.json" "$OUT/A-build-cand-$J.json" "$OUT/A-compare-$J.json"; echo "exit $?"
echo "=== analysis $(date)"; run_c "$RT/scripts/rt_analysis.py" "$FR_C" "$OUT/A-analysis-$J.json"; echo "exit $?"
echo "=== memo $(date)"; run_c "$RT/scripts/rt_memo.py" "$FR_C" "$OUT/A-memo-$J.json"; echo "exit $?"
echo "issues log lines: $(wc -l < "$LSMS_ISSUES_LOG" 2>/dev/null || echo 0)"
for s in "$SRC_1" "$SRC_2"; do echo "in-tree parquet $s: $(find "$s/lsms_library/countries" -name '*.parquet' | wc -l)"; done
( cd "$SRC_1" && bin/savio_venv.sh umount ) || true
echo "done $(date)"
