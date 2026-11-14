#!/bin/bash
# GH #1559 follow-up: sbatch -J NAME job2.sh CAND_SHA   (cold candidate build + analysis2)
set -uo pipefail
CAND=$1
REP=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/cropkg
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh cropkg "$CAND" || exit 9
FR_C=/local/job${SLURM_JOB_ID}/private/frames-cand; mkdir -p "$FR_C"
J=${SLURM_JOB_ID}
run_c() { ( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" "$@" ); }
echo "=== build $(date)"; run_c "$REP/scripts/build.py" "$FR_C" cand "$OUT/build2-cand-$J.json"; echo "=== build exit $?"
echo "=== analysis2 $(date)"; run_c "$REP/scripts/analysis2.py" "$FR_C" "$OUT/analysis2-$J.json"; echo "=== analysis2 exit $?"
echo "issues log lines: $(wc -l < "$LSMS_ISSUES_LOG" 2>/dev/null || echo 0)"
echo "in-tree parquet: $(find "$SRC_1/lsms_library/countries" -name '*.parquet' | wc -l)"
echo done
