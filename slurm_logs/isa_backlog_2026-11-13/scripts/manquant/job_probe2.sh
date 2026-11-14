#!/bin/bash
#SBATCH -J manquant-probe2
set -uo pipefail
R=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/manquant
BASE=f138a3d61c203f892148592675a7ef72e160a322
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh manquant $BASE || exit 1
cd "$SRC_1"
PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" "$R/scripts/probe2.py" "$OUT/base-probe" "$OUT/cand-f001cfbd0aac" > "$OUT/probe2.json" 2> "$OUT/probe2.log"
echo "exit $?"
