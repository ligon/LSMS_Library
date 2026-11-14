#!/bin/bash
#SBATCH -J manquant-senjoin
set -uo pipefail
R=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/manquant
BASE=f138a3d61c203f892148592675a7ef72e160a322
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh manquant $BASE || exit 1
cd "$SRC_1"
PYTHONPATH=$SRC_1 "$PY" "$R/scripts/sen_join.py" "$OUT/base-probe" > "$OUT/senjoin_base.json" 2> "$OUT/senjoin_base.log"
echo "exit $?"
