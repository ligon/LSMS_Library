#!/bin/bash
# Red team GH #1559 job F: donor independence (household counts).  sbatch ... jobF.sh CAND
set -uo pipefail
CAND=$1
RT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/cropkg-redteam
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh cropkg-redteam "$CAND" || exit 9
J=$SLURM_JOB_ID
( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" "$RT/scripts/rt_hh.py" "$OUT/F-hh-$J.json" ); echo "exit $?"
echo "in-tree parquet: $(find "$SRC_1/lsms_library/countries" -name '*.parquet' | wc -l)"
( cd "$SRC_1" && bin/savio_venv.sh umount ) || true
echo done
