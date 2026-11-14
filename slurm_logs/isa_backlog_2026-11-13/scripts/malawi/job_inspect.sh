#!/bin/bash
#SBATCH -J mw-inspect
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/malawi/logs/%x-%j.out
REP=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/malawi
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh malawi f138a3d61c203f892148592675a7ef72e160a322 || exit 9
cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $REP/scripts/inspect_cols.py $OUT/verify-v2/frames $OUT/verify-v2/inspect_cols.json ag_h14b ag_h33 ag_h36b_oth ag_h42b_os ag_h06a > $OUT/verify-v2/inspect_cols.log 2>&1
echo "inspect rc=$?"
