#!/bin/bash
#SBATCH -J mw-diag5
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/malawi/logs/%x-%j.out
REP=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/malawi
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh malawi 51a9186848d73983f19fb4e50c45d25fc408dfd6 || exit 9
cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $REP/scripts/diag5.py $SRC_1 $OUT/verify-v5 $OUT/verify-v5/diag5 > $OUT/verify-v5/diag5.log 2>&1
echo "diag5 rc=$?"
