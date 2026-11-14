#!/bin/bash
#SBATCH -J mw-otherpi
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/malawi/logs/%x-%j.out
REP=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/malawi
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh malawi 9ca3518a35e6c688577283121baeb49a939b9e0f f138a3d61c203f892148592675a7ef72e160a322 || exit 9
O=$OUT/verify-final/other_pi; mkdir -p $O; C="Uganda Nigeria Niger"
( cd $SRC_1 && env PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $REP/scripts/other_pi.py $SRC_1 cand $O $C > $O/cand.log 2>&1; echo "cand rc=$?" ) &
( cd $SRC_2 && env PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 $PY $REP/scripts/other_pi.py $SRC_2 base $O $C > $O/base.log 2>&1; echo "base rc=$?" ) &
wait
cd $SRC_1 && $PY $REP/scripts/other_pi_cmp.py $O $C; echo "cmp rc=$?"
