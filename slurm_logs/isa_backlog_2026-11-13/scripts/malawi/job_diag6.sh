#!/bin/bash
#SBATCH -J mw-diag6
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/malawi/logs/%x-%j.out
REP=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/malawi
RT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/malawi-redteam
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh malawi 51a9186848d73983f19fb4e50c45d25fc408dfd6 f138a3d61c203f892148592675a7ef72e160a322 || exit 9
arm() { local which=$1; shift; if [ $which = cand ]; then cd $SRC_1 && env PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$@"; else cd $SRC_2 && env PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 "$@"; fi; }
O=$OUT/verify-v5/diag6; mkdir -p $O
arm cand $PY $REP/scripts/diag6.py $SRC_1 $OUT/verify-v5 $O > $O/diag6.log 2>&1; echo "diag6 rc=$?"
arm cand $PY $RT/scripts/fwimp_parts.py $SRC_1 $O/fwimp-cand.json > $O/fwimp-cand.log 2>&1; echo "fwimp cand rc=$?"
arm base $PY $RT/scripts/fwimp_parts.py $SRC_2 $O/fwimp-base.json > $O/fwimp-base.log 2>&1; echo "fwimp base rc=$?"
