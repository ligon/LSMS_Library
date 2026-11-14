#!/bin/bash
#SBATCH -J benin-explore
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
source $BATCH/tools/burst-env.sh benin f138a3d61c203f892148592675a7ef72e160a322 || exit 1
cd "$SRC_1"
PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" $BATCH/reports/benin/scripts/explore.py > $OUT/explore.txt 2>&1
echo "exit $?" >> $OUT/explore.txt
