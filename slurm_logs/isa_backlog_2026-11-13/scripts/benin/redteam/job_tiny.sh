#!/bin/bash
#SBATCH -J rt-benin-tiny
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
RT=$BATCH/reports/benin-redteam
CAND=d5fdf326cd2bcec359e3c1a89e63c732cb6e0815
source $BATCH/tools/burst-env.sh benin-redteam $CAND || exit 1
cd $SRC_1; PYTHONPATH=$SRC_1 $PY $RT/scripts/rt_tiny.py $OUT/main/cand/plot_inputs.pkl > $OUT/tiny.txt 2>&1; echo "exit $?" >> $OUT/tiny.txt
