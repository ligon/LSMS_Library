#!/bin/bash
#SBATCH -J mwrt-8
CAND=9ca3518a35e6c688577283121baeb49a939b9e0f
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
RT=$BATCH/reports/malawi-redteam
source $BATCH/tools/burst-env.sh malawi-redteam $CAND || exit 9
O=$OUT/job8; mkdir -p $O
( cd $SRC_1 && env PYTHONPATH=$SRC_1 $PY $RT/scripts/probe_1712.py $SRC_1 > $O/probe.out 2>&1; echo "probe rc=$?" )
