#!/bin/bash
#SBATCH -J mqrt-raw
set -uo pipefail
RT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/manquant-redteam
CAND=fc40960d27b800045984735a38c68cf91c9b2194
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh manquant-redteam $CAND || exit 1
( cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 timeout 2400 $PY $RT/scripts/rt_raw.py $OUT/raw.json ) > $OUT/raw.log 2>&1
echo "raw exit $?"; tail -3 $OUT/raw.log
