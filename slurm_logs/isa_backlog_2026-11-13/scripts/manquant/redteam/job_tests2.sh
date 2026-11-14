#!/bin/bash
#SBATCH -J mqrt-tests2
set -uo pipefail
RT=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/manquant-redteam
CAND=fc40960d27b800045984735a38c68cf91c9b2194
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh manquant-redteam $CAND || exit 1
cd $SRC_1
PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 timeout 3300 $PY -m pytest -n "$SLURM_CPUS_ON_NODE" --dist=loadfile -p no:cacheprovider -q -rfEs $(cat $RT/out/pytest_extra_generic.txt | tr '\n' ' ') > $OUT/pytest-extra-generic.txt 2>&1
echo "pytest exit $?"; tail -3 $OUT/pytest-extra-generic.txt
echo "in-tree:"; find $SRC_1/lsms_library/countries -name '*.parquet' | head
