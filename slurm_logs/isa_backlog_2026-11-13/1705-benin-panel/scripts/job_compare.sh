#!/bin/bash
#SBATCH -J benin-compare
# usage: job_compare.sh <sha-for-venv> <base_dir> <cand_dir> <updated_ids.json> <out.json>
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
source $BATCH/tools/burst-env.sh benin $1 || exit 1
cd "$SRC_1"
PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" $BATCH/reports/benin/scripts/compare.py $2 $3 $4 $5 > ${5%.json}.txt 2>&1
echo "exit $?" >> ${5%.json}.txt
