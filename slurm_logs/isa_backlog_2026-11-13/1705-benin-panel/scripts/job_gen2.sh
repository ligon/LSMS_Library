#!/bin/bash
#SBATCH -J benin-gen2
# usage: job_gen2.sh <sha>  -- make panel-ids at <sha>; copy the JSON out; link quality on it
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
SHA=$1
source $BATCH/tools/burst-env.sh benin $SHA || exit 1
G=$OUT/gen-${SHA:0:12}; mkdir -p $G
cd "$SRC_1/lsms_library/countries/Benin/_"
export PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 PATH=$(dirname $PY):$PATH
{ make panel-ids; echo "exit $?"; git -C "$SRC_1" status --short; echo status-end; } > $G/gen.txt 2>&1
cp panel_ids.json updated_ids.json $G/
cd "$SRC_1"
"$PY" $BATCH/reports/benin/scripts/link_quality.py $G/updated_ids.json > $G/link_quality.txt 2>&1; echo "exit $?" >> $G/link_quality.txt
