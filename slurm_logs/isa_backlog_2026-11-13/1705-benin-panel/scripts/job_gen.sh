#!/bin/bash
#SBATCH -J benin-gen
# usage: job_gen.sh <sha>   -- run `make panel-ids` in a clone at <sha>; copy the JSON to out/gen-<sha12>/
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
SHA=$1
source $BATCH/tools/burst-env.sh benin $SHA || exit 1
G=$OUT/gen-${SHA:0:12}; mkdir -p $G
cd "$SRC_1/lsms_library/countries/Benin/_"
export PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 PATH=$(dirname $PY):$PATH
{
  echo "== make panel-ids"; make panel-ids; echo "exit $?"
  cp panel_ids.json updated_ids.json $G/
  echo "== git status in clone"; git -C "$SRC_1" status --short
  echo "== make -n checks"
  for t in panel_ids.json updated_ids.json; do echo "-- make -n $t"; make -n $t; echo "exit $?"; done
  for w in 2018-19 2021-22; do for t in community_prices crop_production livestock people_last7days plot_features plot_inputs plot_labor; do
    echo "-- make -n ../$w/_/$t.parquet"; make -n ../$w/_/$t.parquet 2>&1; echo "exit $?"; done; done
  echo "-- make -n all"; make -n all 2>&1; echo "exit $?"
  echo "-- make -n clean"; make -n clean 2>&1; echo "exit $?"
  echo "-- make -n panel-ids"; make -n panel-ids; echo "exit $?"
} > $G/gen.txt 2>&1
echo done >> $G/gen.txt
