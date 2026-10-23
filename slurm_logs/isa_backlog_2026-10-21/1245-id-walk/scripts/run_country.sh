#!/bin/bash
# run_country.sh <label base|cand> <checkout> <country>
label=$1; checkout=$2; c=$3
source /local/job40390255/data-private/1245/env.sh
export PYTHONPATH=$checkout
root=/local/job40390255/data-private/1245/$label/$c
logs=/local/job40390255/data-private/1245/$label/logs
mkdir -p $root $logs
ln -sfn /global/scratch/fsa/fc_jevons/ligon/cache/lsms_library/dvc-cache $root/dvc-cache
export LSMS_DATA_DIR=$root
cd $checkout
H=/local/job40390255/data-private/1245/harness.py
out=$logs/$c.jsonl
for r in cold warm_fresh assume feature; do
  nice -n 19 $PY $H $checkout $c $r $out >> $logs/$c.$r.log 2>&1
done
echo DONE >> $logs/$c.done
