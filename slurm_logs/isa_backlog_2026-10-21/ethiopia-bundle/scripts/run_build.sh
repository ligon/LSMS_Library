#!/bin/bash
# usage: run_build.sh <checkout> <datadir> <outdir>
set -e
CO=$1; DD=$2; OUT=$3
cd $CO
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
export LSMS_DATA_DIR=$DD PYTHONPATH=$CO
nice -n 19 /local/job40390255/venv_img/bin/python /local/job40390255/data-private/eth-bundle/scripts/build_cp.py $CO $OUT > $OUT.build.log 2>&1
nice -n 19 /local/job40390255/venv_img/bin/python /local/job40390255/data-private/eth-bundle/scripts/downstream.py $CO $OUT/downstream.json > $OUT.ds.log 2>&1
echo done $OUT
