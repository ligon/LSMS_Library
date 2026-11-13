#!/bin/bash
# usage: run.sh <label> <checkout> <datadir> <script> [args]
label=$1; checkout=$2; data=$3; script=$4; shift 4
export PYTHONPATH=$checkout LSMS_DATA_DIR=$data
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
cd $checkout
exec nice -n 19 /local/job40390255/venv_img/bin/python $script "$label" "$checkout" "$@"
