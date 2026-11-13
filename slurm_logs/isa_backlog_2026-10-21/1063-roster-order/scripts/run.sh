#!/bin/bash
# Usage: run.sh <checkout> <rootname> <country> <outname> [table]  (extra env passed through)
# Creates a fresh data root (if absent) with the shared dvc-cache symlink and builds.
set -e
CHECKOUT=$1; ROOT=/local/job40390255/data-private/1063/$2; COUNTRY=$3; OUT=$4; TABLE=${5:-household_roster}
S=/local/job40390255/tmp/claude-43292/-local-job40390255-mirrors-LSMS-Library/aad69bbf-39c1-40e1-bf95-cbd7d49b5cce/scratchpad/1063
mkdir -p $ROOT
[ -e $ROOT/dvc-cache ] || ln -s /global/scratch/fsa/fc_jevons/ligon/cache/lsms_library/dvc-cache $ROOT/dvc-cache
cd $CHECKOUT
export PYTHONPATH=$CHECKOUT LSMS_DATA_DIR=$ROOT OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
nice -n 19 /local/job40390255/venv_img/bin/python $S/build.py $CHECKOUT $COUNTRY $S/out/$OUT.pkl $TABLE
