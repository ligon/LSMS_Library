#!/bin/bash
# usage: run_test_on.sh <checkout> <datadir> <testfile-in-branch> <log> [pytest args...]
# Copies the branch's test file INTO <checkout>/tests (so pytest's tests/__init__.py
# rootdir insertion imports <checkout>'s lsms_library), runs it, removes the copy.
CO=$1; DD=$2; TF=$3; LOG=$4; shift 4
B=$(basename $TF)
CP=$CO/tests/$B
if [ "$CO" != "/local/job40390255/worktrees/eth-bundle" ]; then cp $TF $CP; fi
cd $CO
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 LSMS_DATA_DIR=$DD PYTHONPATH=$CO \
  nice -n 19 /local/job40390255/venv_img/bin/python -c "import lsms_library,os; print('IMPORT', os.path.realpath(lsms_library.__file__))" > $LOG 2>&1
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 LSMS_DATA_DIR=$DD PYTHONPATH=$CO \
  nice -n 19 /local/job40390255/venv_img/bin/python -m pytest tests/$B -p no:cacheprovider -rA "$@" >> $LOG 2>&1
if [ "$CO" != "/local/job40390255/worktrees/eth-bundle" ]; then rm -f $CP; fi
grep -E "^IMPORT|^(PASSED|FAILED|ERROR)|passed|failed" $LOG | tail -40
