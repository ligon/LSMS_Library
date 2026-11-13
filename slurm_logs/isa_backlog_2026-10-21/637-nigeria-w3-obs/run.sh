#!/bin/bash
# usage: run.sh dev|branch python-args...
which=$1; shift
case $which in
  dev) CO=/local/job40390255/worktrees/base-637-ng ;;
  branch) CO=/local/job40390255/worktrees/637-ng ;;
  *) echo bad; exit 2;;
esac
export PYTHONPATH=$CO
export LSMS_DATA_DIR=/local/job40390255/data-private/637-ng/$which
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
export CHECKOUT=$CO
cd $CO
exec nice -n 19 /local/job40390255/venv_img/bin/python "$@"
