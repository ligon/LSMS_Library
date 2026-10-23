# usage: source env.sh base|branch
case "$1" in
  base) export CHECKOUT=/local/job40390255/worktrees/base-894 ;;
  branch) export CHECKOUT=/local/job40390255/worktrees/894 ;;
  *) echo "need base|branch"; return 1 ;;
esac
export PYTHONPATH=$CHECKOUT
export LSMS_DATA_DIR=/local/job40390255/data-private/894/${2:-$1}
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1
export PY=/local/job40390255/venv_img/bin/python
