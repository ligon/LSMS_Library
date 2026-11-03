#!/bin/bash
# burst.sh [--cold] <branch> <cmd...>  -- run INSIDE a burst job (normally via submit.sh).
#
# Clones <branch> from the shared mirror into this job's /local, mounts the venv image,
# points PYTHONPATH / LSMS_COUNTRIES_ROOT at the clone, pins BLAS to one thread and sets
# LSMS_BUILD_WORKERS to half the cores, asserts the import identity, then execs <cmd> from
# the clone.  The branch must be committed: the post-commit hook publishes it to the mirror.
#   default : shared data root (warm harmonized cache + shared source blobs)
#   --cold  : private EMPTY harmonized cache on this node; shared source blobs (L1) symlinked
set -euo pipefail
MIRROR=${SWEEP_MIRROR:-$HOME/mirrors/LSMS_Library}
cold=0; if [ "${1:-}" = "--cold" ]; then cold=1; shift; fi
branch=${1:?usage: burst.sh [--cold] <branch> <cmd...>}; shift
J=${SLURM_JOB_ID:?run inside a Slurm job (use submit.sh)}
[ "$J" != "${SWEEP_COORD_JOB:-}" ] || { echo "burst.sh: refusing to run inside the coordinator allocation $J" >&2; exit 2; }
L=/local/job$J; mkdir -p "$L/burst" "$L/tmp"; export TMPDIR=$L/tmp
W=$(mktemp -d "$L/burst/XXXXXX")/LSMS_Library
git clone -q --branch "$branch" "$MIRROR" "$W"
cd "$W"
echo "BURST host=$(hostname) job=$J cpus=${SLURM_CPUS_ON_NODE:-?} head=$(git log -1 --format='%h %s')"
bin/savio_venv.sh mount > "$L/venv_mount.log" 2>&1 || { cat "$L/venv_mount.log"; exit 3; }
export W PYTHONPATH=$W LSMS_COUNTRIES_ROOT=$W/lsms_library/countries PATH=$W/.venv/bin:$PATH
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1
export LSMS_BUILD_WORKERS=${LSMS_BUILD_WORKERS:-$(( ${SLURM_CPUS_ON_NODE:-2} / 2 ))}
if [ "$cold" = 1 ]; then
  shared_l1=${SWEEP_SHARED_L1:-$(python -c 'from lsms_library.paths import data_root; print(data_root())' 2>/dev/null | tail -1)/dvc-cache}
  export LSMS_DATA_DIR=$L/data; mkdir -p "$LSMS_DATA_DIR"
  ln -sfn "$shared_l1" "$LSMS_DATA_DIR/dvc-cache"
  echo "BURST cold data root $LSMS_DATA_DIR (L1 -> $shared_l1)"
fi
python -c "import lsms_library,os;p=os.path.realpath(lsms_library.__file__);w=os.path.realpath('$W');assert p.startswith(w+os.sep),p;print('BURST import',p)"
exec "$@"
