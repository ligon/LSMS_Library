#!/bin/bash
#SBATCH -J benin-gh932
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
source $BATCH/tools/burst-env.sh benin $1 $2 || exit 1
O=$OUT/gh932-${1:0:12}; mkdir -p $O
for i in 1 2; do s=SRC_$i; d=DATA_$i; (cd ${!s} && PYTHONPATH=${!s} LSMS_DATA_DIR=${!d} "$PY" -m pytest -p no:cacheprovider -q tests/test_gh932_safe_ci_failure_locations.py > $O/alone-$i.txt 2>&1; echo "exit $?" >> $O/alone-$i.txt)
(cd ${!s} && PYTHONPATH=${!s} LSMS_DATA_DIR=${!d} "$PY" -m pytest -n 8 --dist=loadfile -p no:cacheprovider -q tests/test_gh932_safe_ci_failure_locations.py tests/test_private_ci_check.py > $O/xdist-$i.txt 2>&1; echo "exit $?" >> $O/xdist-$i.txt); done
