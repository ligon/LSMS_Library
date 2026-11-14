#!/bin/bash
#SBATCH -J rt-benin-gh932
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
CAND=d5fdf326cd2bcec359e3c1a89e63c732cb6e0815
BASE=f138a3d61c203f892148592675a7ef72e160a322
source $BATCH/tools/burst-env.sh benin-redteam $CAND $BASE || exit 1
O=$OUT/gh932; mkdir -p $O
for s in 1 2; do v=SRC_$s; d=DATA_$s; ( cd ${!v} && PYTHONPATH=${!v} LSMS_DATA_DIR=${!d} $PY -m pytest -p no:cacheprovider -q -rfE tests/test_gh932_safe_ci_failure_locations.py > $O/side$s.txt 2>&1; echo "exit $?" >> $O/side$s.txt ); done
echo ALLDONE > $O/ALLDONE
