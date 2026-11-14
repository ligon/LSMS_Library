#!/bin/bash
#SBATCH -J rt-benin-raw3
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
RT=$BATCH/reports/benin-redteam
CAND=d5fdf326cd2bcec359e3c1a89e63c732cb6e0815
source $BATCH/tools/burst-env.sh benin-redteam $CAND || exit 1
O=$OUT/raw3; mkdir -p $O
( cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $RT/scripts/rt_raw3.py $SRC_1 $O $OUT/main > $O/log.txt 2>&1; echo "exit $?" >> $O/log.txt )
# regenerate the crosswalk in this job-local clone: the asserts run, and the output must be byte-identical
( cd $SRC_1/lsms_library/countries/Benin/_ && PATH=$(dirname $PY):$PATH PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 make panel-ids > $O/make_panel_ids.txt 2>&1; echo "exit $?" >> $O/make_panel_ids.txt )
{ echo "== git status"; git -C $SRC_1 status --short; echo "== git diff --stat"; git -C $SRC_1 diff --stat; md5sum $SRC_1/lsms_library/countries/Benin/_/*.json; } > $O/regen_status.txt 2>&1
echo ALLDONE > $O/ALLDONE
