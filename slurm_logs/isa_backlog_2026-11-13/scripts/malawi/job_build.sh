#!/bin/bash
#SBATCH -J mw-build
#SBATCH -A co_carleton
#SBATCH -p savio4_htc
#SBATCH -q carleton_htc4_normal
#SBATCH -N 1
#SBATCH -n 1
#SBATCH -c 4
#SBATCH --mem=48G
#SBATCH -t 3:00:00
#SBATCH --exclude=n0035.savio4,n0034.savio4,n0046.savio4
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/malawi/logs/%x-%j.out
# Cold build of one arm in an empty private root.  usage: sbatch --export=ALL,SHA=<sha>,ARM=<name> job_build.sh
SHA=${SHA:?}; ARM=${ARM:?}
REP=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/malawi
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh malawi $SHA || exit 9
FR=$OUT/frames
mkdir -p $FR
cd $SRC_1 && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 $PY $REP/scripts/build.py $SRC_1 $ARM $FR > $FR/$ARM.build.log 2>&1
echo "build $ARM rc=$?"
tail -3 $FR/$ARM.build.log
echo "in-tree parquets: $(find $SRC_1/lsms_library/countries -name '*.parquet' | wc -l)"
git -C $SRC_1 status --porcelain | head
echo "JOB DONE $(date -Is)"
