#!/bin/bash
#SBATCH -J mwrt-2
#SBATCH -o /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/malawi-redteam/logs/%x-%j.out
# Red team, malawi: early-wave scan + documentation text, raw-file oracle, frame compare.
CAND=af81784630ef1ad650a705f4f9c85dde98912797
BATCH=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
RT=$BATCH/reports/malawi-redteam
source $BATCH/tools/burst-env.sh malawi-redteam $CAND || exit 9
O=$OUT/job2; mkdir -p $O/scan $O/oracle
JL=/local/job$SLURM_JOB_ID
SCANROOT=$JL/private/data-scan; ORROOT=$JL/private/data-oracle; mkdir -p $SCANROOT $ORROOT
( cd $JL && $PY -m pip install --quiet --target $JL/pylib pypdf > $O/pip.log 2>&1; echo "pip rc=$?" )
( cd $SRC_1 && env PYTHONPATH=$SRC_1:$JL/pylib LSMS_DATA_DIR=$SCANROOT SCAN_WAVES=2010-11,2013-14 \
    DOC_WAVES=2010-11,2013-14,2016-17,2019-20,2024-25 $PY $RT/scripts/scan_meta.py $SRC_1 $O/scan > $O/scan/scan.out 2>&1; echo "scan rc=$? $(date -Is)" ) &
( cd $SRC_1 && env PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$ORROOT $PY $RT/scripts/oracle.py $SRC_1 $OUT/job1/frames $O/oracle > $O/oracle/oracle.out 2>&1; echo "oracle rc=$? $(date -Is)" ) &
( cd $SRC_1 && env PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$ORROOT $PY $RT/scripts/compare.py $OUT/job1/frames $O/compare.json > $O/compare.out 2>&1; echo "compare rc=$? $(date -Is)" ) &
wait
find $SRC_1/lsms_library/countries -name '*.parquet' | wc -l
git -C $SRC_1 status --porcelain | head -5
echo "JOB DONE $(date -Is)"
