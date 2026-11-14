#!/bin/bash
# Red team, burkina stream: cold base-vs-candidate builds, raw census, hash census, strict levers,
# cold single-wave crop_production requests in their own empty roots.
# Usage: job_build.sh <cand-sha> <base-sha>
set -uo pipefail
B=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113
S=$B/reports/burkina-redteam
# roots: 1 cand, 2 base, 3 raw+hash (cand), 4 cand 2018-only, 5 cand 2021-only, 6 base 2018-only
source $B/tools/burst-env.sh burkina-redteam "$1" "$2" "$1" "$1" "$1" "$2" || exit 9
export PATH="$(dirname "$PY"):$PATH"
J=$SLURM_JOB_ID
FRC=$JOBLOCAL/frames-cand; FRB=$JOBLOCAL/frames-base; FRS=$JOBLOCAL/frames-single; mkdir -p $FRC $FRB $FRS; chmod 700 $FRC $FRB $FRS
( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" $S/scripts/build.py cand $FRC $OUT/build-cand.json > $S/logs/build-cand-$J.log 2>&1 ) &
P1=$!
( cd "$SRC_2" && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 "$PY" $S/scripts/build.py base $FRB $OUT/build-base.json > $S/logs/build-base-$J.log 2>&1 ) &
P2=$!
( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_3 "$PY" $S/scripts/raw_census.py $OUT/raw_census.json > $S/logs/raw-census-$J.log 2>&1
  cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_3 "$PY" $B/tools/hash_census.py $OUT/census-cand.json > $S/logs/hash-cand-$J.log 2>&1
  cd "$SRC_2" && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_3 "$PY" $B/tools/hash_census.py $OUT/census-base.json > $S/logs/hash-base-$J.log 2>&1
  "$PY" $B/tools/hashdiff.py $OUT/census-base.json $OUT/census-cand.json $OUT/hashdiff.json > $S/logs/hashdiff-$J.log 2>&1 ) &
P3=$!
( cd "$SRC_1" && mkdir -p $FRS/cand && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_4 "$PY" $S/scripts/single_wave.py build 2018-19 $FRS/cand $OUT/single-cand-2018.json > $S/logs/single-cand-2018-$J.log 2>&1
  cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_5 "$PY" $S/scripts/single_wave.py build 2021-22 $FRS/cand $OUT/single-cand-2021.json > $S/logs/single-cand-2021-$J.log 2>&1
  cd "$SRC_2" && mkdir -p $FRS/base && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_6 "$PY" $S/scripts/single_wave.py build 2018-19 $FRS/base $OUT/single-base-2018.json > $S/logs/single-base-2018-$J.log 2>&1 ) &
P4=$!
wait $P1; echo "cand build exit $?"; wait $P2; echo "base build exit $?"
( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" $S/scripts/compare.py $FRB $FRC $OUT/build-base.json $OUT/build-cand.json $OUT/compare.json ) > $S/logs/compare-$J.log 2>&1
echo "compare exit $?"
for arm in cand base; do
  if [ $arm = cand ]; then SRC=$SRC_1; DATA=$DATA_1; FR=$FRC; else SRC=$SRC_2; DATA=$DATA_2; FR=$FRB; fi
  ( cd "$SRC" && PYTHONPATH=$SRC LSMS_DATA_DIR=$DATA "$PY" $S/scripts/strict.py warm $FR $OUT/strict-warm-$arm.json ) > $S/logs/strict-warm-$arm-$J.log 2>&1
  ( cd "$SRC" && PYTHONPATH=$SRC LSMS_DATA_DIR=$DATA LSMS_GRAIN_STRICT=1 "$PY" $S/scripts/strict.py grain $FR $OUT/strict-grain-$arm.json ) > $S/logs/strict-grain-$arm-$J.log 2>&1
  ( cd "$SRC" && PYTHONPATH=$SRC LSMS_DATA_DIR=$DATA LSMS_DATE_STRICT=1 "$PY" $S/scripts/strict.py date $FR $OUT/strict-date-$arm.json ) > $S/logs/strict-date-$arm-$J.log 2>&1
done
wait $P4; echo "single exit $?"
( cd "$SRC_1" && PYTHONPATH=$SRC_1 "$PY" $S/scripts/single_wave.py cmp $FRC $FRS/cand 2018-19 $OUT/single-cmp-cand-2018.json
  cd "$SRC_1" && PYTHONPATH=$SRC_1 "$PY" $S/scripts/single_wave.py cmp $FRC $FRS/cand 2021-22 $OUT/single-cmp-cand-2021.json
  cd "$SRC_1" && PYTHONPATH=$SRC_1 "$PY" $S/scripts/single_wave.py cmp $FRB $FRS/base 2018-19 $OUT/single-cmp-base-2018.json ) > $S/logs/single-cmp-$J.log 2>&1
wait $P3; echo "raw/hash exit $?"
echo "in-tree parquets cand: $(find $SRC_1/lsms_library/countries -name '*.parquet' | wc -l) base: $(find $SRC_2/lsms_library/countries -name '*.parquet' | wc -l)"
echo done
