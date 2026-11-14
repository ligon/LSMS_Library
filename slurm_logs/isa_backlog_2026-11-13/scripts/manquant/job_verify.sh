#!/bin/bash
#SBATCH -J manquant-verify
# Usage: job_verify.sh <cand-sha>: cold candidate build + compare to base-probe, and hash census.
set -uo pipefail
R=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/manquant
T=/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools
BASE=f138a3d61c203f892148592675a7ef72e160a322
CAND=${1:?cand sha}
source /global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/tools/burst-env.sh manquant $CAND $BASE || exit 1
S=${CAND:0:12}
( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" "$R/scripts/build_tables.py" "$OUT/cand-$S" Togo,Senegal,CotedIvoire ) > "$OUT/build_cand_$S.log" 2>&1
echo "build exit $?"
( cd "$SRC_1" && PYTHONPATH=$SRC_1 "$PY" "$R/scripts/compare.py" "$OUT/base-probe" "$OUT/cand-$S" "$OUT/compare_$S.json" ) > "$OUT/compare_$S.log" 2>&1
echo "compare exit $?"
( cd "$SRC_1" && PYTHONPATH=$SRC_1 "$PY" "$R/scripts/sen_join.py" "$OUT/cand-$S" ) > "$OUT/senjoin_cand_$S.json" 2>&1
( cd "$SRC_1" && PYTHONPATH=$SRC_1 LSMS_DATA_DIR=$DATA_1 "$PY" $T/hash_census.py $OUT/census-cand-$S.json ) > "$OUT/census_cand_$S.log" 2>&1
echo "census cand exit $?"
if [ ! -s "$OUT/census-base.json" ]; then
  ( cd "$SRC_2" && PYTHONPATH=$SRC_2 LSMS_DATA_DIR=$DATA_2 "$PY" $T/hash_census.py $OUT/census-base.json ) > "$OUT/census_base.log" 2>&1
  echo "census base exit $?"
fi
"$PY" $T/hashdiff.py $OUT/census-base.json $OUT/census-cand-$S.json $OUT/hashdiff-$S.json > "$OUT/hashdiff_$S.txt" 2>&1
echo "hashdiff exit $?"
find "$SRC_1/lsms_library/countries" -name '*.parquet' | head
